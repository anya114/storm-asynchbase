/*
 * Charles-Antoine Mathieu <charles-antoine.mathieu@ovh.net>
 */

package storm.asynchbase.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.PleaseThrottleException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.asynchbase.bolt.mapper.IAsyncHBaseFieldMapper;
import storm.asynchbase.bolt.mapper.IAsyncHBaseMapper;
import storm.asynchbase.utils.AsyncHBaseClientFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * <p>
 * This bold executes one or more HBase RPC using the AsyncHBase client<br/>
 * </p>
 * <p>
 * You have to provide some AsyncHBase fields mappers to map tuple fields
 * to HBase RPC requests.
 * </p>
 * <p>
 * By default the bolt is asynchronous ie: the tuples are acked or failed in
 * the Callback when the result is available. So the thread won't block waiting
 * for the response. But callback may be executed by another thread and unfortunately
 * the OuputCollector object is no longer thread safe, so the calls to ack/fail/emit
 * are synchronized. It should be ok as long as you have one tasks per executor
 * thread ( this is the default behaviour of storm ).<br/>
 * You may make the bolt synchronous by calling setAsync(false) but of course it's
 * killing performance.<br/>
 * Note : Even in synchronous mode multiple RPCs will run in parallel.
 * </p>
 * <p>
 * Throttling :<br/>
 * If HBase can't keep with the stream speed you should get some
 * "There are now N RPCs pending due to NSRE on..." in the logs and the AsyncHBase
 * client will trigger a PleaseThrottleException when reaching 10k pending requests
 * on a specific region. This appends especially when HBase is splitting regions.<br/>
 * This bolt will try to throttle stream speed by turning some execute calls to
 * synchronous mode and sleeping a little.<br/>
 * Please verify that your spout ack tuples and use conf.setMaxSpoutPending
 * to control stream speed.
 * Tuple failed due to PleaseThrottleExecption have to be replayed by the spout if
 * needed.<br/>
 * Note: the throttle code comes from net.opentsdb.tools.TextImporter
 * </p>
 * <p>
 * Results :<br/>
 * This bolt will emit a tuple containing the RPCs results in the same order the
 * mapper returned the RPCs.<br/>
 * AsyncHBaseMapper return the requests in the same order you added fieldMappers
 * </p>
 * <p>
 * Look at storm.asynchbase.example.topology.AsyncHBaseBoltExampleTopology for a
 * concrete use case.
 * </p>
 */
public class AsyncHBaseBolt implements IRichBolt {
    public static final Logger log = LoggerFactory.getLogger(AsyncHBaseBolt.class);
    private final String cluster;
    private final IAsyncHBaseMapper mapper;
    private HBaseClient client;
    private OutputCollector collector;
    private boolean async = true;
    private long timeout = 0;
    private volatile boolean throttle = false;

    private Callback<Object, Long> incrCastCallback;
    private Callback<Object, ArrayList<KeyValue>> getCastCallback;

    /**
     * @param cluster Cluster name to get the right AsyncHBase client.
     * @param mapper  Mapper containing all RPC configuration.
     */
    public AsyncHBaseBolt(String cluster, IAsyncHBaseMapper mapper) {
        this.cluster = cluster;
        this.mapper = mapper;
    }

    /**
     * @param async set synchronous/asynchronous mode
     * @return This so you can do method chaining.
     */
    public AsyncHBaseBolt setAsync(boolean async) {
        this.async = async;
        return this;
    }

    /**
     * @param timeout how long to wait for results in synchronous mode
     *                (in millisecond).
     * @return This so you can do method chaining.
     */
    public AsyncHBaseBolt setTimeout(long timeout) {
        this.timeout = timeout;
        return this;
    }

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.client = AsyncHBaseClientFactory.getHBaseClient(conf, this.cluster);
        this.mapper.prepare(conf);
        this.incrCastCallback = new Callback<Object, Long>() {
            @Override
            public Object call(Long value) throws Exception {
                return value;
            }
        };
        this.getCastCallback = new Callback<Object, ArrayList<KeyValue>>() {
            @Override
            public Object call(ArrayList<KeyValue> values) throws Exception {
                return values;
            }
        };
    }

    @Override
    public void execute(final Tuple tuple) {
        List<IAsyncHBaseFieldMapper> mappers = mapper.getFieldMappers();
        ArrayList<Deferred<Object>> requests = new ArrayList<>(mappers.size());
        for (IAsyncHBaseFieldMapper fieldMapper : mappers) {
            switch (fieldMapper.getRpcType()) {
                case PUT:
                    requests.add(
                            client.put(fieldMapper.getPutRequest(tuple)));
                    break;
                case INCR:
                    requests.add(
                            client.atomicIncrement(fieldMapper.getIncrementRequest(tuple))
                                    // Dummy callback to cast long to Object
                                    .addCallback(incrCastCallback)
                    );
                    break;
                case DELETE:
                    requests.add(
                            client.delete(fieldMapper.getDeleteRequest(tuple)));
                    break;
                case GET:
                    requests.add(
                            client.get(fieldMapper.getGetRequest(tuple))
                                    // Dummy callback to cast ArrayList<KeyValue> to Object
                                    .addCallback(getCastCallback)
                    );
                    break;
            }
        }

        Deferred<ArrayList<Object>> results = Deferred.groupInOrder(requests);

        if (throttle) {
            log.warn("Throttling...");
            long throttle_time = System.nanoTime();
            try {
                results.joinUninterruptibly(this.timeout);
                this.collector.ack(tuple);
            } catch (Exception ex) {
                log.error("AsyncHBase exception : " + ex.toString());
                this.collector.fail(tuple);
            } finally {
                throttle_time = System.nanoTime() - throttle_time;
                if (throttle_time < 1000000000L) {
                    log.info("Got throttled for only " + throttle_time + "ns, sleeping a bit now");
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ex) {
                        log.error("AsyncHBase exception : " + ex.toString());
                    }
                }
                log.info("Done throttling...");
                this.throttle = false;
            }
        } else if (!this.async) {
            try {
                this.collector.emit(results.joinUninterruptibly(this.timeout));
                this.collector.ack(tuple);
            } catch (Exception ex) {
                log.error("AsyncHBase exception : " + ex.toString());
                this.collector.fail(tuple);
            }
            this.collector.ack(tuple);
        } else {
            results.addCallbacks(new SuccessCallback(tuple), new ErrorCallback(tuple));
        }
    }

    @Override
    public void cleanup() {
        // TODO gracefully shutdown HBaseClient.
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("results"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    /**
     * Called on success in async mode to asynchronously ack the tuple.
     */
    class SuccessCallback implements Callback<Object, ArrayList<Object>> {
        final Tuple tuple;

        SuccessCallback(Tuple tuple) {
            this.tuple = tuple;
        }

        @Override
        public Object call(ArrayList<Object> results) throws Exception {
            synchronized (collector) {
                collector.ack(tuple);
            }
            return null;
        }
    }

    /**
     * Called on failure in async mode to asynchronously fail the tuple.
     */
    class ErrorCallback implements Callback<Object, Exception> {
        final Tuple tuple;

        ErrorCallback(Tuple tuple) {
            this.tuple = tuple;
        }

        @Override
        public Object call(Exception ex) throws Exception {
            log.error("AsyncHBase exception : " + ex.toString());
            if (ex instanceof PleaseThrottleException) {
                throttle = true;
                return ex;
            }
            synchronized (collector) {
                collector.fail(tuple);
            }
            return ex;
        }
    }
}