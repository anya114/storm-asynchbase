/*
 * Charles-Antoine Mathieu <charles-antoine.mathieu@ovh.net>
 */

package storm.asynchbase.trident.operation;

import backtype.storm.topology.FailedException;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.PleaseThrottleException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.asynchbase.trident.mapper.IAsyncHBaseTridentFieldMapper;
import storm.asynchbase.trident.mapper.IAsyncHBaseTridentMapper;
import storm.asynchbase.utils.AsyncHBaseClientFactory;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * <p>
 * This function execute one or more HBase RPC using the AsyncHBase client<br/>
 * </p>
 * <p>
 * You have to provide some AsyncHBase fields mappers to map tuple fields
 * to HBase RPC requests.
 * </p>
 * <p>
 * By default the function is asynchronous ie: the tuples are emitted in
 * the Callback when the result is available. So the thread won't block waiting
 * for the response. But callback may be executed by another thread and unfortunately
 * the OuputCollector object is no longer thread safe, so the calls to ack/fail/emit
 * are synchronized. It should be ok as long as you have one tasks per executor
 * thread ( this is the default behaviour of storm ).<br/>
 * You may make the bolt synchronous by calling setAsync(false) but of course it's
 * killing performance.<br/>
 * Note : Even in synchronous mode multiple RPCs will run in a parallel.
 * Note : In DRPC mode you may have to use the synchronous mode or some other kind
 *        of synchronous function or the DRPC may return before the results are ready
 * </p>
 * <p>
 * Throttling :<br/>
 * If HBase can't keep with the stream speed you should get some
 * "There are now N RPCs pending due to NSRE on..." in the logs and the AsyncHBase
 * client will trigger a PleaseThrottleException when reaching 10k pending requests
 * on a specific region. This appends especially when HBase is splitting regions.<br/>
 * This bolt will try to throttle stream speed by turning some execute calls to<
 * synchronous mode and sleeping a little.<br/>
 * Tuple failed due to PleaseThrottleExecption have to be replayed by the spout if
 * needed.<br/>
 * Note: the throttle code comes from net.opentsdb.tools.TextImporter
 * </p>
 * <p>
 * Results :<br/>
 * This function will emit a tuple containing the RPCs results in the same order the
 * mapper returned the RPCs.<br/>
 * AsyncHBaseMapper return the requests in the same order
 * you provided names, but as it use a HashMap to store fields mappers it will return
 * all requests in an undefined order if you don't provide names. Feel free
 * to write you own mapper implementation.
 * </p>
 * <p>
 * Look at storm.asynchbase.example.topology.AsyncHBaseTridentExampleTopology for a
 * concrete use case.
 * </p>
 */
public class ExecuteHBaseRpcs extends BaseFunction {
    public static final Logger log = LoggerFactory.getLogger(ExecuteHBaseRpcs.class);

    private final String cluster;
    private final IAsyncHBaseTridentMapper mapper;
    private final List<String> names;
    public FailStrategy failStrategy = FailStrategy.LOG;
    Callback<ArrayList<Object>, ArrayList<Object>> successCallback;
    Callback<Object, Exception> errorCallback;
    private HBaseClient client;
    private boolean async = false;
    private long timeout = 0;
    private volatile boolean throttle = false;
    ;

    /**
     * @param cluster Cluster name to get the right AsyncHBase client.
     * @param mapper  Mapper containing all RPC configuration.
     * @param names   List of RPCs to execute.
     */
    public ExecuteHBaseRpcs(String cluster, IAsyncHBaseTridentMapper mapper, List<String> names) {
        this.cluster = cluster;
        this.mapper = mapper;
        this.names = names;
    }

    /**
     * @param cluster Cluster name to get the right AsyncHBase client.
     * @param mapper  Mapper containing all RPC configuration.
     * @param name    Name of the single RPC to execute.
     */
    public ExecuteHBaseRpcs(String cluster, IAsyncHBaseTridentMapper mapper, String name) {
        this(cluster, mapper, Collections.singletonList(name));
    }

    /**
     * @param callback Add a success callback between RPC return and tuple ack/emit.
     * @return This so you can do method chaining.
     */
    public ExecuteHBaseRpcs addCallback(Callback<ArrayList<Object>, ArrayList<Object>> callback) {
        this.successCallback = callback;
        return this;
    }

    /**
     * @param errback Add an error callback between RPC return and tuple failure.
     * @return This so you can do method chaining.
     */
    public ExecuteHBaseRpcs addErrback(Callback<Object, Exception> errback) {
        this.errorCallback = errback;
        return this;
    }

    /**
     * @param async set synchronous/asynchronous mode
     * @return This so you can do method chaining.
     */
    public ExecuteHBaseRpcs setAsync(boolean async) {
        this.async = async;
        return this;
    }

    /**
     * @param timeout how long to wait for results in synchronous mode
     *                (in millisecond).
     * @return This so you can do method chaining.
     */
    public ExecuteHBaseRpcs setTimeout(long timeout) {
        this.timeout = timeout;
        return this;
    }

    /**
     * <p>
     * LOG : Only log error and don't return any results.<br/>
     * RETRY : Ask the spout to replay the batch.<br/>
     * FAILFAST : Let the function crash.<br/>
     * null/NOOP : Do nothing.
     * </p>
     * <p>
     * http://svendvanderveken.wordpress.com/2014/02/05/error-handling-in-storm-trident-topologies/
     * </p>
     *
     * @param strategy Set the strategy to adopt in case of AsyncHBase execption
     * @return This so you can do method chaining.
     */
    public ExecuteHBaseRpcs setFailStrategy(FailStrategy strategy) {
        this.failStrategy = strategy;
        return this;
    }

    /**
     * <p>
     * Get corresponding HBaseClient and Initialize mappers
     * </p>
     * @param conf Topology configuration
     * @param context Operation context
     */
    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        super.prepare(conf, context);
        this.client = AsyncHBaseClientFactory.getHBaseClient(conf, this.cluster);
        this.mapper.prepare(conf);
    }

    @Override
    public void execute(final TridentTuple tuple, final TridentCollector collector) {
        List<Deferred<Object>> requests = new ArrayList<>(names.size());
        for (IAsyncHBaseTridentFieldMapper fieldMapper : mapper.getFieldMappers(names)) {
            switch (fieldMapper.getRpcType()) {
                case PUT:
                    requests.add(client.put(fieldMapper.getPutRequest(tuple)));
                    break;
                case INCR:
                    requests.add(client.atomicIncrement(fieldMapper.getIncrementRequest(tuple))
                        // Dummy callback to cast long to Object
                        .addCallback(new Callback<Object, Long>() {
                            @Override
                            public Object call(Long value) throws Exception {
                                return value;
                            }
                        }));
                    break;
                case DELETE:
                    requests.add(client.delete(fieldMapper.getDeleteRequest(tuple)));
                    break;
                case GET:
                    requests.add(client.get(fieldMapper.getGetRequest(tuple))
                        // Dummy callback to cast ArrayList<KeyValue> to Object
                        .addCallback(new Callback<Object, ArrayList<KeyValue>>() {
                            @Override
                            public Object call(ArrayList<KeyValue> values) throws Exception {
                                return values;
                            }
                        }));
                    break;
            }
        }

        Deferred<ArrayList<Object>> results = Deferred.groupInOrder(requests);

        if (this.successCallback != null) {
            results = results.addCallback(this.successCallback);
        }

        if (this.errorCallback != null) {
            results = results.addErrback(this.errorCallback);
        }

        if (throttle) {
            log.warn("Throttling...");
            long throttle_time = System.nanoTime();
            try {
                collector.emit(results.joinUninterruptibly(this.timeout));
            } catch (Exception ex) {
                this.handleFailure(ex);
            } finally {
                throttle_time = System.nanoTime() - throttle_time;
                if (throttle_time < 1000000000L) {
                    log.info("Got throttled for only " + throttle_time + "ns, sleeping a bit now");
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ex) {
                        this.handleFailure(ex);
                    }
                }
                log.info("Done throttling...");
                this.throttle = false;
            }
        } else if (!this.async) {
            try {
                collector.emit(results.joinUninterruptibly(this.timeout));
            } catch (Exception ex) {
                this.handleFailure(ex);
            }
        } else {
            results.addCallbacks(new Callback<Object, ArrayList<Object>>() {
                @Override
                public Object call(ArrayList<Object> results) throws Exception {
                    synchronized (collector) {
                        collector.emit(results);
                    }
                    return null;
                }
            }, new Callback<Object, Exception>() {
                @Override
                public Object call(Exception ex) throws Exception {
                    // TODO : This may trigger the wrong batch to be replayed ?
                    handleFailure(ex);
                    return ex;
                }
            });
        }
    }

    @Override
    public void cleanup() {
        super.cleanup();
    }

    private void handleFailure(Exception ex) {
        if (ex instanceof PleaseThrottleException) {
            throttle = true;
        }
        switch (this.failStrategy) {
            case LOG:
                log.error("AsyncHBase error while executing HBase RPC" + ex.getMessage());
                break;
            case RETRY:
                log.error("AsyncHBase error while executing HBase RPC" + ex.getMessage());
                throw new FailedException("AsyncHBase error while executing HBase RPC " + ex.getMessage());
            case FAILFAST:
                log.error("AsyncHBase error while executing HBase RPC" + ex.getMessage());
                throw new RuntimeException("AsyncHBase error while executing HBase RPC " + ex.getMessage());
        }
    }

    public enum FailStrategy {NOOP, LOG, FAILFAST, RETRY}
}
