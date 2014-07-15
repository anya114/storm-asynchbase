/*
 * Charles-Antoine Mathieu <charles-antoine.mathieu@ovh.net>
 */

package storm.asynchbase.trident.state;

import backtype.storm.topology.FailedException;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.asynchbase.trident.mapper.IAsyncHBaseTridentFieldMapper;
import storm.asynchbase.utils.AsyncHBaseClientFactory;
import storm.trident.operation.TridentCollector;
import storm.trident.state.State;
import storm.trident.tuple.TridentTuple;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * <p>
 * This is a TridentState implementation to persist a partition to HBase using AsyncHBase client.<br/>
 * You should only use this state if your update is idempotent regarding batch replay.
 * </p>
 * <p>
 * Use storm.asynchbase.trident.state.StateFactory to handle state creation<br/>
 * Use storm.asynchbase.trident.state.StateUpdater to update state<br/>
 * Use storm.asynchbase.trident.state.StateQuery to query state<br/>
 * </p>
 * <p>
 * Please look at storm.asynchbase.example.topology.AsyncHBaseTridentStateExampleTopology
 * for a concrete use case.
 * </p>
 */
public class AsyncHBaseState implements State {
    public static final Logger log = LoggerFactory.getLogger(AsyncHBaseState.class);
    private final HBaseClient client;
    private final Options options;

    /**
     * @param options
     * @param conf
     */
    public AsyncHBaseState(Options options, Map conf) {
        this.options = options;
        this.client = AsyncHBaseClientFactory.getHBaseClient(conf, options.cluster);
    }

    /**
     * @param tuples    Storm tuples to process.
     * @param collector Unused as PutRequest returns void.
     */
    public void updateState(final List<TridentTuple> tuples, final TridentCollector collector) {
        ArrayList<Deferred<Object>> results = new ArrayList<>(tuples.size());

        for (TridentTuple tuple : tuples) {
            results.add(this.client.put(this.options.updateMapper.getPutRequest(tuple)));
        }

        try{
            Deferred.group(results).joinUninterruptibly(this.options.timeout);
        } catch (Exception ex) {
            this.handleFailure(ex);
        }
    }

    /**
     * @param tuples Storm Storm tuples to process.
     * @return Future results.
     */
    public List<Deferred<ArrayList<KeyValue>>> get(final List<TridentTuple> tuples) {
        ArrayList<Deferred<ArrayList<KeyValue>>> results = new ArrayList<>(tuples.size());

        for (TridentTuple tuple : tuples) {
            results.add(this.client.get(this.options.queryMapper.getGetRequest(tuple)));
        }

        return results;
    }

    @Override
    public void beginCommit(Long txid) {
        log.debug("Beginning commit for tx " + txid);
    }

    @Override
    public void commit(Long txid) {
        log.debug("Commit tx " + txid);
    }

    private void handleFailure(Exception ex) {
        switch (this.options.failStrategy) {
            case LOG:
                log.error("AsyncHBase error while executing HBase RPC" + ex.getMessage());
                break;
            case RETRY:
                throw new FailedException("AsyncHBase error while executing HBase RPC " + ex.getMessage());
            case FAILFAST:
                throw new RuntimeException("AsyncHBase error while executing HBase RPC " + ex.getMessage());
        }
    }

    /**
     * <ul>
     * <li>
     * cluster : AsyncHBase client to use.
     * </li>
     * <li>
     * timeout : timeout while doing multiget/multiput in milliseconds
     * </li>
     * <li>
     * table : table to use<br/>
     * columnFamily : columnFamily to use<br/>
     * columnQualifier : columnQualifier to use<br/>
     * </li>
     * <li>
     * FailStrategy : Modify the way strom will handle AsyncHBase failures.<br/>
     * LOG : Only log error and don't return any results.<br/>
     * RETRY : Ask the spout to replay the batch.<br/>
     * FAILFAST : Let the function crash.<br/>
     * null/NOOP : Do nothing.<br/>
     * http://svendvanderveken.wordpress.com/2014/02/05/error-handling-in-storm-trident-topologies/
     * </li>
     * </ul>
     */
    public static class Options implements Serializable {
        public String cluster;
        public long timeout=0;
        public IAsyncHBaseTridentFieldMapper updateMapper;
        public IAsyncHBaseTridentFieldMapper queryMapper;
        public FailStrategy failStrategy = FailStrategy.LOG;

        public enum FailStrategy {NOOP, LOG, FAILFAST, RETRY}
    }
}
