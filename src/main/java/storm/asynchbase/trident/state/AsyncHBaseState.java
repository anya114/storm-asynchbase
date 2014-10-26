/*
 * Charles-Antoine Mathieu <charles-antoine.mathieu@ovh.net>
 */

package storm.asynchbase.trident.state;

import backtype.storm.topology.FailedException;
import com.stumbleupon.async.Deferred;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.asynchbase.trident.mapper.IAsyncHBaseTridentFieldMapper;
import storm.asynchbase.trident.mapper.IAsyncHBaseTridentMapper;
import storm.asynchbase.utils.AsyncHBaseClientFactory;
import storm.trident.operation.TridentCollector;
import storm.trident.state.State;
import storm.trident.tuple.TridentTuple;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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

    /**
     * @param client AsyncHBaseClient to use.
     */
    public AsyncHBaseState(HBaseClient client) {
        this.client = client;
    }

    /**
     * @param tuples        Storm tuples to process.
     * @param updateMapper  A mapper to map trident tuple to AsyncHBase requests.
     * @return Future results.
     */
    public Deferred<ArrayList<Object>> updateState(final IAsyncHBaseTridentMapper updateMapper, final List<TridentTuple> tuples) {
        ArrayList<Deferred<Object>> results = new ArrayList<>(tuples.size());

        for (TridentTuple tuple : tuples) {
            for ( IAsyncHBaseTridentFieldMapper fieldMapper : updateMapper.getFieldMappers() ) {
                results.add(this.client.put(fieldMapper.getPutRequest(tuple)));
            }
        }

        return Deferred.groupInOrder(results);
    }

    /**
     * @param tuples        Storm tuples to process.
     * @param queryMapper   A mapper to map trident tuple to AsyncHBase requests.
     * @return List of Future results.
     */
    public List<Deferred<ArrayList<KeyValue>>> get(final IAsyncHBaseTridentMapper queryMapper, final List<TridentTuple> tuples) {
        List<Deferred<ArrayList<KeyValue>>> requests = new ArrayList<>(tuples.size());

        for (TridentTuple tuple : tuples) {
            for ( IAsyncHBaseTridentFieldMapper fieldMapper : queryMapper.getFieldMappers() ) {
                requests.add(this.client.get(fieldMapper.getGetRequest(tuple)));
            }
        }

        return requests;
    }

    @Override
    public void beginCommit(Long txid) {
        log.debug("Beginning commit for tx " + txid);
    }

    @Override
    public void commit(Long txid) {
        log.debug("Commit tx " + txid);
    }
}
