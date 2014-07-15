/*
 * Charles-Antoine Mathieu <charles-antoine.mathieu@ovh.net>
 */

package storm.asynchbase.trident.state;

import backtype.storm.topology.FailedException;
import backtype.storm.tuple.Values;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import org.hbase.async.KeyValue;
import org.hbase.async.PleaseThrottleException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.asynchbase.utils.AsyncHBaseDeserializer;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.state.BaseQueryFunction;
import storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * This function is used to query an AsyncHBaseState with
 * TridentStream.stateQuery(State, StateQuery,...)
 * <p>
 * You have to provide some AsyncHBase fields mappers to map tuple fields
 * to HBase RPC GetRequests.
 * </p>
 * <p>
 * Even if the execute function in synchronous all RPCs for the batch will run in a parallel.
 * </p>
 * <p>
 * Look at storm.asynchbase.example.topology.AsyncHBaseTridentStateExampleTopology for a
 * concrete use case.
 * </p>
 */
public class AsyncHBaseStateQuery extends BaseQueryFunction<AsyncHBaseState, Deferred<ArrayList<KeyValue>>> {
    public static final Logger log = LoggerFactory.getLogger(AsyncHBaseStateQuery.class);
    public FailStrategy failStrategy = FailStrategy.LOG;
    private AsyncHBaseDeserializer valueDeserializer;
    private long timeout = 0;

    /**
     * @param deserializer Deserializer to use to map cell value to a specific type.
     * @return This so you can do method chaining.
     */
    public AsyncHBaseStateQuery setValueDeserializer(AsyncHBaseDeserializer deserializer) {
        this.valueDeserializer = deserializer;
        return this;
    }

    /**
     * @param timeout how long to wait for results in synchronous mode
     *                (in millisecond).
     * @return This so you can do method chaining.
     */
    public AsyncHBaseStateQuery setTimeout(long timeout) {
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
     * @param strategy Set the strategy to adopt in case of AsyncHBase exception.
     * @return This so you can do method chaining.
     */
    public AsyncHBaseStateQuery setFailStrategy(FailStrategy strategy) {
        this.failStrategy = strategy;
        return this;
    }

    /**
     * <p>
     * Get corresponding HBaseClient and Initialize serializer if any.
     * </p>
     *
     * @param conf    Topology configuration.
     * @param context Operation context.
     */
    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        super.prepare(conf, context);
        if (this.valueDeserializer != null) {
            this.valueDeserializer.prepare(conf);
        }
    }

    /**
     * @param state  AsyncHBaseState to query.
     * @param tuples Storm tuples to process.
     * @return List of Future results.
     */
    @Override
    public List<Deferred<ArrayList<KeyValue>>> batchRetrieve(AsyncHBaseState state, List<TridentTuple> tuples) {
        return state.get(tuples);
    }

    @Override
    public void execute(TridentTuple tuple, Deferred<ArrayList<KeyValue>> result, final TridentCollector collector) {
        try {
            ArrayList<KeyValue> results = result.joinUninterruptibly(this.timeout);
            if (results.size() > 0) {
                if (valueDeserializer != null) {
                    collector.emit(new Values(valueDeserializer.deserialize(results.get(0).value())));
                } else {
                    collector.emit(new Values(results.get(0).value()));
                }
            }
        } catch (Exception ex) {
            this.handleFailure(ex);
        }
    }

    private void handleFailure(Exception ex) {
        switch (this.failStrategy) {
            case LOG:
                log.error("AsyncHBase error while executing HBase RPC" + ex.getMessage());
                break;
            case RETRY:
                throw new FailedException("AsyncHBase error while executing HBase RPC " + ex.getMessage());
            case FAILFAST:
                throw new RuntimeException("AsyncHBase error while executing HBase RPC " + ex.getMessage());
        }
    }

    public enum FailStrategy {NOOP, LOG, FAILFAST, RETRY}
}
