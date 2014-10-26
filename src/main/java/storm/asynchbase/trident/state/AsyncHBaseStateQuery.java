/*
 * Charles-Antoine Mathieu <charles-antoine.mathieu@ovh.net>
 */

package storm.asynchbase.trident.state;

import backtype.storm.topology.FailedException;
import backtype.storm.tuple.Values;
import com.stumbleupon.async.Deferred;
import org.hbase.async.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.asynchbase.trident.mapper.IAsyncHBaseTridentMapper;
import storm.asynchbase.utils.serializer.AsyncHBaseDeserializer;
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
    private AsyncHBaseDeserializer valueDeserializer;
    private long timeout = 0;

    private final IAsyncHBaseTridentMapper mapper;

    public AsyncHBaseStateQuery(IAsyncHBaseTridentMapper mapper) {
        this.mapper = mapper;
    }

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

    @Override
    public List<Deferred<ArrayList<KeyValue>>> batchRetrieve(AsyncHBaseState state, List<TridentTuple> tuples) {
        return state.get(mapper,tuples);
    }

    @Override
    public void execute(TridentTuple tuple, Deferred<ArrayList<KeyValue>> requests, final TridentCollector collector) {
        try {
            List<KeyValue> cells = requests.joinUninterruptibly(this.timeout);
            for ( KeyValue cell : cells ) {
                if (valueDeserializer != null) {
                    collector.emit(new Values(valueDeserializer.deserialize(cell.value())));
                } else {
                    collector.emit(new Values(cell.value()));
                }
            }
        } catch (Exception ex) {
            log.warn("AsyncHBase failure ", ex);
            collector.reportError(ex);
        }
    }
}
