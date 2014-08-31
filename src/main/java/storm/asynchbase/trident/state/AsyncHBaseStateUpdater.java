/*
 * Charles-Antoine Mathieu <charles-antoine.mathieu@ovh.net>
 */

package storm.asynchbase.trident.state;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseStateUpdater;
import storm.trident.tuple.TridentTuple;

import java.util.List;

/**
 * Update an AsyncHBaseState<br/>
 * Use this function with Stream.partitionAggregate(State,Aggregator,StateUpdater,...)
 */
public class AsyncHBaseStateUpdater extends BaseStateUpdater<AsyncHBaseState> {
    public static final Logger log = LoggerFactory.getLogger(AsyncHBaseStateUpdater.class);

    /**
     * @param state     AsyncHBaseState to update.
     * @param tuples    Storm Tuples to process.
     * @param collector Unused as PutRequest returns void.
     */
    @Override
    public void updateState(final AsyncHBaseState state,
                            final List<TridentTuple> tuples,
                            final TridentCollector collector) {
        state.updateState(tuples, collector);
    }
}
