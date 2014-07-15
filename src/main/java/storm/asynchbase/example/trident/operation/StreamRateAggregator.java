/*
 * Charles-Antoine Mathieu <charles-antoine.mathieu@ovh.net>
 */

package storm.asynchbase.example.trident.operation;

import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.BaseAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class StreamRateAggregator extends BaseAggregator<StreamRateAggregator.Rate> {
    public static final Logger log = LoggerFactory.getLogger(StreamRateAggregator.class);
    private final long interval;
    private Rate rate;

    public StreamRateAggregator(long interval) {
        this.interval = interval * 1000;
    }

    @Override
    public Rate init(Object batchId, TridentCollector collector) {
        if (this.rate == null) {
            this.rate = new Rate();
        }
        return this.rate;
    }

    @Override
    public void aggregate(Rate state, TridentTuple tuple, TridentCollector collector) {
        state.count++;
    }

    @Override
    public void complete(Rate state, TridentCollector collector) {
        long now = System.currentTimeMillis();
        if ((now - state.timestamp) > interval) {
            collector.emit(new Values(state.count / ((now - state.timestamp) / 1000)));
            state.timestamp = now;
            state.count = 0;
        }
    }

    static class Rate {
        long timestamp = System.currentTimeMillis();
        long count = 0;
    }

}
