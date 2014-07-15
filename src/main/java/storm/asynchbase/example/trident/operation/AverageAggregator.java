/*
 * Charles-Antoine Mathieu <charles-antoine.mathieu@ovh.net>
 */

package storm.asynchbase.example.trident.operation;

import backtype.storm.tuple.Values;
import clojure.lang.Numbers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.BaseAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.List;

public class AverageAggregator extends BaseAggregator<List<Number>> {
    public static final Logger log = LoggerFactory.getLogger(AverageAggregator.class);

    @Override
    public List<Number> init(Object batchId, TridentCollector collector) {
        return new ArrayList<>();
    }

    @Override
    public void aggregate(List<Number> values, TridentTuple tuple, TridentCollector collector) {
        values.add((Number) tuple.getValue(0));
    }

    @Override
    public void complete(List<Number> values, TridentCollector collector) {
        Number sum = 0;
        for (Number value : values) {
            sum = Numbers.add(sum, value);
        }
        collector.emit(new Values(Numbers.divide(sum.doubleValue(), values.size())));
    }
}
