/*
 * Charles-Antoine Mathieu <charles-antoine.mathieu@ovh.net>
 */

package storm.asynchbase.trident.state;

import backtype.storm.task.IMetricsContext;
import storm.trident.state.State;
import storm.trident.state.StateFactory;

import java.util.Map;

/**
 * Factory to handle creation of AsyncHBaseState objects
 */
public class AsyncHBaseStateFactory implements StateFactory {
    private final AsyncHBaseState.Options options;

    /**
     * @param options State configuration
     */
    public AsyncHBaseStateFactory(AsyncHBaseState.Options options) {
        this.options = options;
    }

    /**
     * <p>
     * Factory method to create a AsyncHBaseState object
     * </p>
     *
     * @param conf           topology configuration.
     * @param metrics        metrics helper.
     * @param partitionIndex partition index handled by this state.
     * @param numPartitions  number of partition to handle.
     * @return An initialized AsyncHBaseState.
     */
    @Override
    public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
        if (this.options.queryMapper != null) {
            this.options.queryMapper.prepare(conf);
        }
        if (this.options.updateMapper != null) {
            this.options.updateMapper.prepare(conf);
        }
        return new AsyncHBaseState(this.options, conf);
    }
}