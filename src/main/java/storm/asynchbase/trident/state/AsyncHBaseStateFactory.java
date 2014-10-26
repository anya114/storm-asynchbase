/*
 * Charles-Antoine Mathieu <charles-antoine.mathieu@ovh.net>
 */

package storm.asynchbase.trident.state;

import backtype.storm.task.IMetricsContext;
import storm.asynchbase.bolt.mapper.IAsyncHBaseMapper;
import storm.asynchbase.trident.mapper.IAsyncHBaseTridentMapper;
import storm.asynchbase.utils.AsyncHBaseClientFactory;
import storm.trident.state.State;
import storm.trident.state.StateFactory;

import java.util.Map;

/**
 * Factory to handle creation of AsyncHBaseState objects
 */
public class AsyncHBaseStateFactory implements StateFactory {
    private final String cluster;

    /**
     * @param cluster         HBase cluster to use
     */
    public AsyncHBaseStateFactory(String cluster) {
        this.cluster = cluster;
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
        return new AsyncHBaseState(AsyncHBaseClientFactory.getHBaseClient(conf, cluster));
    }
}