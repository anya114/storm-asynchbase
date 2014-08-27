/*
 * Charles-Antoine Mathieu <charles-antoine.mathieu@ovh.net>
 */

package storm.asynchbase.bolt.mapper;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * This interface holds several fields mappers ( RPC configuration )
 */
public interface IAsyncHBaseMapper extends Serializable {

    /**
     * @return List of mappers/RPCs to execute.
     */
    List<IAsyncHBaseFieldMapper> getFieldMappers();

    /**
     * <p>
     * This method will initialize all mappers and serializers.<br/>
     * It will typically has to be called by the bolt prepare method.
     * </p>
     *
     * @param conf Topology configuration.
     */
    void prepare(Map conf);
}