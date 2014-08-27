/*
 * Charles-Antoine Mathieu <charles-antoine.mathieu@ovh.net>
 */

package storm.asynchbase.bolt.mapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * This class holds several fields mappers ( RPC configuration ) by name
 */
public class AsyncHBaseMapper implements IAsyncHBaseMapper {
    public static final Logger log = LoggerFactory.getLogger(AsyncHBaseMapper.class);
    private List<IAsyncHBaseFieldMapper> fieldMappers;

    /**
     * @param asyncHBaseFieldMapper Field mapper used to map tuple fields to RPC settings.
     * @return This so you can do method chaining.
     */
    public AsyncHBaseMapper addFieldMapper(IAsyncHBaseFieldMapper asyncHBaseFieldMapper) {
        if (this.fieldMappers == null) {
            this.fieldMappers = new ArrayList<>();
        }
        this.fieldMappers.add(asyncHBaseFieldMapper);
        return this;
    }

    /**
     * @return List of mappers/RPCs to execute.
     */
    @Override
    public List<IAsyncHBaseFieldMapper> getFieldMappers() {
        return fieldMappers;
    }

    /**
     * <p>
     * This method will initialize all mappers and serializers.<br/>
     * It will typically has to be called by the bolt prepare method.
     * </p>
     *
     * @param conf Topology configuration.
     */
    @Override
    public void prepare(Map conf) {
        for (IAsyncHBaseFieldMapper mapper : this.fieldMappers) {
            mapper.prepare(conf);
        }
    }
}