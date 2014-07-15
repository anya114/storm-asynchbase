/*
 * Charles-Antoine Mathieu <charles-antoine.mathieu@ovh.net>
 */

package storm.asynchbase.bolt.mapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class holds several fields mappers ( RPC configuration ) by name
 */
public class AsyncHBaseMapper implements IAsyncHBaseMapper {
    public static final Logger log = LoggerFactory.getLogger(AsyncHBaseMapper.class);
    private Map<String, IAsyncHBaseFieldMapper> asyncHBaseFieldMappers;

    /**
     * @param name                  Name of the RPC.<br/> Note: use unique names.
     * @param asyncHBaseFieldMapper Field mapper used to map tuple fields to RPC settings.
     * @return This so you can do method chaining.
     */
    public AsyncHBaseMapper addFieldMapper(String name, IAsyncHBaseFieldMapper asyncHBaseFieldMapper) {
        if (this.asyncHBaseFieldMappers == null) {
            this.asyncHBaseFieldMappers = new HashMap<>();
        }
        this.asyncHBaseFieldMappers.put(name, asyncHBaseFieldMapper);
        return this;
    }

    /**
     * @param names Names of the RPC to execute. If null it will return
     *              all available mappers.
     * @return List of mappers/RPCs to execute.
     */
    @Override
    public List<IAsyncHBaseFieldMapper> getFieldMappers(List<String> names) {
        if (names != null) {
            List<IAsyncHBaseFieldMapper> mappers = new ArrayList<>(names.size());
            for (String name : names) {
                mappers.add(asyncHBaseFieldMappers.get(name));
            }
            return mappers;
        } else {
            return new ArrayList<>(asyncHBaseFieldMappers.values());
        }
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
        for (IAsyncHBaseFieldMapper mapper : this.asyncHBaseFieldMappers.values()) {
            mapper.prepare(conf);
        }
    }
}