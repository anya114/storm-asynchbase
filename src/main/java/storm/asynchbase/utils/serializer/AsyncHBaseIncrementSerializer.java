/*
 * Charles-Antoine Mathieu <charles-antoine.mathieu@ovh.net>
 */

package storm.asynchbase.utils.serializer;

import java.util.Map;

/**
 * This interface represent a serializer
 * capable of returning a long increment amount from
 * a specific object to be stored to from HBase.
 */
public interface AsyncHBaseIncrementSerializer {
    /**
     * @param object Object to serialize
     * @return A long increment amount.
     */
    long serialize(Object object);

    /**
     * <p>
     * Initialize the serializer
     * </p>
     *
     * @param conf Topology configuration
     */
    void prepare(Map conf);
}
