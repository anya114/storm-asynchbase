/*
 * Charles-Antoine Mathieu <charles-antoine.mathieu@ovh.net>
 */

package storm.asynchbase.utils.serializer;

import java.io.Serializable;
import java.util.Map;

/**
 * This interface represent a deserializer
 * capable of returning a specific object type
 * from a byte array fetched from HBase.
 */
public interface AsyncHBaseDeserializer extends Serializable {

    /**
     * @param value Byte array from HBase.
     * @return An Object of your specific type.
     */
    Object deserialize(byte[] value);

    /**
     * <p>
     * Initialize the deserializer
     * </p>
     *
     * @param conf Topology configuration
     */
    void prepare(Map conf);
}
