/*
 * Charles-Antoine Mathieu <charles-antoine.mathieu@ovh.net>
 */

package storm.asynchbase.utils.serializer;

import java.io.Serializable;
import java.util.Map;

public class StringSerializer implements AsyncHBaseSerializer, AsyncHBaseDeserializer, Serializable {
    @Override
    public Object deserialize(byte[] value) {
        return new String(value);
    }

    @Override
    public byte[] serialize(Object string) {
        return ((String) string).getBytes();
    }

    @Override
    public void prepare(Map conf) {
    }
}
