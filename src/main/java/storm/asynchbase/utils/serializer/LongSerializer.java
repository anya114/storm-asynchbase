/*
 * Charles-Antoine Mathieu <charles-antoine.mathieu@ovh.net>
 */

package storm.asynchbase.utils.serializer;

import org.hbase.async.Bytes;

import java.io.Serializable;
import java.util.Map;

public class LongSerializer implements AsyncHBaseSerializer, AsyncHBaseDeserializer, Serializable {
    @Override
    public Object deserialize(byte[] value) {
        return Bytes.getLong(value);
    }

    @Override
    public byte[] serialize(Object object) {
        return Bytes.fromLong((Integer) (object));
    }

    @Override
    public void prepare(Map conf) {

    }
}
