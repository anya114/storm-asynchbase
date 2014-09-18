/*
 * Charles-Antoine Mathieu <charles-antoine.mathieu@ovh.net>
 */

package storm.asynchbase.utils.serializer;

import org.hbase.async.Bytes;

import java.io.Serializable;
import java.util.Map;

public class DoubleSerializer implements AsyncHBaseSerializer, AsyncHBaseDeserializer, Serializable {
    @Override
    public Object deserialize(byte[] value) {
        return Double.longBitsToDouble(Bytes.getLong(value));
    }

    @Override
    public byte[] serialize(Object object) {
        return Bytes.fromLong(Double.doubleToLongBits((Double) object));
    }

    @Override
    public void prepare(Map conf) {

    }
}
