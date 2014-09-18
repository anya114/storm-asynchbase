/*
 * Charles-Antoine Mathieu <charles-antoine.mathieu@ovh.net>
 */

package storm.asynchbase.utils.serializer;

import org.hbase.async.Bytes;

import java.io.Serializable;
import java.util.Map;

public class FloatSerializer implements AsyncHBaseSerializer, AsyncHBaseDeserializer, Serializable {
    @Override
    public Object deserialize(byte[] value) {
        return Float.intBitsToFloat(Bytes.getInt(value));
    }

    @Override
    public byte[] serialize(Object object) {
        return Bytes.fromInt(Float.floatToIntBits((Integer) object));
    }


    @Override
    public void prepare(Map conf) {

    }
}
