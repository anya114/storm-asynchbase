/*
 * Charles-Antoine Mathieu <charles-antoine.mathieu@ovh.net>
 */

package storm.asynchbase.utils.serializer;

import java.io.Serializable;
import java.util.Map;

public class BooleanSerializer implements AsyncHBaseSerializer, AsyncHBaseDeserializer, Serializable {
    @Override
    public Object deserialize(byte[] value) {
        return (value[0] & 0x01) != 0;
    }

    @Override
    public byte[] serialize(Object object) {
        byte[] ba = new byte[1];
        if ((Boolean) object) {
            ba[0] = 1;
        } else {
            ba[0] = 0;
        }
        return ba;
    }

    @Override
    public void prepare(Map conf) {

    }
}
