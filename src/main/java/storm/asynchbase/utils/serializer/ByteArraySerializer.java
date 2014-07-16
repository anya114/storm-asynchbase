/*
 * Charles-Antoine Mathieu <charles-antoine.mathieu@ovh.net>
 */

package storm.asynchbase.utils.serializer;

    import java.util.Map;

public class ByteArraySerializer implements AsyncHBaseSerializer, AsyncHBaseDeserializer{
    @Override
    public Object deserialize(byte[] value) {
        return value;
    }

    @Override
    public byte[] serialize(Object object) {
        return (byte[]) object;
    }

    @Override
    public void prepare(Map conf) {

    }
}