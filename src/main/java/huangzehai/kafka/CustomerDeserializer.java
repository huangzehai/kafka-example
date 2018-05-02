package huangzehai.kafka;


import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;

public class CustomerDeserializer implements Deserializer<Customer> {
    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public Customer deserialize(String topic, byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        try {
            if (bytes.length >= 8) {
                byte[] idBytes = Arrays.copyOfRange(bytes, 0, 4);
                byte[] nameBytes = Arrays.copyOfRange(bytes, 8, bytes.length);
                int id = ByteBuffer.wrap(idBytes).getInt();
                String name = new String(nameBytes, "UTF-8");
                return new Customer(id, name);
            }
        } catch (Exception e) {
            throw new SerializationException("Error when serializing Customer to byte[]", e);
        }
        return null;
    }

    @Override
    public void close() {

    }
}
