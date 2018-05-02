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
    public Customer deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        try {
            if (data.length >= 8) {
                int id;
                int nameSize;
                String name;
                ByteBuffer buffer = ByteBuffer.wrap(data);
                id = buffer.getInt();
                nameSize = buffer.getInt();
                byte[] nameBytes = new byte[nameSize];
                buffer.get(nameBytes);
                name = new String(nameBytes, "UTF-8");
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
