package huangzehai.kafka;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

public class ProducerExample {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092,localhost:9093");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");


        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("compression.type", "snappy");

        Producer<String, String> producer = new KafkaProducer<>(props);

        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>("test", String.valueOf(i), "Hey, Kafka " + i);
            producer.send(record, (recordMetadata, e) -> {
                if (recordMetadata != null) {
                    System.out.println(recordMetadata);
                }

                if (e != null) {
                    e.printStackTrace();
                }

            });
        }

        producer.close();
    }
}
