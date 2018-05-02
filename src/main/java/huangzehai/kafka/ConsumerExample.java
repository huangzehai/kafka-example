package huangzehai.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.util.*;
import java.util.regex.Pattern;

@Slf4j
public class ConsumerExample {

    private Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
    private int count = 0;

    private KafkaConsumer<String, String> consumer;

    public static void main(String[] args) {
        ConsumerExample example = new ConsumerExample();
        example.consume();
    }

    public void consume() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test1");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "client1");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put("enable.auto.commit", "false");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("test"));
        final int minBatchSize = 200;
        List<ConsumerRecord<String, String>> buffer = new ArrayList<>();
        consumer.subscribe(Pattern.compile("test"), new HandleBalance());
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                log.info("consume: {}", record);
                buffer.add(record);

                currentOffsets.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1, "no metadat"));
                if (count % 1000 == 0) {
                    consumer.commitAsync(currentOffsets, null);
                }
                count++;
            }
            if (buffer.size() >= minBatchSize) {
//                insertIntoDb(buffer);
                consumer.commitSync();
                buffer.clear();
            }
        }
    }


    private class HandleBalance implements ConsumerRebalanceListener {

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> collection) {
            consumer.commitAsync(currentOffsets, null);
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> collection) {

        }
    }
}
