package huangzehai.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

@Slf4j
public class StandaloneConsumer {
    private KafkaConsumer<String, Customer> consumer;

    public static void main(String[] args) {
        StandaloneConsumer consumer = new StandaloneConsumer();
        consumer.consume();

        final Thread mainThread = Thread.currentThread();
        // Registering a shutdown hook so we can exit cleanly
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Starting exit...");
            // Note that shutdownhook runs in a separate thread, so the only thing we can safely do to a consumer is wake it up
            consumer.consumer.wakeup();
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));
    }

    private void consume() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test1");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "client1");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put("enable.auto.commit", "false");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "huangzehai.kafka.CustomerDeserializer");
        consumer = new KafkaConsumer<>(props);

        List<PartitionInfo> partitionInfos = consumer.partitionsFor("consumer");
        if (partitionInfos != null) {
            List<TopicPartition> partitions = partitionInfos.stream().map(partition -> new TopicPartition(partition.topic(), partition.partition())).collect(Collectors.toList());
            consumer.assign(partitions);
            try {
                while (true) {
                    ConsumerRecords<String, Customer> records = consumer.poll(1000);
                    for (ConsumerRecord<String, Customer> record : records) {
                        log.info("topic = {}, partition = {}, offset = {}, customer = {}, country = {}", record.topic(), record.partition(), record.offset(), record.key(), record.value());
                    }
                    consumer.commitSync();
                }
            } catch (WakeupException e) {
                e.printStackTrace();
            } finally {
                consumer.close();
                log.info("consumer close.");
            }
        }
    }
}
