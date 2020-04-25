package consumergroup;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerApp03 {
    public static void main(String[] args) {
        Properties prop = new Properties();
        prop.put("bootstrap.servers", "localhost:9092,localhost:9093");
        prop.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        prop.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        prop.put("group.id", "test");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(prop);
        consumer.subscribe(Arrays.asList("my_big_topic"));

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));

                for (ConsumerRecord record : records) {
                    System.out.println(String.format("Topic:%s Partition:%s Offset:%s Key:%s Value:%s"
                            , record.topic(), record.partition(), record.offset(), record.key(), record.value()));
                }


            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }

    }
}
/*
Topic:my_big_topic Partition:2 Offset:0 Key:0 Value:message - 0- abc
Topic:my_big_topic Partition:2 Offset:1 Key:2 Value:message - 2- abc
Topic:my_big_topic Partition:2 Offset:2 Key:3 Value:message - 3- abc
Topic:my_big_topic Partition:2 Offset:3 Key:9 Value:message - 9- abc
Topic:my_big_topic Partition:2 Offset:4 Key:16 Value:message - 16- abc
Topic:my_big_topic Partition:2 Offset:5 Key:29 Value:message - 29- abc
 */