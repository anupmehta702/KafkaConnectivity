package consumergroup;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerApp02 {
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
Topic:my_big_topic Partition:1 Offset:0 Key:4 Value:message - 4- abc
Topic:my_big_topic Partition:1 Offset:1 Key:6 Value:message - 6- abc
Topic:my_big_topic Partition:1 Offset:2 Key:10 Value:message - 10- abc
Topic:my_big_topic Partition:1 Offset:3 Key:12 Value:message - 12- abc
Topic:my_big_topic Partition:1 Offset:4 Key:13 Value:message - 13- abc
Topic:my_big_topic Partition:1 Offset:5 Key:14 Value:message - 14- abc
Topic:my_big_topic Partition:1 Offset:6 Key:18 Value:message - 18- abc
Topic:my_big_topic Partition:1 Offset:7 Key:19 Value:message - 19- abc
Topic:my_big_topic Partition:1 Offset:8 Key:20 Value:message - 20- abc
Topic:my_big_topic Partition:1 Offset:9 Key:24 Value:message - 24- abc
Topic:my_big_topic Partition:1 Offset:10 Key:26 Value:message - 26- abc
 */