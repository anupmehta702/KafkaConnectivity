package consumergroup;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerApp01 {
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
Topic:my_big_topic Partition:0 Offset:0 Key:1 Value:message - 1- abc
Topic:my_big_topic Partition:0 Offset:1 Key:5 Value:message - 5- abc
Topic:my_big_topic Partition:0 Offset:2 Key:7 Value:message - 7- abc
Topic:my_big_topic Partition:0 Offset:3 Key:8 Value:message - 8- abc
Topic:my_big_topic Partition:0 Offset:4 Key:11 Value:message - 11- abc
Topic:my_big_topic Partition:0 Offset:5 Key:15 Value:message - 15- abc
Topic:my_big_topic Partition:0 Offset:6 Key:17 Value:message - 17- abc
Topic:my_big_topic Partition:0 Offset:7 Key:21 Value:message - 21- abc
Topic:my_big_topic Partition:0 Offset:8 Key:22 Value:message - 22- abc
Topic:my_big_topic Partition:0 Offset:9 Key:23 Value:message - 23- abc
Topic:my_big_topic Partition:0 Offset:10 Key:25 Value:message - 25- abc
Topic:my_big_topic Partition:0 Offset:11 Key:27 Value:message - 27- abc
Topic:my_big_topic Partition:0 Offset:12 Key:28 Value:message - 28- abc

 */