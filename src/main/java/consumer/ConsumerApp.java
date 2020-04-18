package consumer;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class ConsumerApp {
    public static void main(String[] args) {
        Properties prop = new Properties();
        prop.put("bootstrap.servers", "localhost:9092,localhost:9093");
        prop.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        prop.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(prop);
        consumer.subscribe(Arrays.asList("my_partition_topic"));
        //subscribe method reads messages from every partition of topic
        //it helps you manage the partitions

        try{
            while(true){
                ConsumerRecords<String,String> records = consumer.poll(Duration.ofSeconds(1));
            }

        }finally {
            consumer.close();
        }

    }

    public static void assignPartition(KafkaConsumer consumer){
        TopicPartition tp0 = new TopicPartition("my_partition_topic",0);
        TopicPartition tp1 = new TopicPartition("my_partition_topic",1);
         consumer.assign(Arrays.asList(tp0,tp1));
    }
}
