package consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class AssignConsumerApp {
    public static void main(String[] args) {
        Properties prop = new Properties();
        prop.put("bootstrap.servers", "localhost:9092,localhost:9093");
        prop.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        prop.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        prop.put("group.id","test");

        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(prop);
        TopicPartition tp0 = new TopicPartition("my_partition_topic_one",0);
        TopicPartition tp1 = new TopicPartition("my_partition_topic_two",2);
        consumer.assign(Arrays.asList(tp0,tp1));

        try{
            while(true){
                ConsumerRecords<String,String> records = consumer.poll(Duration.ofSeconds(1));
                for(ConsumerRecord record : records){
                    System.out.println(String.format("Topic:%s Partition:%s Offset:%s Key:%s Value:%s"
                            ,record.topic(),record.partition(),record.offset(),record.key(),record.value()));
                }
            }

        }catch(Exception e){
            e.printStackTrace();
        }
        finally {
            consumer.close();
        }

    }
}
/*
input -
send 10 msgs on my_partition_topic_one
send 10 msgs on my_partition_topic_two
1)  --topic my_partition_topic_two --num-records 10 --record-size 1 --throughput 10 --producer-props bootstrap.servers=localhost:9092 key.serializer=org.apache.kafka.common.serialization.StringSerializer value.serializer=org.apache.kafka.common.serialization.StringSerializer
2) --topic my_partition_topic_one --num-records 10 --record-size 1 --throughput 10 --producer-props bootstrap.servers=localhost:9092 key.serializer=org.apache.kafka.common.serialization.StringSerializer value.serializer=org.apache.kafka.common.serialization.StringSerializer


output -
Topic:my_partition_topic_two Partition:2 Offset:0 Key:null Value:S
Topic:my_partition_topic_two Partition:2 Offset:1 Key:null Value:S
Topic:my_partition_topic_two Partition:2 Offset:2 Key:null Value:S
Topic:my_partition_topic_one Partition:0 Offset:12 Key:null Value:S
Topic:my_partition_topic_one Partition:0 Offset:13 Key:null Value:S
Summary -
only listening to partition 2 of my_partition_topic_two
only listening to partition 0 of my_partition_topic_one

 */