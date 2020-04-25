package consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerApp {
    public static void main(String[] args) {
        Properties prop = new Properties();
        prop.put("bootstrap.servers", "localhost:9092,localhost:9093");
        prop.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        prop.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        prop.put("group.id","test");

        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(prop);
        consumer.subscribe(Arrays.asList("my_partition_topic_one"));//,"my_partition_topic_two"));
        //subscribe method reads messages from every partition of topic
        //it helps you manage the partitions

        try{
            while(true){
                ConsumerRecords<String,String> records = consumer.poll(Duration.ofSeconds(1));
                //poll method is single threaded application
                //consumer.seek(new TopicPartition("my_partition_topic_one",0),12); to seek a particular offset value
                //also check seekToBeginning ,seekToEnd
                for(ConsumerRecord record : records){
                    System.out.println(String.format("Topic:%s Partition:%s Offset:%s Key:%s Value:%s"
                            ,record.topic(),record.partition(),record.offset(),record.key(),record.value()));
                }
                consumer.commitSync();
                //this would commit the offset of the messages listened ,however it is a blocking call
                //this is manual way of commit ,automatic way is having property autocommit = true
                //this reduces the gap between 'last committed offset' and 'current position' that the consumer is listening to
                //consumer.commitAsync()
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

Create topic - .\bin\windows\kafka-topics.bat --create --topic my_partition_topic_one --zookeeper localhost:2181 --replication-factor 3 --partitions 3
input -  .\bin\windows\kafka-producer-perf-test.bat --topic my_partition_topic_one --num-records 50 --record-size 1 --throughput 10 --producer-props bootstrap.servers=localhost:9092 key.serializer=org.apache.kafka.common.serialization.StringSerializer value.serializer=org.apache.kafka.common.serialization.StringSerializer
Output -
...
...
Topic:my_partition_topic_one Partition:2 Offset:24 Key:null Value:S
Topic:my_partition_topic_one Partition:0 Offset:11 Key:null Value:S
Topic:my_partition_topic_one Partition:1 Offset:12 Key:null Value:S
summary - for topic - my_partition_topic_one 50 messages were sent
partition 0 - had 25 msgs
partition 1 - had 12 msgs
partition 2 - had 13 msgs
...
 */