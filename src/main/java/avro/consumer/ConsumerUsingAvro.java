package avro.consumer;

import avro.vo.User;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerUsingAvro {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092,localhost:9093");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        props.put("schema.registry.url", "http://localhost:8081");
        props.put("specific.avro.reader","true");
        props.put("group.id","avro-test");
        KafkaConsumer<String, User> consumer = new KafkaConsumer<String, User>(props);
        consumer.subscribe(Arrays.asList("my_avro_topic"));

        try{
            while(true){
                ConsumerRecords<String,User> records = consumer.poll(Duration.ofSeconds(1));

                for(ConsumerRecord record : records){
                    System.out.println(String.format("Topic:%s Partition:%s Offset:%s Key:%s Value:%s"
                            ,record.topic(),record.partition(),record.offset(),record.key(),record.value()));
                }
                consumer.commitSync();

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
Output -
Topic:my_avro_topic Partition:0 Offset:10 Key:50 Value:{"userId": "50", "username": "Anup Mehta", "dateOfBirth": 1505}
Topic:my_avro_topic Partition:0 Offset:11 Key:51 Value:{"userId": "51", "username": "Anup Mehta", "dateOfBirth": 1505}
Topic:my_avro_topic Partition:0 Offset:12 Key:52 Value:{"userId": "52", "username": "Anup Mehta", "dateOfBirth": 1505}
Topic:my_avro_topic Partition:0 Offset:13 Key:53 Value:{"userId": "53", "username": "Anup Mehta", "dateOfBirth": 1505}
Topic:my_avro_topic Partition:0 Offset:14 Key:54 Value:{"userId": "54", "username": "Anup Mehta", "dateOfBirth": 1505}
Topic:my_avro_topic Partition:0 Offset:15 Key:55 Value:{"userId": "55", "username": "Anup Mehta", "dateOfBirth": 1505}
Topic:my_avro_topic Partition:0 Offset:16 Key:56 Value:{"userId": "56", "username": "Anup Mehta", "dateOfBirth": 1505}
Topic:my_avro_topic Partition:0 Offset:17 Key:57 Value:{"userId": "57", "username": "Anup Mehta", "dateOfBirth": 1505}
Topic:my_avro_topic Partition:0 Offset:18 Key:58 Value:{"userId": "58", "username": "Anup Mehta", "dateOfBirth": 1505}
Topic:my_avro_topic Partition:0 Offset:19 Key:59 Value:{"userId": "59", "username": "Anup Mehta", "dateOfBirth": 1505}

 */