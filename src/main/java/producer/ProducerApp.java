package producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.stream.Stream;

public class ProducerApp {
    public static void main(String[] args) {
        Properties prop = new Properties();
        prop.put("bootstrap.servers", "localhost:9092,localhost:9093");
        prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer producer = new KafkaProducer(prop);
        /*
        //kafkaProducers only send producerRecords that match the key and value serialization
        ProducerRecord myMessage = new ProducerRecord("my_topic", "1", "my message from java");
        //producerRecords - mandatory details - topic , value . Other details - partition,timestamp,key
        producer.send(myMessage);
        */
        try{
            Stream.iterate(0, i -> i + 1)
                    .limit(10)
                    .forEach(i -> producer.send(new ProducerRecord("my_partition_topic", i.toString(), "New message - " + i)));
        }catch(Exception e){
            e.printStackTrace();

        }finally {
            producer.close();
        }

        /*
           Check output using
           .\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic my_partition_topic --from-beginning
           .\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9093 --topic my_partition_topic --from-beginning
            .\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9094 --topic my_partition_topic --from-beginning
            output - ..
            ..
            ..
            message - 140
            message - 143
            message - 146
            message - 147
            Processed a total of 150 messages
            Note messages are not ordered
         */

    }
}
