package consumergroup;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.stream.Stream;

public class ProducerApp01 {
    public static void main(String[] args) {
        Properties prop = new Properties();
        prop.put("bootstrap.servers", "localhost:9092,localhost:9093");
        prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer producer = new KafkaProducer(prop);

        try{
            Stream.iterate(0, i -> i + 1)
                    .limit(30)
                    .forEach(i -> producer.send(new ProducerRecord("my_big_topic"
                            , i.toString(), "message - " + i+"- abc")));
        }catch(Exception e){
            e.printStackTrace();

        }finally {
            producer.close();
        }

    }
}
