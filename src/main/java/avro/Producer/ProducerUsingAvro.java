package avro.Producer;

import avro.vo.User;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.stream.Stream;

public class ProducerUsingAvro {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092,localhost:9093");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put("schema.registry.url", "http://localhost:8081");
        KafkaProducer<String, User> producer = new KafkaProducer(props);
        try {
            Stream.iterate(60, i -> i + 1)
                    .limit(10)
                    .forEach(i -> {
                                System.out.println("Sending record with key --> "+i.toString());
                                producer.send(new ProducerRecord<String, User>
                                        ("my_avro_topic", i.toString(), new User(i.toString(), "Anup Mehta", 1505)));
                            }
                    );

        } catch (Exception e) {
            e.printStackTrace();

        } finally {
            producer.close();
        }


    }
}
