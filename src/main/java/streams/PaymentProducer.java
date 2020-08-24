package streams;

import avro.vo.Order;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;
import java.util.stream.Stream;

public class PaymentProducer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        try (KafkaProducer producer = new KafkaProducer(props)) {
            Stream.iterate(1, i -> i + 1)
                    .limit(10)
                    .forEach(i -> {
                                Order order = new Order("UserId-" + i, 1500, 10);
                                if (i % 2 == 0) {
                                    order.setTotalAmount(980 + i);
                                }
                            System.out.println("Sending record key-->" + i.toString()+" ,Value-->"+order.toString());
                                producer.send(new ProducerRecord<String,Order>
                                        ("payment", i.toString(), order));
                            }
                    );

        } catch (Exception e) {
            e.printStackTrace();

        }


    }
}
