package streams;

import avro.vo.Order;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import java.util.Properties;

public class FraudDetectionApplication {
    /**
     * Aim of the program is to read events from payment
     * key - transactionId , value Order
     * Filter the events whose totalAmount < 1000
     * and send the filtered messages payments
     *
     */
    //Note - please run PaymentProducer first to populate msgs on payment topic
    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "fraud-detection-application");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        props.put("specific.avro.reader","true");
        props.put("group.id","stream-test");

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, Order> stream = streamsBuilder.stream("payment");

        stream.peek(FraudDetectionApplication::printOnEnter)
                .filter((transactionId, order) -> order.getTotalAmount() < 1000)
                .mapValues((order) -> {
                    order.setUserId(order.getUserId().toString().toUpperCase());
                    return order;
                })
                .peek(FraudDetectionApplication::printOnExit)
                .to("validated-payments");

        Topology topology = streamsBuilder.build();

        KafkaStreams streams = new KafkaStreams(topology,props);
        streams.start();
        Thread.sleep(5000);
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));


    }

    private static void printOnExit(String s, Order order) {
        System.out.println("Printing filtered event, key - " + s + " value-" + order.toString());
    }

    private static void printOnEnter(String s, Order order) {
        System.out.println("Printing received event, key - " + s + " value-" + order.toString());
    }
}

/*
OUTPUT -
run command
PS C:\kafka_2.11-2.4.0> .\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic validated-payments --from-beginning
    USERID-2
    USERID-4
    USERID-6
    USERID-8
    USERID-10
Program output -
Printing received event, key - 1 value-{"userId": "UserId-1", "totalAmount": 1500, "nbOfItems": 10}
Printing received event, key - 2 value-{"userId": "UserId-2", "totalAmount": 982, "nbOfItems": 10}
Printing filtered event, key - 2 value-{"userId": "USERID-2", "totalAmount": 982, "nbOfItems": 10}
Printing received event, key - 3 value-{"userId": "UserId-3", "totalAmount": 1500, "nbOfItems": 10}
Printing received event, key - 4 value-{"userId": "UserId-4", "totalAmount": 984, "nbOfItems": 10}
Printing filtered event, key - 4 value-{"userId": "USERID-4", "totalAmount": 984, "nbOfItems": 10}
Printing received event, key - 5 value-{"userId": "UserId-5", "totalAmount": 1500, "nbOfItems": 10}
Printing received event, key - 6 value-{"userId": "UserId-6", "totalAmount": 986, "nbOfItems": 10}
Printing filtered event, key - 6 value-{"userId": "USERID-6", "totalAmount": 986, "nbOfItems": 10}
Printing received event, key - 7 value-{"userId": "UserId-7", "totalAmount": 1500, "nbOfItems": 10}
Printing received event, key - 8 value-{"userId": "UserId-8", "totalAmount": 988, "nbOfItems": 10}
Printing filtered event, key - 8 value-{"userId": "USERID-8", "totalAmount": 988, "nbOfItems": 10}
Printing received event, key - 9 value-{"userId": "UserId-9", "totalAmount": 1500, "nbOfItems": 10}
Printing received event, key - 10 value-{"userId": "UserId-10", "totalAmount": 990, "nbOfItems": 10}
Printing filtered event, key - 10 value-{"userId": "USERID-10", "totalAmount": 990, "nbOfItems": 10}

 */
