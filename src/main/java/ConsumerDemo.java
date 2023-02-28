
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());

    public static void main(String[] args) {
        log.info("I am a Kafka Consumer!");

        String groupId = "my-java-application";
        String topic = "demo_java";

        // create Producer Properties
        Properties properties = new Properties();

        // connect to Localhost
//        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        // connect to Conduktor Playground
        properties.setProperty("bootstrap.servers", "cluster.playground.cdkt.io:9092");
        properties.setProperty("security.protocol", "SASL_SSL");
 //       properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"16kymQRjYy0dsBv6i0FDAP\" password=\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiIxNmt5bVFSall5MGRzQnY2aTBGREFQIiwib3JnYW5pemF0aW9uSWQiOjY5MzY2LCJ1c2VySWQiOjgwMTM5LCJmb3JFeHBpcmF0aW9uQ2hlY2siOiI4NzdkYjkxZC00ZDQ2LTQzOTAtYjMxZS1kZTg4ZTNlOWQwMDAifX0.UFY_nHp1bMg7XWYwVhxKaPF3Fv9YCw_WWtDstArye2c");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"16kymQRjYy0dsBv6i0FDAP\" password=\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiIxNmt5bVFSall5MGRzQnY2aTBGREFQIiwib3JnYW5pemF0aW9uSWQiOjY5MzY2LCJ1c2VySWQiOjgwMTM5LCJmb3JFeHBpcmF0aW9uQ2hlY2siOiI4NzdkYjkxZC00ZDQ2LTQzOTAtYjMxZS1kZTg4ZTNlOWQwMDAifX0.UFY_nHp1bMg7XWYwVhxKaPF3Fv9YCw_WWtDstArye2c\";");
        properties.setProperty("sasl.mechanism", "PLAIN");

        // create consumer configs
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "earliest");

        // create a consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        // subscribe to a topic
        consumer.subscribe(Arrays.asList(topic));

        // poll for data
        while (true) {

            log.info("Polling");

            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(1000));

            for (ConsumerRecord<String, String> record: records) {
                log.info("Key: " + record.key() + ", Value: " + record.value());
                log.info("Partition: " + record.partition() + ", Offset: " + record.offset());
            }


        }


    }
}
