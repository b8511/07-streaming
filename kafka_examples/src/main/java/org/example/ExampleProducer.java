package org.example;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.example.data.Ride;

import java.io.FileReader;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class ExampleProducer {

    private final KafkaProducer<String, Ride> producer;
    private static final String TOPIC = "example_rides";

    public ExampleProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Secrets.KAFKA_BOOTSTRAP_SERVER);
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required username='"
                        + Secrets.KAFKA_CLUSTER_KEY + "' password='" + Secrets.KAFKA_CLUSTER_SECRET + "';");
        props.put("sasl.mechanism", "PLAIN");
        props.put("client.dns.lookup", "use_all_dns_ips");
        props.put("session.timeout.ms", "45000");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "io.confluent.kafka.serializers.KafkaJsonSerializer");

        producer = new KafkaProducer<>(props);
    }

    public List<Ride> getRides() throws IOException, CsvException {
        var ridesStream = this.getClass().getResource("/rides.csv");
        var reader = new CSVReader(new FileReader(ridesStream.getFile()));
        reader.skip(1); // skip header row
        return reader.readAll().stream()
                .map(Ride::new)
                .collect(Collectors.toList());
    }

    public void publishRides(List<Ride> rides) throws ExecutionException, InterruptedException {
        System.out.println("Publishing " + rides.size() + " rides to topic: " + TOPIC);
        for (Ride ride : rides) {
            // Shift timestamps to "now" so the consumer sees recent data
            ride.tpep_pickup_datetime = LocalDateTime.now().minusMinutes(5);
            ride.tpep_dropoff_datetime = LocalDateTime.now();

            var future = producer.send(
                    new ProducerRecord<>(TOPIC, String.valueOf(ride.DOLocationID), ride),
                    (metadata, exception) -> {
                        if (exception != null) {
                            System.err.println("Failed to send ride: " + exception.getMessage());
                        } else {
                            System.out.printf("Sent → partition=%d offset=%d DOLocationID=%d%n",
                                    metadata.partition(), metadata.offset(), ride.DOLocationID);
                        }
                    });

            future.get(); // wait for acknowledgement
            Thread.sleep(200);
        }

        producer.flush();
        producer.close();
        System.out.println("Done producing.");
    }

    public static void main(String[] args) throws IOException, CsvException, ExecutionException, InterruptedException {
        ExampleProducer exampleProducer = new ExampleProducer();
        List<Ride> rides = exampleProducer.getRides();
        exampleProducer.publishRides(rides);
    }
}
