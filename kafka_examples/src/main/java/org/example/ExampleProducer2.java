package org.example;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.example.data.PickupLocation;

import java.io.FileReader;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * Publishes PickupLocation events (PULocationID + pickup time) to the
 * "example_pickup_locations" topic, one event per ride row from rides.csv.
 * Each message is keyed by PULocationID so that all pickups in the same
 * location land on the same partition.
 */
public class ExampleProducer2 {

    private final KafkaProducer<String, PickupLocation> producer;
    private static final String TOPIC = "example_pickup_locations";

    public ExampleProducer2() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Secrets.KAFKA_BOOTSTRAP_SERVER);
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required username='"
                        + Secrets.KAFKA_CLUSTER_KEY + "' password='" + Secrets.KAFKA_CLUSTER_SECRET + "';");
        props.put("sasl.mechanism", "PLAIN");
        props.put("client.dns.lookup", "use_all_dns_ips");
        props.put("session.timeout.ms", "45000");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "io.confluent.kafka.serializers.KafkaJsonSerializer");

        producer = new KafkaProducer<>(props);
    }

    public List<PickupLocation> getPickupLocations() throws IOException, CsvException {
        var ridesStream = this.getClass().getResource("/rides.csv");
        var reader = new CSVReader(new FileReader(ridesStream.getFile()));
        reader.skip(1); // skip header row
        // column 7 = PULocationID, column 1 = tpep_pickup_datetime — use "now" to keep events fresh
        return reader.readAll().stream()
                .map(arr -> new PickupLocation(Long.parseLong(arr[7]), LocalDateTime.now()))
                .collect(Collectors.toList());
    }

    public void publishPickupLocations(List<PickupLocation> locations)
            throws ExecutionException, InterruptedException {
        System.out.println("Publishing " + locations.size() + " pickup locations to topic: " + TOPIC);
        for (PickupLocation location : locations) {
            var future = producer.send(
                    new ProducerRecord<>(TOPIC, String.valueOf(location.PULocationID), location),
                    (metadata, exception) -> {
                        if (exception != null) {
                            System.err.println("Failed to send: " + exception.getMessage());
                        } else {
                            System.out.printf("Sent → partition=%d offset=%d PULocationID=%d%n",
                                    metadata.partition(), metadata.offset(), location.PULocationID);
                        }
                    });

            future.get(); // wait for broker acknowledgement
            Thread.sleep(100);
        }

        producer.flush();
        producer.close();
        System.out.println("Done producing.");
    }

    public static void main(String[] args) throws IOException, CsvException, ExecutionException, InterruptedException {
        ExampleProducer2 producer = new ExampleProducer2();
        List<PickupLocation> locations = producer.getPickupLocations();
        producer.publishPickupLocations(locations);
    }
}
