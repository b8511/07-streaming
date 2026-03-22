package org.example;

import io.confluent.kafka.serializers.KafkaJsonDeserializerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.example.data.Ride;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Properties;

public class ExampleConsumer {

    private final KafkaConsumer<String, Ride> consumer;
    private static final String TOPIC = "example_rides";
    private static final String GROUP_ID = "example_consumer_group.v1";

    public ExampleConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Secrets.KAFKA_BOOTSTRAP_SERVER);
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required username='"
                        + Secrets.KAFKA_CLUSTER_KEY + "' password='" + Secrets.KAFKA_CLUSTER_SECRET + "';");
        props.put("sasl.mechanism", "PLAIN");
        props.put("client.dns.lookup", "use_all_dns_ips");
        props.put("session.timeout.ms", "45000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "io.confluent.kafka.serializers.KafkaJsonDeserializer");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(KafkaJsonDeserializerConfig.JSON_VALUE_TYPE, Ride.class);

        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(List.of(TOPIC));
    }

    public void consumeRides() {
        System.out.println("Starting consumer on topic: " + TOPIC + " (group: " + GROUP_ID + ")");
        int emptyPolls = 0;

        while (emptyPolls < 5) {
            var records = consumer.poll(Duration.of(1, ChronoUnit.SECONDS));

            if (records.isEmpty()) {
                emptyPolls++;
                System.out.println("No records yet (" + emptyPolls + "/5 empty polls)...");
                continue;
            }

            emptyPolls = 0; // reset on successful poll
            System.out.println("Polled " + records.count() + " records");

            for (ConsumerRecord<String, Ride> record : records) {
                Ride ride = record.value();
                System.out.printf("  partition=%d offset=%d key=%s | DOLocationID=%d fare=%.2f distance=%.2f%n",
                        record.partition(), record.offset(), record.key(),
                        ride.DOLocationID, ride.fare_amount, ride.trip_distance);
            }
        }

        consumer.close();
        System.out.println("Consumer finished.");
    }

    public static void main(String[] args) {
        ExampleConsumer exampleConsumer = new ExampleConsumer();
        exampleConsumer.consumeRides();
    }
}
