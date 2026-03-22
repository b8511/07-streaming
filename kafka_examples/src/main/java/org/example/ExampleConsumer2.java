package org.example;

import io.confluent.kafka.serializers.KafkaJsonDeserializerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.example.data.PickupLocation;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Consumes PickupLocation events from "example_pickup_locations" using
 * manual offset commits (enable.auto.commit=false).
 *
 * After each batch is fully processed, the consumer commits the highest
 * offset per partition explicitly, giving full control over at-least-once
 * delivery semantics.
 */
public class ExampleConsumer2 {

    private final KafkaConsumer<String, PickupLocation> consumer;
    private static final String TOPIC = "example_pickup_locations";
    private static final String GROUP_ID = "example_consumer2_group.v1";

    public ExampleConsumer2() {
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
        // Disable auto-commit — we will commit offsets manually after processing
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(KafkaJsonDeserializerConfig.JSON_VALUE_TYPE, PickupLocation.class);

        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(List.of(TOPIC));
    }

    public void consumePickupLocations() {
        System.out.println("Starting consumer on topic: " + TOPIC + " (group: " + GROUP_ID + ")");
        int emptyPolls = 0;

        while (emptyPolls < 5) {
            var records = consumer.poll(Duration.of(1, ChronoUnit.SECONDS));

            if (records.isEmpty()) {
                emptyPolls++;
                System.out.println("No records yet (" + emptyPolls + "/5 empty polls)...");
                continue;
            }

            emptyPolls = 0;
            System.out.println("Polled " + records.count() + " records");

            // Track the highest offset per partition in this batch
            Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();

            for (ConsumerRecord<String, PickupLocation> record : records) {
                PickupLocation location = record.value();
                System.out.printf("  partition=%d offset=%d key=%s | PULocationID=%d pickup=%s%n",
                        record.partition(), record.offset(), record.key(),
                        location.PULocationID, location.tpep_pickup_datetime);

                // Commit offset = last processed offset + 1
                offsetsToCommit.put(
                        new TopicPartition(record.topic(), record.partition()),
                        new OffsetAndMetadata(record.offset() + 1));
            }

            // Commit only after the entire batch has been processed
            consumer.commitSync(offsetsToCommit);
            System.out.println("Committed offsets: " + offsetsToCommit);
        }

        consumer.close();
        System.out.println("Consumer finished.");
    }

    public static void main(String[] args) {
        ExampleConsumer2 consumer = new ExampleConsumer2();
        consumer.consumePickupLocations();
    }
}
