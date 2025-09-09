package com.evolve.integration.KafkaApp.standalone;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaStandaloneConsumer {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092"); // Replace with your Kafka broker address
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", "my-group-id"); // A unique group ID for your consumer
        properties.setProperty("auto.offset.reset", "earliest"); // Start reading from the beginning if no offset is committed

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        String topicName = "my-topic";
        consumer.subscribe(Collections.singletonList(topicName));

        System.out.println("Listening for messages on topic: " + topicName);

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100)); // Poll for new records
                records.forEach(record -> {
                    System.out.println("Received message: Key = " + record.key() + ", Value = " + record.value() +
                            ", Partition = " + record.partition() + ", Offset = " + record.offset());
                });
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }
}