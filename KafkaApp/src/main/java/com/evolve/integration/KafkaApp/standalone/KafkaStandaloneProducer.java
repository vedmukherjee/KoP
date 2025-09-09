package com.evolve.integration.KafkaApp.standalone;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaStandaloneProducer {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092"); // Replace with your Kafka broker address
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        String topicName = "my-topic";
        String key = "messageKey";
        String value = "Hello Kafka from Java!";

        ProducerRecord<String, String> record = new ProducerRecord<>(topicName, key, value);

        try {
            producer.send(record).get(); // .get() makes it synchronous, remove for asynchronous
            System.out.println("Message sent successfully: " + value);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
}