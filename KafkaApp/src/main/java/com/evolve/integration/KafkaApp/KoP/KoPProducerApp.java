package com.evolve.integration.KafkaApp.KoP;

import java.util.Properties;

import com.evolve.integration.kafka.basic.producer.KafkaProducer;

public class KoPProducerApp {
    private static final String SERVICE_URL = "pulsar://localhost:6650";
    private static final String TOPIC = "persistent://public/default/my-topic";
    public static void main(String[] args) throws Exception {
        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", SERVICE_URL);
        producerProps.put("topic", TOPIC);
        try (KafkaProducer producer = new KafkaProducer(producerProps)) {
            producer.send("key1", "Hello Pulsar with Kafka-style Producer!");
            producer.send("key2", "Another message");
        }
    }
}

