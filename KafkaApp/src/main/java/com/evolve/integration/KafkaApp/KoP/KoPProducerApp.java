package com.evolve.integration.KafkaApp;

import com.evolve.integration.kafka.producer.KafkaProducer;

public class ProducerApp {
    public static void main(String[] args) throws Exception {
        try (KafkaProducer producer = new KafkaProducer("pulsar://localhost:6650", "my-topic")) {
            producer.send("key1", "Hello Pulsar with Kafka-style Producer!");
            producer.send("key2", "Another message");
        }
    }
}

