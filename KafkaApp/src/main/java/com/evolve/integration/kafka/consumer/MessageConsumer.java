package com.evolve.integration.kafka.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class MessageConsumer {

    @KafkaListener(topics = "my-topic-1", groupId = "my-group-id")
    public void listen(String message) {
        System.out.println("[KAFKA] Received message: " + message);
    }

}