package com.evolve.integration.pulsar.consumer;

import org.springframework.pulsar.annotation.PulsarListener;
import org.springframework.stereotype.Component;

@Component
public class PulsarMessageConsumer {

    @PulsarListener(subscriptionName = "my-subscription", topics = "my-topic-1")
    public void consume(String msg) {
        System.out.println("[PULSAR] Received message: " + msg);
    }
}
