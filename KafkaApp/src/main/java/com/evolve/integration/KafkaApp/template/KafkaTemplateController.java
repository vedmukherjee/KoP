package com.evolve.integration.KafkaApp.template;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.evolve.integration.kafka.producer.MessageProducer;

@RestController
public class KafkaTemplateController {

    @Autowired
    private MessageProducer messageProducer;

    @PostMapping("/send")
    public String sendMessage(@RequestParam("message") String message) {
        messageProducer.sendMessage("my-topic-1", message);
        return "Message sent: " + message;
    }

}
