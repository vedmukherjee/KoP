package com.evolve.integration.KafkaApp;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.pulsar.core.PulsarTemplate;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/messages")
public class PulsarController {

    @Autowired
    private PulsarTemplate<String> pulsarTemplate;

    @PostMapping
    public String publish(@RequestParam String msg) {
        pulsarTemplate.send("my-topic-1", msg);
        return "Sent: " + msg;
    }
}
