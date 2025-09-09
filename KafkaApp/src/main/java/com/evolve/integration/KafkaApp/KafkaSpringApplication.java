package com.evolve.integration.KafkaApp;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages="com.evolve")
public class KafkaSpringApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaSpringApplication.class, args);
	}

}
