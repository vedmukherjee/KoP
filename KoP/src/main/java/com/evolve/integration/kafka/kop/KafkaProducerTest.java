package com.evolve.integration.kafka.kop;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.Future;

public class KafkaProducerTest {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        props.put("acks", "1"); // works with our fake response
        props.put("api.version.request", "false"); 
        props.put("inter.broker.protocol.version", "0.10.0"); 

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        ProducerRecord<String, String> record =
                new ProducerRecord<>("my-topic", "key-1", "hello from KafkaProducer!");

        Future<RecordMetadata> future = producer.send(record);

        RecordMetadata metadata = future.get(); // blocks until response
        System.out.printf("Message sent to topic=%s partition=%d offset=%d%n",
                metadata.topic(), metadata.partition(), metadata.offset());

        producer.close();
    }
}
