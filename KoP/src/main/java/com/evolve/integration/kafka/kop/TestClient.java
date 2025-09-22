package com.evolve.integration.kafka.kop;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class TestClient {

    public static void main(String[] args) throws Exception {
        String host = "127.0.0.1";
        int port = 9092;
        String topic = "demo-topic";
        String message = "hello-from-client";

        try (Socket socket = new Socket(host, port)) {
            OutputStream out = socket.getOutputStream();
            InputStream in = socket.getInputStream();

            byte api = 1; // Produce
            byte[] topicBytes = topic.getBytes(StandardCharsets.UTF_8);
            byte[] payload = message.getBytes(StandardCharsets.UTF_8);

            ByteBuffer buf = ByteBuffer.allocate(1 + 4 + topicBytes.length + 8 + payload.length);
            buf.put(api);
            buf.putInt(topicBytes.length);
            buf.put(topicBytes);
            buf.putLong(payload.length);
            buf.put(payload);

            out.write(buf.array());
            out.flush();

            // read response
            byte[] header = in.readNBytes(1 + 4); // api + topicLen
            if (header.length < 5) throw new RuntimeException("Short read on header");

            ByteBuffer hbuf = ByteBuffer.wrap(header);
            byte respApi = hbuf.get();
            int topicLen = hbuf.getInt();

            byte[] topicRespBytes = in.readNBytes(topicLen);
            String respTopic = new String(topicRespBytes, StandardCharsets.UTF_8);

            byte[] lenBytes = in.readNBytes(8);
            long payloadLen = ByteBuffer.wrap(lenBytes).getLong();

            byte[] payloadResp = in.readNBytes((int) payloadLen);
            String respMsg = new String(payloadResp, StandardCharsets.UTF_8);

            System.out.printf("Response: api=%d topic=%s msg=%s%n", respApi, respTopic, respMsg);
        }
        /*Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        producer.send(new ProducerRecord<>("my-topic", "hello from KafkaProducer"),
                (metadata, exception) -> {
                    if (exception != null) {
                        exception.printStackTrace();
                    } else {
                        System.out.printf("Sent message to %s-%d @ offset %d%n",
                            metadata.topic(), metadata.partition(), metadata.offset());
                    }
                });*/
    }
}
