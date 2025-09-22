package com.evolve.integration.kafka.kop;

import io.netty.buffer.ByteBuf;
import java.util.*;

public class KafkaProduceRequest {
    public final String topic;
    public final List<String> messages;

    private KafkaProduceRequest(String topic, List<String> messages) {
        this.topic = topic;
        this.messages = messages;
    }

    public static KafkaProduceRequest parse(ByteBuf buf) {
        // Skip request header
        int apiKey = buf.readShort();     // should be 0 for Produce
        short apiVersion = buf.readShort();
        int correlationId = buf.readInt();
        String clientId = readString(buf);

        // RequiredAcks
        short requiredAcks = buf.readShort();
        int timeout = buf.readInt();

        // [TopicData]
        int topicCount = buf.readInt();
        String topic = null;
        List<String> messages = new ArrayList<>();

        for (int i = 0; i < topicCount; i++) {
            topic = readString(buf);
            int partitionCount = buf.readInt();

            for (int p = 0; p < partitionCount; p++) {
                int partition = buf.readInt();
                int recordSetSize = buf.readInt();
                byte[] records = new byte[recordSetSize];
                buf.readBytes(records);
                messages.add(new String(records)); // naive: no compression, raw bytes
            }
        }

        return new KafkaProduceRequest(topic, messages);
    }

    private static String readString(ByteBuf buf) {
        short len = buf.readShort();
        byte[] bytes = new byte[len];
        buf.readBytes(bytes);
        return new String(bytes);
    }
}
