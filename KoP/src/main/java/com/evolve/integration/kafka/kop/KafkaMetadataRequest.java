package com.evolve.integration.kafka.kop;

import java.util.ArrayList;
import java.util.List;

import io.netty.buffer.ByteBuf;

/*public class KafkaMetadataRequest {
    public final List<String> topics;

    private KafkaMetadataRequest(List<String> topics) {
        this.topics = topics;
    }

    /*public static KafkaMetadataRequest parse(ByteBuf buf) {
        short apiKey = buf.readShort();    // should be 3 for Metadata
        short apiVersion = buf.readShort();
        int correlationId = buf.readInt();
        String clientId = readString(buf);

        int topicCount = buf.readInt();
        List<String> topics = new ArrayList<>();
        for (int i = 0; i < topicCount; i++) {
            topics.add(readCompactString(buf));
        }

        return new KafkaMetadataRequest(topics);
    }

    private static String readString(ByteBuf buf) {
        short len = buf.readShort();
        byte[] bytes = new byte[len];
        buf.readBytes(bytes);
        return new String(bytes);
    }
    private static int readVarInt(ByteBuf buf) {
        int value = 0;
        int i = 0;
        int b;
        while (((b = buf.readByte()) & 0x80) != 0) {
            value |= (b & 0x7F) << i;
            i += 7;
            if (i > 28) throw new IllegalArgumentException("VarInt too long");
        }
        return value | (b << i);
    }

    private static String readCompactString(ByteBuf buf) {
        int len = readVarInt(buf) - 1;
        if (len < 0) return null;
        byte[] bytes = new byte[len];
        buf.readBytes(bytes);
        return new String(bytes);
    }

    public static KafkaMetadataRequest parse(ByteBuf buf) {
        short apiKey = buf.readShort();    // should be 3
        short apiVersion = buf.readShort();
        int correlationId = buf.readInt();

        String clientId;
        if (apiVersion >= 9) {
            clientId = readCompactString(buf); // flexible versions
            skipTaggedFields(buf);
        } else {
            clientId = readKafkaString(buf);   // classic
        }

        List<String> topics = new ArrayList<>();
        if (apiVersion >= 9) {
            int topicCount = readVarInt(buf); // compact array
            for (int i = 0; i < topicCount; i++) {
                topics.add(readCompactString(buf));
                skipTaggedFields(buf);
            }
            skipTaggedFields(buf); // request-level tagged fields
        } else {
            int topicCount = buf.readInt();
            for (int i = 0; i < topicCount; i++) {
                topics.add(readKafkaString(buf));
            }
        }

        return new KafkaMetadataRequest(topics);
    }

    private static String readKafkaString(ByteBuf buf) {
        short len = buf.readShort();
        if (len < 0) return null;
        byte[] bytes = new byte[len];
        buf.readBytes(bytes);
        return new String(bytes);
    }

    private static int readVarInt(ByteBuf buf) {
        int value = 0;
        int i = 0;
        int b;
        while (((b = buf.readByte()) & 0x80) != 0) {
            value |= (b & 0x7F) << i;
            i += 7;
            if (i > 28) throw new IllegalArgumentException("VarInt too long");
        }
        return value | (b << i);
    }

    private static String readCompactString(ByteBuf buf) {
        int len = readVarInt(buf) - 1;
        if (len < 0) return null;
        byte[] bytes = new byte[len];
        buf.readBytes(bytes);
        return new String(bytes);
    }

    private static void skipTaggedFields(ByteBuf buf) {
        int numTaggedFields = readVarInt(buf);
        for (int i = 0; i < numTaggedFields; i++) {
            int tag = readVarInt(buf);
            int size = readVarInt(buf);
            buf.skipBytes(size);
        }
    }
}*/
import io.netty.buffer.ByteBuf;
import java.util.*;

public class KafkaMetadataRequest {
    public final int correlationId;
    public final List<String> topics;

    private KafkaMetadataRequest(int correlationId, List<String> topics) {
        this.correlationId = correlationId;
        this.topics = topics;
    }

    public static KafkaMetadataRequest parse(ByteBuf buf) {
        // ---- Request header ----
        short apiKey = buf.readShort();      // should be 3
        short apiVersion = buf.readShort();  // version
        int correlationId = buf.readInt();
        String clientId = readString(buf);

        // ---- Request body (v7 simplified) ----
        int topicCount = buf.readInt();
        List<String> topics = new ArrayList<>();
        for (int i = 0; i < topicCount; i++) {
            String topic = readString(buf);
            topics.add(topic);
        }

        // Some versions have allowAutoTopicCreation (boolean)
        if (buf.isReadable(1)) {
            buf.readBoolean();
        }
        // Some versions have includeClusterAuthorizedOperations (boolean)
        if (buf.isReadable(1)) {
            buf.readBoolean();
        }
        // Some versions have includeTopicAuthorizedOperations (boolean)
        if (buf.isReadable(1)) {
            buf.readBoolean();
        }

        return new KafkaMetadataRequest(correlationId, topics);
    }

    private static String readString(ByteBuf buf) {
        short len = buf.readShort();
        if (len < 0) return null; // nullable string
        byte[] bytes = new byte[buf.readableBytes() < len ? buf.readableBytes()-2 : len];
        buf.readBytes(bytes);
        return new String(bytes);
    }
}

