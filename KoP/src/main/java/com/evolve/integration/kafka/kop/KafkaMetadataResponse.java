package com.evolve.integration.kafka.kop;

import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class KafkaMetadataResponse {

    public static ByteBuf encode(int correlationId, List<String> topics) {
        ByteBuf payload = Unpooled.buffer();

        // Write correlationId
        payload.writeInt(correlationId);

        // Brokers array (1 broker)
        payload.writeInt(1);        // array length
        payload.writeInt(0);        // nodeId
        writeString(payload, "localhost"); // host
        payload.writeInt(9092);     // port
        writeNullableString(payload, "rack-1");

        // Cluster ID (nullable string)
        writeNullableString(payload, "demo-cluster");

        // Controller ID
        payload.writeInt(0);

        // Topics array
        payload.writeInt(topics.size());
        for (String topic : topics) {
            payload.writeByte(0); // errorCode = 0
            writeString(payload, topic);
            payload.writeByte(0); // isInternal = false

            // Partitions array
            payload.writeInt(1);
            payload.writeInt(0);   // errorCode
            payload.writeInt(0);   // partitionId
            payload.writeInt(0);   // leader = broker 0
            payload.writeInt(1);   // replicas array length
            payload.writeInt(0);   // replica nodeId
            payload.writeInt(1);   // ISR array length
            payload.writeInt(0);   // isr nodeId
        }

        // Wrap with size prefix
        ByteBuf frame = Unpooled.buffer();
        frame.writeInt(payload.readableBytes());
        frame.writeBytes(payload);
        return frame;
    }

    private static void writeString(ByteBuf buf, String s) {
        buf.writeShort(s.length());
        buf.writeBytes(s.getBytes());
    }

    private static void writeNullableString(ByteBuf buf, String s) {
        if (s == null) {
            buf.writeShort(-1);
        } else {
            writeString(buf, s);
        }
    }
}
