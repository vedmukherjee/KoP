package com.evolve.integration.kafka.kop;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class KafkaApiVersionsResponse {
    public static ByteBuf encode(int correlationId) {
        ByteBuf body = Unpooled.buffer();

        // CorrelationId (from request)
        body.writeInt(correlationId);

        // ErrorCode = 0 (short)
        body.writeShort(0);

        // Number of ApiKeys we support
        body.writeInt(2);

        // 1. Produce (apiKey=0)
        body.writeShort(0);   // ApiKey
        body.writeShort(0);   // MinVersion
        body.writeShort(7);   // MaxVersion

        // 2. Metadata (apiKey=3)
        body.writeShort(3);   // ApiKey
        body.writeShort(0);   // MinVersion
        body.writeShort(9);   // MaxVersion

        // Final throttle_time_ms
        body.writeInt(0);
        // Wrap with size prefix
        ByteBuf frame = Unpooled.buffer();
        frame.writeInt(body.readableBytes()); // length prefix
        frame.writeBytes(body);
        return frame;
    }
}
