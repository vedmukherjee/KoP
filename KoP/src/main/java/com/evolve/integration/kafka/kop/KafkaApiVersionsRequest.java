package com.evolve.integration.kafka.kop;

import io.netty.buffer.ByteBuf;

public class KafkaApiVersionsRequest {
    public static int parseCorrelationId(ByteBuf buf) {
        buf.skipBytes(2); // apiKey
        buf.skipBytes(2); // apiVersion
        int correlationId = buf.readInt();
        return correlationId;
    }
}

