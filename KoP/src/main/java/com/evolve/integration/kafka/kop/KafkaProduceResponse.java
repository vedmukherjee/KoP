package com.evolve.integration.kafka.kop;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class KafkaProduceResponse {

    public static ByteBuf encode(int correlationId) {
        ByteBuf payload = Unpooled.buffer();
        payload.writeInt(correlationId);
        // [TopicData array length = 0] => ack success
        payload.writeInt(0);

        // Wrap with size prefix
        ByteBuf frame = Unpooled.buffer();
        frame.writeInt(payload.readableBytes());
        frame.writeBytes(payload);
        return frame;
    }
}

