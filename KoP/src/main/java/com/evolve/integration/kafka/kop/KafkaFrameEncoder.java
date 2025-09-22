package com.evolve.integration.kafka.kop;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public class KafkaFrameEncoder extends MessageToByteEncoder<com.evolve.integration.kafka.kop.KafkaFrameEncoder.KafkaResponse> {
    @Override
    protected void encode(ChannelHandlerContext ctx, KafkaResponse msg, ByteBuf out) throws Exception {
        out.writeByte(msg.api);
        byte[] topicBytes = msg.topic.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        out.writeInt(topicBytes.length);
        out.writeBytes(topicBytes);
        out.writeLong(msg.payload.length);
        out.writeBytes(msg.payload);
    }

    public static class KafkaResponse {
        public final byte api;
        public final String topic;
        public final byte[] payload;
        public KafkaResponse(byte api, String topic, byte[] payload) {
            this.api = api; this.topic = topic; this.payload = payload;
        }
    }
}

