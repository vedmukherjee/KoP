package com.evolve.integration.kafka.kop;

/*import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class KafkaFrameDecoder extends ByteToMessageDecoder {
    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        // need at least 1 byte(api) + 4(topicLen)
        if (in.readableBytes() < 5) {
            return;
        }
        in.markReaderIndex();
        byte api = in.readByte();
        int topicLen = in.readInt();

        if (in.readableBytes() < topicLen + 8) {
            in.resetReaderIndex();
            return;
        }

        byte[] topicBytes = new byte[topicLen];
        in.readBytes(topicBytes);
        String topic = new String(topicBytes, StandardCharsets.UTF_8);

        long payloadLen = in.readLong();
        if (in.readableBytes() < payloadLen) {
            in.resetReaderIndex();
            return;
        }
        byte[] payload = new byte[(int) payloadLen];
        in.readBytes(payload);

        // Simple request POJO
        KafkaRequest req = new KafkaRequest(api, topic, payload);
        out.add(req);
    }

    public static class KafkaRequest {
        public final byte api;
        public final String topic;
        public final byte[] payload;
        public KafkaRequest(byte api, String topic, byte[] payload) {
            this.api = api; this.topic = topic; this.payload = payload;
        }
    }
}*/
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

public class KafkaFrameDecoder extends LengthFieldBasedFrameDecoder {

    public KafkaFrameDecoder() {
        // Kafka request: 4-byte length prefix
        super(1024 * 1024, 0, 4, 0, 4);
    }

    @Override
    protected Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
        return super.decode(ctx, in); // extracts one full frame
    }
    public static class KafkaRequest {
        public final byte api;
        public final String topic;
        public final byte[] payload;
        public KafkaRequest(byte api, String topic, byte[] payload) {
            this.api = api; this.topic = topic; this.payload = payload;
        }
    }
}

