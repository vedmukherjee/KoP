package com.evolve.integration.kafka.kop;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

public class KafkaProduceRequestHandler extends SimpleChannelInboundHandler<ByteBuf> {

    private final PulsarProducerManager pulsarProducerManager;

    public KafkaProduceRequestHandler(PulsarProducerManager pulsarProducerManager) {
        this.pulsarProducerManager = pulsarProducerManager;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
        msg.markReaderIndex();
        short apiKey = msg.readShort();
        short apiVersion = msg.readShort();
        int correlationId = msg.readInt();   // needed to echo back
        //correlationId = KafkaApiVersionsRequest.parseCorrelationId(msg);
    // reset so parsers can re-read
        msg.resetReaderIndex();

        switch (apiKey) {
            case 0: // ProduceRequest
                // Parse request
                KafkaProduceRequest req = KafkaProduceRequest.parse(msg);

                // Forward each record to Pulsar
                for (String record : req.messages) {
                    try {
                        pulsarProducerManager.send(req.topic, record);
                        System.out.println("Produced to topic " + req.topic + ": " + record);
                    } catch (Exception e) {
                        e.printStackTrace();
                        // TODO: return an error response instead of ack
                    }
                }

                // Build and send response back to Kafka client
                ByteBuf response = KafkaProduceResponse.encode(correlationId); // TODO: use real correlationId
                ctx.writeAndFlush(response);
                break;

            case 3: // MetadataRequest
                KafkaMetadataRequest metaReq = KafkaMetadataRequest.parse(msg);
                ctx.writeAndFlush(KafkaMetadataResponse.encode(correlationId, metaReq.topics));
                break;
            case 18: // ApiVersionsRequest
                ctx.writeAndFlush(KafkaApiVersionsResponse.encode(correlationId));
                break;

            default:
                System.err.println("Unsupported API key: " + apiKey);
                break;
        }
    }
}
