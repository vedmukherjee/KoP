package com.evolve.integration.kafka.kop;


import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.apache.pulsar.client.api.MessageId;

import com.evolve.integration.kafka.kop.KafkaFrameDecoder.KafkaRequest;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class KafkaRequestHandler extends SimpleChannelInboundHandler<KafkaRequest> {

    private final PulsarProducerManager producerManager;
    private final PulsarConsumerManager consumerManager;
    private final OffsetManager offsetManager;
    private final GroupCoordinator groupCoordinator;

    public KafkaRequestHandler(PulsarProducerManager p, PulsarConsumerManager c,
                               OffsetManager o, GroupCoordinator g) {
        this.producerManager = p;
        this.consumerManager = c;
        this.offsetManager = o;
        this.groupCoordinator = g;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, KafkaRequest msg) throws Exception {
        if (msg.api == KafkaMessages.API_PRODUCE) {
            handleProduce(ctx, msg);
        } else if (msg.api == KafkaMessages.API_FETCH) {
            handleFetch(ctx, msg);
        } else {
            // unknown
            ctx.writeAndFlush(new KafkaFrameEncoder.KafkaResponse((byte)-1,
                    msg.topic, "UNKNOWN_API".getBytes(StandardCharsets.UTF_8)));
        }
    }

    private void handleProduce(ChannelHandlerContext ctx, KafkaRequest req) {
        // payload is raw bytes to send
        try {
            CompletableFuture<MessageId> mf = producerManager.sendAsync(req.topic, req.payload);
            mf.whenComplete((msgId, ex) -> {
                if (ex != null) {
                    byte[] err = ("ERR:" + ex.getMessage()).getBytes(StandardCharsets.UTF_8);
                    ctx.writeAndFlush(new KafkaFrameEncoder.KafkaResponse(KafkaMessages.API_PRODUCE, req.topic, err));
                } else {
                    // map msgId to a kafka-like offset
                    long offset = offsetManager.record(req.topic, msgId);
                    String resp = "OK-offset:" + offset;
                    ctx.writeAndFlush(new KafkaFrameEncoder.KafkaResponse(KafkaMessages.API_PRODUCE, req.topic, resp.getBytes(StandardCharsets.UTF_8)));
                }
            });
        } catch (Exception e) {
            byte[] err = ("ERR:" + e.getMessage()).getBytes(StandardCharsets.UTF_8);
            ctx.writeAndFlush(new KafkaFrameEncoder.KafkaResponse(KafkaMessages.API_PRODUCE, req.topic, err));
        }
    }

    private void handleFetch(ChannelHandlerContext ctx, KafkaRequest req) {
        try {
            // A very simple approach: create/lookup a Pulsar consumer for the topic
            List<byte[]> messages = consumerManager.receiveBatch(req.topic, 10, 500);
            // Build a simple payload: join messages with newline
            int total = messages.size();
            StringBuilder sb = new StringBuilder();
            for (int i=0;i<messages.size();i++){
                sb.append(new String(messages.get(i), StandardCharsets.UTF_8));
                if (i!=messages.size()-1) sb.append("\\n");
            }
            // Send payload back
            ctx.writeAndFlush(new KafkaFrameEncoder.KafkaResponse(KafkaMessages.API_FETCH, req.topic, sb.toString().getBytes(StandardCharsets.UTF_8)));
        } catch (Exception e) {
            ctx.writeAndFlush(new KafkaFrameEncoder.KafkaResponse(KafkaMessages.API_FETCH, req.topic, ("ERR:"+e.getMessage()).getBytes(StandardCharsets.UTF_8)));
        }
    }
}

