package com.evolve.integration.kafka.kop;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

public class NettyKafkaServer {
    private final int port;
    private final KafkaRequestHandler handler;
    private final KafkaProduceRequestHandler kafkaProduceRequestHandler;

    public NettyKafkaServer(int port, KafkaRequestHandler handler, KafkaProduceRequestHandler kafkaProduceRequestHandler) {
        this.port = port; this.handler = handler;
        this.kafkaProduceRequestHandler = kafkaProduceRequestHandler; 
    }

    public void start() throws InterruptedException {
        EventLoopGroup boss = new NioEventLoopGroup(1);
        EventLoopGroup worker = new NioEventLoopGroup();

        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(boss, worker)
             .channel(NioServerSocketChannel.class)
             .childHandler(new ChannelInitializer<SocketChannel>() {
                 @Override
                 protected void initChannel(SocketChannel ch) {
                    ChannelPipeline p = ch.pipeline();
                     /*p.addLast(new KafkaFrameDecoder());
                     p.addLast(new KafkaFrameEncoder());
                     p.addLast(handler);*/
                    //p.addLast(new KafkaFrameDecoder());
                    p.addLast(kafkaProduceRequestHandler);
                 }
             })
             .childOption(ChannelOption.SO_KEEPALIVE, true);

            ChannelFuture f = b.bind(port).sync();
            System.out.println("Kafka-on-Pulsar demo server listening on " + port);
            f.channel().closeFuture().sync();
        } finally {
            boss.shutdownGracefully();
            worker.shutdownGracefully();
        }
    }
}

