package com.evolve.integration.kafka.kop;

public class Main {
    public static void main(String[] args) throws Exception {
        //final String pulsarUrl = System.getProperty("pulsar.service.url", "pulsar://127.0.0.1:6650");
        //final int port = Integer.parseInt(System.getProperty("kop.port", "9092"));

        final String pulsarUrl =  "pulsar://127.0.0.1:6650";
        final int port = Integer.parseInt("9092");

        PulsarProducerManager pm = new PulsarProducerManager(pulsarUrl);
        PulsarConsumerManager cm = new PulsarConsumerManager(pulsarUrl);
        OffsetManager om = new OffsetManager();
        GroupCoordinator gc = new GroupCoordinator();
        KafkaRequestHandler handler = new KafkaRequestHandler(pm, cm, om, gc);
        KafkaProduceRequestHandler kafkaProduceRequestHandler = new KafkaProduceRequestHandler(pm);

        NettyKafkaServer server = new NettyKafkaServer(port, handler, kafkaProduceRequestHandler);
        server.start();

        // On shutdown you'd call pm.close(), cm.close()
    }
}

