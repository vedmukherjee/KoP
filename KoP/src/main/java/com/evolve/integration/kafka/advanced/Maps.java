package com.evolve.integration.kafka.advanced;

import java.util.Locale;

import org.apache.pulsar.client.api.CompressionType;

public final class Maps {
    static CompressionType compression(String s){
        if (s==null) return CompressionType.NONE;
        switch (s.toUpperCase(Locale.ROOT)){
            case "GZIP": case "ZLIB": return CompressionType.ZLIB;
            case "LZ4": return CompressionType.LZ4;
            case "ZSTD": return CompressionType.ZSTD;
            case "SNAPPY": return CompressionType.SNAPPY;
            default: return CompressionType.NONE;
        }
    }
}
