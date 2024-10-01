package com.kafkastream;

import com.kafkastream.Topology.ProductTopology;
import com.kafkastream.config.AppConfig;
import org.apache.kafka.streams.KafkaStreams;

public class KafkaStreamApplication {

    public static void main(String[] args) {
        KafkaStreams streams = new KafkaStreams(ProductTopology.buildTopology(), AppConfig.getKafkaProperties());

    streams.start();
    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

}
