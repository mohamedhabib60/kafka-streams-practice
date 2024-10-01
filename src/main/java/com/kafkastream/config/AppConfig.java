package com.kafkastream.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

public class AppConfig {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String APPLICATION_ID = "product-streams-app";
    private static final String SOURCE_TOPIC = "bo-product";
    private static final String SINK_TOPIC = "product-risk";

    public static Properties getKafkaProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, "true");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        return props;
    }

    public static String getSourceTopic() {
        return SOURCE_TOPIC;
    }

    public static String getSinkTopic() {
        return SINK_TOPIC;
    }
}

