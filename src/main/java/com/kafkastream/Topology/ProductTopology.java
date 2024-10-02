package com.kafkastream.Topology;
import com.kafkastream.config.AppConfig;
import com.kafkastream.model.BoProduct;
import com.kafkastream.model.JsonSerde;
import com.kafkastream.model.RiskProduct;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;

import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded;
import static org.apache.kafka.streams.kstream.Suppressed.untilTimeLimit;

public class ProductTopology {

        /**
         * Builds the Kafka Streams topology for processing products.
         *
         * The topology reads products from a source topic, filters products with a stock less than 10,
         * converts them to a RiskProduct format, and then writes them to a sink topic.
         *
         * @return the built topology.
         */
        public static Topology buildTopology() {
            // Create a custom Serde (Serializer/Deserializer) for BoProduct, which handles the serialization and deserialization
            // of BoProduct objects to and from JSON format. This is necessary to work with JSON data in Kafka Streams.
            Serde<BoProduct> boProductSerde = new JsonSerde<>(BoProduct.class);

            Serde<RiskProduct> riskProductSerde = new JsonSerde<>(RiskProduct.class);

            // Initialize a StreamsBuilder, which is used to construct the Kafka Streams topology.
            // The StreamsBuilder provides the high-level DSL to define the stream processing steps.
            StreamsBuilder builder = new StreamsBuilder();

            // Define a KStream to represent the source stream of data coming from the input topic.
            // The 'stream()' method creates a KStream from the specified input topic, using String as the key
            // and BoProduct as the value, with deserialization managed by the custom Serde.
            KStream<String, BoProduct> sourceStream = builder.stream(
                    AppConfig.getSourceTopic(), Consumed.with(Serdes.String(), boProductSerde));

            // Process the stream by filtering, mapping, and sending to an output topic.
            sourceStream

                    .peek((key, boProduct) -> System.out.println("Processing product: " + boProduct))
                    // Filter the stream to retain only products that are not null and have a stock count less than 10.
                    // This filters out products that are either null or have sufficient stock, keeping only those at risk.
                    // This method is stateless
                    .filter((key, boProduct) -> boProduct != null && boProduct.getStock() < 10)

                    // Transform each BoProduct in the stream into a RiskProduct. The transformation is done using the
                    // static method 'toRiskProduct', which takes a BoProduct object and returns a RiskProduct object.
                    .mapValues(ProductTopology::toRiskProduct)

                    // Write the resulting stream to the output topic specified in the application configuration.
                    // The 'Produced.with()' method specifies that the keys are serialized as Strings and the values
                    // are serialized using the custom RiskProduct Serde.
                    .to(AppConfig.getSinkTopic(), Produced.with(Serdes.String(), riskProductSerde));

            // Build the topology using the defined processing steps and return it.
            return builder.build();
        }


public static RiskProduct toRiskProduct(BoProduct boProduct) {
    return RiskProduct.builder()
            .id(boProduct.getId())
            .name(boProduct.getName())
            .stock(boProduct.getStock())
            .isTrendProduct(true)
            .build();
    }
}

