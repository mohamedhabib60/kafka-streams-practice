package com.kafkastream.Topology;

import com.kafkastream.config.AppConfig;
import com.kafkastream.model.BoProduct;
import com.kafkastream.model.JsonSerde;
import com.kafkastream.model.RiskProduct;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

import java.time.Duration;

import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded;
import static org.apache.kafka.streams.kstream.Suppressed.untilTimeLimit;

public class ProductKTableTopology {

    public static Topology buildTopology() {
        Serde<BoProduct> boProductSerde = new JsonSerde<>(BoProduct.class);

        Serde<RiskProduct> riskProductSerde = new JsonSerde<>(RiskProduct.class);

        StreamsBuilder builder = new StreamsBuilder();
        KTable<String, BoProduct> productTable = builder.table(
                AppConfig.getSourceTopic(),
                Consumed.with(Serdes.String(), boProductSerde),
                Materialized.as("product-store")
        );

        KTable<String, BoProduct> lowStockTable = productTable
                .filter((key, product) -> product.getStock() < 10)
                .suppress(untilTimeLimit(Duration.ofMillis(1000L), unbounded().withMaxRecords(2)));

        KTable<String, RiskProduct> riskProductTable = lowStockTable
                .mapValues(ProductKTableTopology::toRiskProduct);

        KStream<String, RiskProduct> riskProductStream = riskProductTable.toStream()
                .peek((key, boProduct) -> System.out.println("Processing product: " + boProduct));

        riskProductStream.to(AppConfig.getSinkTopic(), Produced.with(Serdes.String(), riskProductSerde));

        return builder.build();


    }


    public static RiskProduct toRiskProduct(BoProduct boProduct) {
        return RiskProduct.builder().id(boProduct.getId())
                .name(boProduct.getName())
                .stock(boProduct.getStock())
                .isTrendProduct(true)
                .build();
    }
}
