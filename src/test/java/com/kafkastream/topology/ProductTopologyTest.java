package com.kafkastream.topology;

import com.kafkastream.Topology.ProductTopology;
import com.kafkastream.config.AppConfig;
import com.kafkastream.model.BoProduct;
import com.kafkastream.model.JsonSerde;
import com.kafkastream.model.RiskProduct;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ProductTopologyTest {

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, BoProduct> inputTopic;
    private TestOutputTopic<String, RiskProduct> outputTopic;

    @BeforeEach
    public void setup() {
        // Configure test properties
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "product-streams-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonSerde.class.getName());

        // Create the TopologyTestDriver
        testDriver = new TopologyTestDriver(ProductTopology.buildTopology(), props);

        // Define the input and output topics
        inputTopic = testDriver.createInputTopic(
                AppConfig.getSourceTopic(), new StringSerializer(), new JsonSerde<>(BoProduct.class).serializer());
        outputTopic = testDriver.createOutputTopic(
                AppConfig.getSinkTopic(), new StringDeserializer(), new JsonSerde<>(RiskProduct.class).deserializer());
    }

    @AfterEach
    public void tearDown() {
        testDriver.close();
    }

    @Test
    public void testFilterAndMap() {
        // Given: A product with stock less than 10
        BoProduct product = BoProduct.builder().id("1").name("LowStockProduct").stock(5).build();

        new BoProduct("1", "LowStockProduct", 5);
        inputTopic.pipeInput("key1", product);

        // When: Consuming from output topic
        RiskProduct result = outputTopic.readValue();

        // Then: The product should be converted to RiskProduct
        assertNotNull(result);
        assertEquals(result.getId(), "1");
        assertEquals(result.getName(), "LowStockProduct");
        assertEquals(result.getStock(), 5) ;
        assertTrue(result.isTrendProduct());

        // And: There should be no more messages in the output topic
        assertTrue(outputTopic.isEmpty());
    }

    @Test
    public void testProductWithStockGreaterThan10IsFilteredOut() {
        // Given: A product with stock greater than or equal to 10
        BoProduct product =BoProduct.builder().id("2").name("HighStockProduct").stock(15).build();

        inputTopic.pipeInput("key2", product);

        // When: Consuming from output topic

        // Then: The output topic should be empty since the product does not meet the criteria
        assertTrue(outputTopic.isEmpty());
    }
}
