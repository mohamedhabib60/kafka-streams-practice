package com.kafkastream.topology;

import com.kafkastream.Topology.ProductKTableTopology;
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

public class ProductKTableTopologyTest {

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, BoProduct> inputTopic;
    private TestOutputTopic<String, RiskProduct> outputTopic;
    Properties props = new Properties();
    @BeforeEach
    public void setup() {
        // Configure test properties
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "product-streams-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonSerde.class.getName());

        // Create the TopologyTestDriver
        testDriver = new TopologyTestDriver(ProductKTableTopology.buildTopology(), props);

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
    public void testOnlyLatestUpdateIsSentToOutput() throws InterruptedException {
        // Given: Two updates for the same product (same key) with different stock values
        BoProduct firstUpdate = new BoProduct("1", "ProductA", 8);  // First update with stock = 8
        BoProduct secondUpdate = new BoProduct("1", "ProductA", 5); // Second update with stock = 5 (latest state)


        // When: Sending both updates to the input topic
        inputTopic.pipeInput("1", firstUpdate, 0L);
        inputTopic.pipeInput("1", secondUpdate, 5000L);

        // Then: Only the last update should be output to the output topic
        // Read the output topic and verify that only one record is output with the latest update
        RiskProduct result = outputTopic.readValue();
        assertNotNull(result);
        assertEquals("1", result.getId());
        assertEquals("ProductA", result.getName());
        assertEquals(5, result.getStock());// Should have the stock of the latest update

        // Ensure that there are no more records in the output topic
        assertTrue(outputTopic.isEmpty());
    }
}