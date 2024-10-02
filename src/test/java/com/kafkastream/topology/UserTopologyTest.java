package com.kafkastream.topology;

import com.kafkastream.Topology.UserTopology;
import com.kafkastream.model.BoProduct;
import com.kafkastream.model.JsonSerde;
import com.kafkastream.model.RiskProduct;
import com.kafkastream.model.UserEvent;
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

public class UserTopologyTest {

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, UserEvent> inputTopic;
    private TestOutputTopic<String, UserEvent> outputTopic;

    @BeforeEach
    public void setup() {
        // Configure test properties
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "product-streams-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonSerde.class.getName());

        // Create the TopologyTestDriver
        testDriver = new TopologyTestDriver(UserTopology.buildTopology(), props);

        // Define the input and output topics
        inputTopic = testDriver.createInputTopic(
                UserTopology.sourceTopic, new StringSerializer(), new JsonSerde<>(UserEvent.class).serializer());
        outputTopic = testDriver.createOutputTopic(
                UserTopology.sinkTopic, new StringDeserializer(), new JsonSerde<>(UserEvent.class).deserializer());
    }
    @Test
    public void testUserEventProcessing() {
        // Step 1: Create and send a test UserEvent
        UserEvent userEvent = new UserEvent("user1", "purchase", "1234-5678-9012");
        inputTopic.pipeInput("key1", userEvent);

        // Step 2: Read the output and verify the transformation
        UserEvent outputEvent = outputTopic.readValue();
        assertEquals("user1", outputEvent.getUserId());
        assertEquals("purchase", outputEvent.getEventType());
        assertEquals("****", outputEvent.getCardId());

        // Verify no more output events
        assertEquals(true, outputTopic.isEmpty());
    }

    @AfterEach
    public void tearDown() {
        testDriver.close();
    }

}
