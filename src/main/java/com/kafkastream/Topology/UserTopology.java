package com.kafkastream.Topology;

import com.kafkastream.config.AppConfig;
import com.kafkastream.model.BoProduct;
import com.kafkastream.model.JsonSerde;
import com.kafkastream.model.RiskProduct;
import com.kafkastream.model.UserEvent;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;


public class UserTopology {

    public static final String sourceTopic = "user-events";
    public static final String sinkTopic = "fraud-monitoring";

    public static Topology buildTopology() {



        Serde<UserEvent> userEventSerde = new JsonSerde<>(UserEvent.class);
        // Create a StreamsBuilder instance to define the processing topology
        StreamsBuilder builder = new StreamsBuilder();
        // Create a KStream for user events from the "user-events" topic
        KStream<String, UserEvent> eventStream = builder.stream(
                "user-events", // Source topic from AppConfig
                Consumed.with(Serdes.String(), userEventSerde) // SerDes for key and value
        );
        // Step 1: Log each event using peek to inspect records without modifying them
        eventStream.peek((key, event) -> System.out.println("Processing event: " +  event));

        // Step 2: Filter the events to keep only "purchase" events
        KStream<String, UserEvent> purchaseStream = eventStream
                .filter((key, event) -> "purchase".equals(event.getEventType()));

        // Step 3: Anonymize the purchase events by transforming the value using mapValues
        KStream<String, UserEvent> anonymizedStream = purchaseStream
                .mapValues(UserTopology::anonymize);

        // Step 4: Change the key of each event to the userId using selectKey
        KStream<String, UserEvent> keyedByUserStream = anonymizedStream
                .selectKey((oldKey, event) -> event.getUserId());

        // Step 5: Write the cleaned and transformed events to the "fraud-monitoring" topic
        keyedByUserStream.to(
                "fraud-monitoring" , // Output topic from AppConfig
                Produced.with(Serdes.String(), userEventSerde ) // SerDes for key and value
        );

        return builder.build();
    }

    public static UserEvent anonymize(UserEvent event) {
        return new UserEvent(event.getUserId(), event.getEventType(), "****");
    }
}
