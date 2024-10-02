# Kafka Streams Product Topology , example 1  (checkout to the branch basic-example)
https://medium.com/@mhabib990/kafka-streams-introduction-article-1-861eb45eb06b

This project defines a Kafka Streams topology to process product data.

## Overview

The topology reads from a source topic (`bo-product`), filters products with a stock less than 10, converts them to `RiskProduct` objects, and writes the result to a sink topic (`product-risk`).

## How to Run

### Using Docker Compose
The required Kafka infrastructure can be started using `docker-compose.yml` located in the `resources` folder.

- Navigate to the `resources` folder and run:

    ```sh
    docker-compose up
    ```

### Start the Application
Run the main class (`KafkaStreamApplication`) to start the Kafka Streams application.

## Testing

### Unit Tests
Use `ProductTopologyTest` to validate the topology using `TopologyTestDriver`.

### Manual Testing
Produce messages to the `bo-product` topic:

```sh
kafka-console-producer.sh --broker-list localhost:9092 --topic bo-product --property "parse.key=true" --property "key.separator=:"
key1:{"id":"1","name":"ProductA","stock":100}
key1:{"id":"1","name":"ProductA","stock":5}
key1:{"id":"1","name":"ProductA","stock":3}
```

# Kafka Streams User Topology , example 2  (checkout to the branch stateless-operations)

This project defines a Kafka Streams simple topology to process user events .

## Overview

The `UserTopology` reads from a source topic (`user-events`), processes the events to:
1. Log each incoming event.
2. Filter for `purchase` events only.
3. Anonymize the data to remove sensitive information.
4. Set the key of each event to the `userId`.
5. Write the transformed events to a sink topic (`fraud-monitoring`).

## How to test 
### Unit Tests
Use `UserTopologyTest` to validate the topology using `TopologyTestDriver`.

### Manual Testing

### Using Docker Compose
The required Kafka infrastructure can be started using `docker-compose.yml` located in the `resources` folder.

- Navigate to the `resources` folder and run:

    ```sh
    docker-compose up
    ```

### Start the Application
Run the main class (`KafkaStreamApplication`) to start the Kafka Streams application.
Create topics `user-events` and `fraud-monitoring`

## Testing
Produce messages to the `user-events` topic and monitor the output on the `fraud-monitoring` topic.

Produce a sample `purchase` event using Kafka Console Producer:

```sh
kafka-console-producer.sh --broker-list localhost:9092 --topic user-events --property "parse.key=true" --property "key.separator=:"
> user1:{"userId":"user1", "eventType":"purchase", "cardId":"1234-5678-9012"}
```
