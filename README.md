# Kafka Streams Product Topology

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

