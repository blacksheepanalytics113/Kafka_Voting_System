# Project Architecture

![Screenshot (49)](https://github.com/adunajiye/Kafka_Voting_System/assets/80220180/19859ab4-fad1-4c30-bb0c-4ba5a21a3375)

# Real-Time Voting System
This repository contains the code for a realtime election voting system. The system is built using Python, Kafka, Spark Streaming, Postgres and Metabase. The system is built using Docker Compose to easily spin up the required services in Docker containers.


# System Design 
1. creates the required tables on postgres (candidates, voters and votes), it also creates the Kafka topic and creates a copy of the votes table in the Kafka topic. It also contains the logic to consume the votes from the Kafka topic and produce data to voters_topic on Kafka.
2. consumes the votes from the Kafka topic (voters_topic), generate voting data and produce data to votes_topic on Kafka.


<div class="code-container">
  <button class="copy-button" data-clipboard-target="#example-code">Run the following command to check list of Kafka Topic</button>

  ```python
  docker exec -it container_name kafka-topics --list --bootstrap-server localhost:9092
 ```

<div class="code-container">
  <button class="copy-button" data-clipboard-target="#example-code">Run the following command to verify data produced to kafka Topic</button>

  ```python
 
docker exec -it container_name kafka-console-consumer --topic topic_name --bootstrap-server localhost:9092 --from-beginning
 ```

Data Produced into kafka Topics
![Screenshot (53)](https://github.com/adunajiye/Kafka_Voting_System/assets/80220180/2c9267f1-865c-469e-9337-e0da851523e3)
