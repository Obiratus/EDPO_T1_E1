# Exercise 1 - Getting started with Kafka
## Task 2 - Experiments with Kafka
### 1. Producer Experiments
#### Batch Size & Processing Latency

Uses the main class [ProducerExperimentBatchSizeClicksProducer.java](../ClickStream-Producer/src/main/java/com/examples/ProducerExperimentBatchSizeClicksProducer.java)

```
$  java -cp pubsub-producer-1.0-SNAPSHOT-jar-with-dependencies.jar com.examples.ProducerExperimentBatchSizeClicksProducer
``` 

##### Experiment Setup
- Batch Sizes tested:
   - `1024 bytes`
   - `4096 bytes`
   - `16384 bytes`
   - `65536 bytes`
   - `262144 bytes`
   - `1048576 bytes`
- Total Messages: Each batch size sends `10,000` messages.
- Kafka Producer Settings:
   - `linger.ms = 5`: Ensures a consistent delay before sending messages.
   - `buffer.memory = 32MB`: Default Kafka buffer memory.

##### Metrics Measured
- Duration (ms): Total time taken to send all messages.
- Throughput (msgs/sec): The number of messages sent per second.
- Average Latency (ms): The average time it takes for a message to be sent and acknowledged by Kafka.

##### Experiment Results

| Batch Size | Messages | Duration (ms) | Throughput    | Avg Latency (ms) |
|------------|----------|---------------|---------------|------------------|
| 1024       | 10000    | 554           | 18050.54      | 57.17            |
| 4096       | 10000    | 392           | 25510.20      | 10.26            |
| 16384      | 10000    | 389           | 25706.94      | 6.60             |
| 65536      | 10000    | 386           | 25906.74      | 5.11             |
| 262144     | 10000    | 375           | 26666.67      | 9.53             |
| 1048576    | 10000    | 389           | 25706.94      | 9.91             |

---

##### Observations
Throughput:
   - Throughput increases significantly as the `batch.size` grows.
   - Larger batch sizes reduce the overhead of network communication by allowing more messages to be sent in a single request, thus improving efficiency.
   - Maximum throughput is achieved at a batch size of 262144, with 26666.67 messages/sec.

Latency Trade-off:
   - Average latency per message decreases and becomes negligible at batch sizes up to `65536`.
   - However, at larger batch sizes (e.g., `262144` and `1048576`), the average latency increases again to 9.53 ms and 9.91 ms respectively. This is due to the delays introduced by waiting to fill larger batches, despite the gains in throughput.

##### Key Insights for our project

Small Batch Sizes:
   - When the `batch.size` is too small (e.g., `1024`), both throughput and latency suffer due to frequent network communication and higher per-message overhead. --> We don't want that.

**Medium Batch Sizes**:
   - Moderate batch sizes (e.g., `16384 - 65536`) are balanced between throughput and latency. --> Seems suitable for us.

Large Batch Sizes:
   - While large batch sizes (e.g., `262144` or `1048576`) deliver very high throughput, they slightly compromise latency. --> We prefer immediate message delivery.

#### Load Testing



---

 
    

# Run the experiments
## Basic setup
1. Set the correct KAFKA_ADVERTISED_HOST_NAME in [docker-compose.yml](../docker/docker-compose.yml)

2. Start the Kafka and Zookeeper processes using Docker Compose:
    ```
    $ docker compose up
    ```

---
## Running Java services
All tests where done starting the services via console.

**Note**: All commands must be run from the corresponding /target subfolder.

### Producer & Consumer
There are many main classes for different testing purposes. We did not change the pom file all the time.
The poms therefore contain the following as a reminder:
```
<manifest>
   <mainClass>read_EDPO_T1_E1.md</mainClass>
</manifest>
```
To run the services the correct main class must be stated in the command.

Producer
```
$  java -cp pubsub-producer-1.0-SNAPSHOT-jar-with-dependencies.jar com.examples.<main_class>
```
e.g.

```
$  java -cp pubsub-producer-1.0-SNAPSHOT-jar-with-dependencies.jar com.examples.ProducerExperimentBatchSizeClicksProducer
```
Consumer
```
$  java -cp pubsub-consumer-1.0-SNAPSHOT-jar-with-dependencies.jar com.examples.<main_class>
```


