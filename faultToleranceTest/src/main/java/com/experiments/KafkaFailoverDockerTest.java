import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertTrue;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class KafkaFailoverReportTest {

    private final String topic = "test-replication-topic";
    private KafkaProducer<String, String> producer;
    private KafkaConsumer<String, String> consumer;
    private AdminClient adminClient;

    // Test metrics
    private final Map<String, Object> testMetrics = new LinkedHashMap<>();
    private int totalProducedMessages = 0;
    private int totalConsumedMessages = 0;

    @BeforeAll
    public void setup() throws Exception {
        setupAdminClient();
        setupProducer();
        setupConsumer();

        // Create the test topic with 3 partitions and a replication factor of 3
        createTopic(topic, 3, 3);

        // Initialize test metrics
        testMetrics.put("Failover Test", "Kafka Fault Tolerance and Failover Simulation");
        testMetrics.put("Topic Name", topic);
    }

    @AfterAll
    public void cleanup() {
        if (producer != null) producer.close();
        if (consumer != null) consumer.close();
        if (adminClient != null) adminClient.close();

        // Print final report
        printTestReport();
    }

    @Test
    public void testKafkaFailover() throws Exception {
        // Start producing messages
        Thread producerThread = new Thread(() -> startProducingMessages(producer));
        producerThread.start();

        // Start consuming messages
        Thread consumerThread = new Thread(() -> startConsumingMessages(consumer));
        consumerThread.start();

        // Let the system stabilize
        Thread.sleep(10000); // 10 seconds

        // Simulate broker failure (kill a broker)
        long killStartTime = System.currentTimeMillis();
        killBroker("kafka1");
        long timeToKill = System.currentTimeMillis() - killStartTime;
        testMetrics.put("Broker Kill Time (ms)", timeToKill);
        System.out.println("Broker kafka1 stopped. Observing failover...");

        // Measure leader election time
        long failoverStartTime = System.currentTimeMillis();
        boolean allLeadersElected = waitForLeaderElection();
        long failoverTime = System.currentTimeMillis() - failoverStartTime;

        testMetrics.put("Leader Election Time (ms)", failoverTime);
        assertTrue(allLeadersElected, "Leader election did not complete successfully");

        // Restart the failed broker
        long restartStartTime = System.currentTimeMillis();
        restartBroker("kafka1");
        long restartTime = System.currentTimeMillis() - restartStartTime;

        testMetrics.put("Broker Restart Time (ms)", restartTime);
        System.out.println("Broker kafka1 restarted.");

        // Wait for 10 seconds for recovery
        Thread.sleep(10000);

        // Stop threads
        producerThread.interrupt();
        consumerThread.interrupt();

        // Record produced and consumed message counts in the report
        testMetrics.put("Total Produced Messages", totalProducedMessages);
        testMetrics.put("Total Consumed Messages", totalConsumedMessages);
        testMetrics.put("Test Successful", allLeadersElected && totalConsumedMessages > 0);
    }

    /** Create a Kafka topic */
    private void createTopic(String topicName, int partitions, int replicationFactor) throws ExecutionException, InterruptedException {
        NewTopic newTopic = new NewTopic(topicName, partitions, (short) replicationFactor);
        adminClient.createTopics(Collections.singleton(newTopic)).all().get();
        System.out.printf("Topic [%s] created with %d partitions and replication factor of %d.%n", topicName, partitions, replicationFactor);
    }

    /** Simulate stopping a broker using Docker */
    private void killBroker(String brokerName) throws Exception {
        System.out.printf("Stopping broker [%s]...%n", brokerName);
        Runtime.getRuntime().exec(String.format("docker stop %s", brokerName)); // Stop Kafka container
    }

    /** Simulate restarting a broker using Docker */
    private void restartBroker(String brokerName) throws Exception {
        System.out.printf("Restarting broker [%s]...%n", brokerName);
        Runtime.getRuntime().exec(String.format("docker start %s", brokerName)); // Start Kafka container
    }

    /** Wait for leader election to finish */
    private boolean waitForLeaderElection() throws InterruptedException, ExecutionException {
        boolean allElected = false;
        long timeout = System.currentTimeMillis() + 30000; // 30-second timeout

        while (System.currentTimeMillis() < timeout) {
            Map<String, TopicDescription> topicDescriptions = getTopicDescriptions();
            allElected = topicDescriptions.get(topic)
                    .partitions()
                    .stream()
                    .allMatch(p -> p.leader() != null);  // Ensure all partitions have leaders
            if (allElected) break; // If all partitions have elected leaders, exit the loop
            Thread.sleep(1000);
        }

        return allElected;
    }

    /** Producer logic to send messages continuously */
    private void startProducingMessages(KafkaProducer<String, String> producer) {
        try {
            int key = 0;
            while (true) {
                String value = "Message-" + key;
                producer.send(new ProducerRecord<>(topic, Integer.toString(key), value));
                totalProducedMessages++;
                System.out.printf("Produced: %s%n", value);
                Thread.sleep(200); // 5 messages per second
                key++;
                if (Thread.currentThread().isInterrupted()) break;
            }
        } catch (Exception ignored) {
        }
    }

    /** Consumer logic to continuously consume messages */
    private void startConsumingMessages(KafkaConsumer<String, String> consumer) {
        consumer.subscribe(Collections.singleton(topic));
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    totalConsumedMessages++;
                    System.out.printf("Consumed: key=%s, value=%s, partition=%d, offset=%d%n",
                            record.key(), record.value(), record.partition(), record.offset());
                }
                if (Thread.currentThread().isInterrupted()) break;
            }
        } catch (WakeupException ignored) {
        }
    }

    /** Setup Kafka producer */
    public void setupProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("acks", "all");
        producer = new KafkaProducer<>(props);
    }

    /** Setup Kafka consumer */
    public void setupConsumer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094");
        props.put("group.id", "test-consumer-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest");
        props.put("enable.auto.commit", "true");
        consumer = new KafkaConsumer<>(props);
    }

    /** Setup Kafka admin client */
    public void setupAdminClient() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094");
        adminClient = AdminClient.create(props);
    }

    /** Fetch topic details using Kafka AdminClient */
    private Map<String, TopicDescription> getTopicDescriptions() throws ExecutionException, InterruptedException {
        return adminClient.describeTopics(Collections.singleton(topic)).all().get();
    }

    /** Print the test report */
    private void printTestReport() {
        System.out.println("\n========== Kafka Failover Test Report ==========");
        testMetrics.forEach((key, value) -> System.out.printf("%s: %s%n", key, value));
        System.out.println("===============================================");
    }
}