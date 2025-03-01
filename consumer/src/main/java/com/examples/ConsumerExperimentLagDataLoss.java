package com.examples;

import com.google.common.io.Resources;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.io.InputStream;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class ConsumerExperimentLagDataLoss {
    private static final int POLL_DURATION_MS = 100; // Polling duration in milliseconds
    private static final int LOG_INTERVAL_MS = 2000; // Interval for logging metrics in milliseconds

    public static void main(String[] args) throws Exception {
        // Define stepwise processing delays (0ms to 5000ms, increments of 500ms)
        int[] processingDelays = {0, 100, 200, 300, 400, 500};
        String topic = "click-events"; // Target Kafka topic

        // Load Kafka connection properties
        Properties properties = new Properties();
        try (InputStream props = Resources.getResource("consumer.properties").openStream()) {
            properties.load(props);
        }

        // Delete and recreate the topic before starting the tests
        deleteTopic(topic, properties);
        createTopic(topic, 1, properties);

        // Report to aggregate results from all test runs
        List<TestResult> report = new ArrayList<>();

        for (int processingDelay : processingDelays) {
            System.out.printf("Starting test with processing delay: %dms%n", processingDelay);

            // Initialize Kafka Consumer
            KafkaConsumer<String, Object> consumer = new KafkaConsumer<>(properties);

            // Subscribe to topic
            consumer.subscribe(Collections.singletonList(topic));
            System.out.printf("Consumer subscribed to topic: %s%n", topic);

            // Metrics tracking for this test
            Map<TopicPartition, Long> latestOffsets = new HashMap<>();
            AtomicLong totalLag = new AtomicLong(0);
            AtomicLong totalMessagesProcessed = new AtomicLong(0);
            long startTestTime = -1; // Test start time (set once the first event is received)
            long lastLogTime = System.currentTimeMillis();

            // Process messages for the configured delay (run test for 30 seconds as example)
            while (true) {
                // Poll for new records
                ConsumerRecords<String, Object> records = consumer.poll(Duration.ofMillis(POLL_DURATION_MS));
                if (!records.isEmpty()) { // Start the test on the first event
                    if (startTestTime == -1) {
                        startTestTime = System.currentTimeMillis(); // Mark test start time
                        System.out.println("First event received. Test begins now...");
                    }
                }

                // If the test has started, process records
                if (startTestTime != -1) {
                    // Exit after 30 seconds of test runtime
                    if (System.currentTimeMillis() - startTestTime > 30000) { // Test duration: 30 seconds
                        break;
                    }

                    long recordCount = records.count();
                    totalMessagesProcessed.addAndGet(recordCount);

                    // Fetch the latest offsets and calculate lag
                    latestOffsets.putAll(consumer.endOffsets(consumer.assignment()));
                    long currentLag = calculateCurrentLag(consumer, latestOffsets);
                    totalLag.addAndGet(currentLag);

                    // Process Consumer Records with artificial delay
                    for (ConsumerRecord<String, Object> record : records) {
                        System.out.printf("Received record: topic=%s, partition=%d, offset=%d, value=%s%n",
                                record.topic(), record.partition(), record.offset(), record.value());
                        // Simulate artificial delay in data processing
                        Thread.sleep(processingDelay);
                    }

                    // Commit offsets manually after processing
                    consumer.commitSync();

                    // Periodically log metrics
                    if (System.currentTimeMillis() - lastLogTime >= LOG_INTERVAL_MS) {
                        logMetrics(totalMessagesProcessed.get(), currentLag, totalLag.get(), latestOffsets);
                        lastLogTime = System.currentTimeMillis(); // Reset log time
                    }
                }
            }

            // After the test, save metric results for this delay
            long averageLag = totalLag.get() / Math.max(totalMessagesProcessed.get(), 1); // Avoid divide by zero
            report.add(new TestResult(processingDelay, totalMessagesProcessed.get(), averageLag, totalLag.get()));

            // Cleanup the consumer before starting the next test
            consumer.close();
        }

        // Print the combined report at the end
        System.out.println("\nFinal Test Report:");
        printReport(report);
    }

    /**
     * Delete Kafka topic.
     */
    private static void deleteTopic(String topicName, Properties properties) {
        try (AdminClient client = AdminClient.create(properties)) {
            DeleteTopicsResult deleteTopicsResult = client.deleteTopics(Collections.singleton(topicName));
            while (!deleteTopicsResult.all().isDone()) {
                // Wait for deletion to complete
            }
            System.out.printf("Topic '%s' has been deleted successfully.%n", topicName);
        } catch (Exception e) {
            System.out.printf("Exception during topic deletion: %s%n", e.getMessage());
        }
    }

    /**
     * Create Kafka topic.
     */
    private static void createTopic(String topicName, int numPartitions, Properties properties) throws Exception {
        try (AdminClient admin = AdminClient.create(properties)) {
            boolean alreadyExists = admin.listTopics().names().get().stream()
                    .anyMatch(existingTopicName -> existingTopicName.equals(topicName));
            if (alreadyExists) {
                System.out.printf("Topic already exists: %s%n", topicName);
            } else {
                System.out.printf("Creating topic: %s%n", topicName);
                NewTopic newTopic = new NewTopic(topicName, numPartitions, (short) 1);
                admin.createTopics(Collections.singleton(newTopic)).all().get();
                System.out.printf("Topic '%s' created successfully.%n", topicName);
            }
        } catch (Exception e) {
            System.out.printf("Exception during topic creation: %s%n", e.getMessage());
        }
    }

    /**
     * Calculate consumer lag based on the current offsets and the latest end offsets in partitions.
     */
    private static long calculateCurrentLag(KafkaConsumer<String, Object> consumer,
                                            Map<TopicPartition, Long> latestOffsets) {
        long lag = 0;
        for (TopicPartition partition : consumer.assignment()) {
            long latestOffset = latestOffsets.getOrDefault(partition, 0L);
            long currentOffset = consumer.position(partition);
            lag += (latestOffset - currentOffset); // Lag is the difference between latest and current offsets
        }
        return lag;
    }

    /**
     * Log metrics for lag, message processing, and offsets.
     */
    private static void logMetrics(long totalMessagesProcessed, long currentLag, long totalLag,
                                   Map<TopicPartition, Long> latestOffsets) {
        System.out.println("\n===== Metrics =====");
        System.out.println("Messages Processed: " + totalMessagesProcessed);
        System.out.println("Current Lag: " + currentLag);
        System.out.println("Cumulative Lag: " + totalLag);

        System.out.println("Latest Offsets by Partition:");
        for (Map.Entry<TopicPartition, Long> entry : latestOffsets.entrySet()) {
            System.out.printf("Partition: %s, Latest Offset: %d%n", entry.getKey(), entry.getValue());
        }
        System.out.println("===================\n");
    }

    /**
     * Print the combined test report.
     */
    private static void printReport(List<TestResult> report) {
        System.out.println("Processing Delay (ms) | Messages Processed | Avg Lag | Cumulative Lag");
        System.out.println("---------------------------------------------------------------");
        for (TestResult result : report) {
            System.out.printf("%21d | %18d | %7d | %14d%n",
                    result.processingDelay, result.messagesProcessed, result.averageLag, result.cumulativeLag);
        }
    }

    /**
     * Data structure to hold the results of each test run.
     */
    private static class TestResult {
        int processingDelay;
        long messagesProcessed;
        long averageLag;
        long cumulativeLag;

        public TestResult(int processingDelay, long messagesProcessed, long averageLag, long cumulativeLag) {
            this.processingDelay = processingDelay;
            this.messagesProcessed = messagesProcessed;
            this.averageLag = averageLag;
            this.cumulativeLag = cumulativeLag;
        }
    }
}