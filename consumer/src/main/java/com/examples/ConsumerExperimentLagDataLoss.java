package com.examples;

import com.google.common.io.Resources;
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
    private static final int PROCESSING_DELAY_MS = 500; // Simulated processing delay in milliseconds
    private static final int LOG_INTERVAL_MS = 2000; // Interval for logging metrics in milliseconds

    public static void main(String[] args) throws Exception {
        // Read Kafka properties file and create Kafka consumer
        KafkaConsumer<String, Object> consumer;
        Properties properties = new Properties();

        try (InputStream props = Resources.getResource("consumer.properties").openStream()) {
            properties.load(props);

            // Override configurations for the experiment
            properties.put("max.poll.interval.ms", "5000"); // Adjust poll timeout for test
            properties.put("enable.auto.commit", "false"); // Disable auto-commit for manual offset tracking
            properties.put("auto.offset.reset", "earliest");
            consumer = new KafkaConsumer<>(properties);
        }

        // Subscribe to the target topics
        String topic = "click-events"; // Experimenting only on the "click-events" topic for now
        consumer.subscribe(Collections.singletonList(topic));
        System.out.printf("Consumer subscribed to topic: %s%n", topic);

        // Metrics tracking
        Map<TopicPartition, Long> latestOffsets = new HashMap<>(); // To track the latest producer offsets
        AtomicLong totalLag = new AtomicLong(0); // To track cumulative lag
        AtomicLong totalMessagesProcessed = new AtomicLong(0); // To track messages processed
        long lastLogTime = System.currentTimeMillis(); // To manage periodic metric logging

        while (true) {
            // Poll for new records
            ConsumerRecords<String, Object> records = consumer.poll(Duration.ofMillis(POLL_DURATION_MS));
            long recordCount = records.count();
            totalMessagesProcessed.addAndGet(recordCount);

            // Fetch the latest offsets and calculate lag
            latestOffsets.putAll(consumer.endOffsets(consumer.assignment()));
            long currentLag = calculateCurrentLag(consumer, latestOffsets);
            totalLag.addAndGet(currentLag);

            // Process Consumer Records
            for (ConsumerRecord<String, Object> record : records) {
                System.out.printf("Received record: topic=%s, partition=%d, offset=%d, value=%s%n",
                        record.topic(), record.partition(), record.offset(), record.value());
                // Simulate artificial delay in data processing
                Thread.sleep(PROCESSING_DELAY_MS);
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
}