package com.examples;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class DockerStatsTest {

    public static void main(String[] args) {
        // Create a Docker stats collector
        DockerStatsCollector statsCollector = new DockerStatsCollector();

        // Scheduled executor to fetch stats
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);

        // Collect stats every 200ms (5 times per second)
        executor.scheduleAtFixedRate(() -> {
            try {
                // Fetch stats for the container
                statsCollector.fetchStats("docker-kafka-1"); // Replace with actual container name or ID

                // Log the CPU and memory usage to verify
                double cpuUsage = statsCollector.getCpuUsage();
                double memoryUsage = statsCollector.getMemoryUsageMiB();

                System.out.printf("CPU Usage: %.2f%%, Memory Usage: %.2f MiB%n", cpuUsage, memoryUsage);
            } catch (Exception e) {
                System.err.println("Error while fetching stats: " + e.getMessage());
                e.printStackTrace();
            }
        }, 0, 100, TimeUnit.MILLISECONDS); // Set interval to 200ms for testing

        System.out.println("Docker stats collector started. Press Ctrl+C to stop.");

        // Graceful shutdown on JVM termination
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down Docker stats collector...");
            executor.shutdownNow();
            try {
                if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                    System.err.println("Executor did not terminate in the allotted time.");
                }
            } catch (InterruptedException e) {
                System.err.println("Termination interrupted: " + e.getMessage());
            }
        }));
    }
}