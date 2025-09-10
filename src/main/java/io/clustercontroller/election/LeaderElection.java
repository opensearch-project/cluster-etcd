package io.clustercontroller.election;

import io.clustercontroller.store.EtcdMetadataStore;
import io.clustercontroller.util.EnvironmentUtils;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static io.clustercontroller.config.Constants.*;

/**
 * Handles leader election logic for the cluster controller.
 * Extracted to a separate class for easier testing and maintainability.
 */
@Slf4j
public class LeaderElection {
    
    private static final long LEADER_ELECTION_POLL_INTERVAL_MS = 1000L; // 1 second
    private static final long DEFAULT_LEADER_ELECTION_TIMEOUT_MS = 30_000L; // 30 seconds
    
    private final EtcdMetadataStore metadataStore;
    private final String currentNodeName;
    
    public LeaderElection(EtcdMetadataStore metadataStore) {
        this.metadataStore = metadataStore;
        this.currentNodeName = EnvironmentUtils.getRequiredEnv("NODE_NAME");
    }
    
    /**
     * Wait until this node becomes the leader with default timeout.
     * 
     * @throws InterruptedException if the thread is interrupted
     * @throws TimeoutException if leader election times out
     */
    public void waitUntilLeader() throws InterruptedException, TimeoutException {
        waitUntilLeader(DEFAULT_LEADER_ELECTION_TIMEOUT_MS);
    }
    
    /**
     * Wait until this node becomes the leader with specified timeout.
     * Uses CountDownLatch for better concurrency handling instead of Thread.sleep.
     * 
     * @param timeoutMs maximum time to wait for leader election in milliseconds
     * @throws InterruptedException if the thread is interrupted
     * @throws TimeoutException if leader election times out
     */
    public void waitUntilLeader(long timeoutMs) throws InterruptedException, TimeoutException {
        log.info("LeaderElection - Starting leader election process...");
        log.info("LeaderElection - Current node: {}", currentNodeName);
        
        long startTime = System.currentTimeMillis();
        CountDownLatch pollLatch = new CountDownLatch(1);
        
        // Use CompletableFuture to handle the polling logic
        CompletableFuture<Void> leaderWaitFuture = CompletableFuture.runAsync(() -> {
            try {
                while (!metadataStore.isLeader()) {
                    // Check timeout
                    if (System.currentTimeMillis() - startTime > timeoutMs) {
                        throw new RuntimeException("Leader election timeout exceeded: " + timeoutMs + "ms");
                    }
                    
                    // Wait for poll interval using CountDownLatch instead of Thread.sleep
                    CountDownLatch sleepLatch = new CountDownLatch(1);
                    sleepLatch.await(LEADER_ELECTION_POLL_INTERVAL_MS, TimeUnit.MILLISECONDS);
                }
                pollLatch.countDown();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Leader election interrupted", e);
            } catch (Exception e) {
                throw new RuntimeException("Leader election failed", e);
            }
        });
        
        try {
            // Wait for either success or timeout
            if (!pollLatch.await(timeoutMs, TimeUnit.MILLISECONDS)) {
                leaderWaitFuture.cancel(true);
                throw new TimeoutException("Leader election timed out after " + timeoutMs + "ms");
            }
            
            log.info("LeaderElection - SUCCESS: This node ({}) has become the leader!", currentNodeName);
            
        } catch (InterruptedException e) {
            leaderWaitFuture.cancel(true);
            throw e;
        }
    }
}
