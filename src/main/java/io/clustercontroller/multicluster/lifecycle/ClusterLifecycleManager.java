package io.clustercontroller.multicluster.lifecycle;

import io.clustercontroller.TaskManager;
import io.clustercontroller.multicluster.lock.ClusterLock;
import io.clustercontroller.multicluster.lock.DistributedLockManager;
import io.clustercontroller.store.MetadataStore;
import io.clustercontroller.tasks.TaskContext;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Manages TaskManager lifecycle and health monitoring for clusters.
 */
@Component
@Slf4j
public class ClusterLifecycleManager {
    
    private final MetadataStore metadataStore;
    private final TaskContext taskContext;
    private final DistributedLockManager lockManager;
    private final ScheduledExecutorService healthCheckScheduler;
    private final Duration healthCheckInterval;
    
    private final ConcurrentMap<String, ManagedCluster> clusters = new ConcurrentHashMap<>();
    
    @Autowired
    public ClusterLifecycleManager(
            MetadataStore metadataStore,
            TaskContext taskContext,
            DistributedLockManager lockManager,
            @Value("${multi-cluster.health-check-interval:10}") int healthCheckIntervalSeconds) {
        
        this.metadataStore = metadataStore;
        this.taskContext = taskContext;
        this.lockManager = lockManager;
        this.healthCheckInterval = Duration.ofSeconds(healthCheckIntervalSeconds);
        
        this.healthCheckScheduler = Executors.newScheduledThreadPool(10, r -> {
            Thread t = new Thread(r);
            t.setName("cluster-health-check-" + t.getId());
            t.setDaemon(true);
            return t;
        });
        
        log.info("ClusterLifecycleManager initialized (health check interval: {}s)", healthCheckIntervalSeconds);
    }
    
    /**
     * Start managing a cluster.
     */
    public void startCluster(String clusterId, ClusterLock lock) {
        if (clusters.containsKey(clusterId)) {
            log.warn("Cluster {} already managed", clusterId);
            return;
        }
        
        try {
            log.info("Starting management of cluster: {}", clusterId);
            
            // Create TaskManager for this cluster
            TaskManager taskManager = new TaskManager(
                metadataStore,
                taskContext,
                clusterId,
                10L  // Task loop interval: 10 seconds
            );
            taskManager.start();
            
            // Watch for unexpected lock loss (split-brain prevention)
            // If lock is lost while we think we still own it, stop immediately
            var lockWatcher = lockManager.watchLock(lock, () -> {
                log.warn("Lock lost for cluster {}, stopping", clusterId);
                stopCluster(clusterId);
            });
            
            // Schedule health checks
            ScheduledFuture<?> healthCheck = healthCheckScheduler.scheduleAtFixedRate(
                () -> checkHealth(clusterId, taskManager),
                healthCheckInterval.toSeconds(),
                healthCheckInterval.toSeconds(),
                TimeUnit.SECONDS
            );
            
            ManagedCluster managed = new ManagedCluster(
                clusterId, taskManager, lock, lockWatcher, healthCheck
            );
            clusters.put(clusterId, managed);
            
            log.info("✓ Started managing cluster: {} (total: {})", clusterId, clusters.size());
            
        } catch (Exception e) {
            log.error("Failed to start cluster: {}", clusterId, e);
            lockManager.releaseLock(lock);
            throw new RuntimeException("Failed to start cluster: " + clusterId, e);
        }
    }
    
    /**
     * Stop managing a cluster.
     */
    public void stopCluster(String clusterId) {
        ManagedCluster managed = clusters.remove(clusterId);
        if (managed == null) {
            log.debug("Cluster {} not managed", clusterId);
            return;
        }
        
        try {
            log.info("Stopping management of cluster: {}", clusterId);
            
            // Stop TaskManager
            managed.getTaskManager().stop();
            
            // Cancel health check
            if (managed.getHealthCheckTask() != null) {
                managed.getHealthCheckTask().cancel(true);
            }
            
            // Close lock watcher
            if (managed.getLockWatcher() != null) {
                managed.getLockWatcher().close();
            }
            
            // Release lock
            lockManager.releaseLock(managed.getLock());
            
            log.info("✓ Stopped managing cluster: {} (remaining: {})", clusterId, clusters.size());
            
        } catch (Exception e) {
            log.error("Error stopping cluster: {}", clusterId, e);
        }
    }
    
    /**
     * Health check: if TaskManager dies, release the cluster.
     */
    private void checkHealth(String clusterId, TaskManager taskManager) {
        if (!taskManager.isRunning()) {
            log.warn("TaskManager not running for cluster {}, releasing", clusterId);
            stopCluster(clusterId);
        }
    }
    
    /**
     * Check if a cluster is currently being managed by this controller.
     */
    public boolean isClusterManaged(String clusterId) {
        return clusters.containsKey(clusterId);
    }
    
    /**
     * Get set of all cluster IDs currently managed by this controller.
     */
    public Set<String> getManagedClusters() {
        return new HashSet<>(clusters.keySet());
    }
    
    /**
     * Stop all managed clusters.
     */
    public void stopAll() {
        log.info("Stopping all managed clusters");
        new ArrayList<>(clusters.keySet()).forEach(this::stopCluster);
    }
}

