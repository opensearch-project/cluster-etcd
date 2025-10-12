package io.clustercontroller;

import io.clustercontroller.multicluster.AssignmentPolicy;
import io.clustercontroller.multicluster.RendezvousHashPolicy;
import io.clustercontroller.multicluster.lifecycle.ClusterLifecycleManager;
import io.clustercontroller.multicluster.lock.ClusterLock;
import io.clustercontroller.multicluster.lock.DistributedLockManager;
import io.clustercontroller.multicluster.lock.LockException;
import io.clustercontroller.multicluster.registry.ClusterRegistry;
import io.clustercontroller.multicluster.registry.ControllerRegistration;
import io.clustercontroller.multicluster.registry.ControllerRegistry;
import io.etcd.jetcd.Watch;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Multi-cluster manager that coordinates task execution across multiple clusters.
 * 
 * This is the main orchestrator that:
 * - Registers the controller with heartbeat
 * - Discovers clusters and controllers
 * - Uses policy to determine cluster assignments
 * - Delegates lock management to DistributedLockManager
 * - Delegates lifecycle management to ClusterLifecycleManager
 * - Watches for membership changes and rebalances
 * 
 * Architecture:
 * - One controller can manage multiple clusters (up to capacity limit)
 * - Each cluster is locked by exactly one controller at a time
 * - Uses Rendezvous Hashing (HRW) for fair distribution
 * - Automatically rebalances when controllers join/leave
 */
@Slf4j
@Component
public class MultiClusterManager {
    
    // Configuration
    private final String controllerId;
    private final int controllerTtlSeconds;
    private final int clusterLockTtlSeconds;
    
    // Dependencies - all injected, clean separation of concerns
    private final DistributedLockManager lockManager;
    private final ControllerRegistry controllerRegistry;
    private final ClusterRegistry clusterRegistry;
    private final ClusterLifecycleManager lifecycleManager;
    private final AssignmentPolicy assignmentPolicy;
    
    // Runtime state
    private final ScheduledExecutorService reconcileScheduler;
    private ControllerRegistration registration;
    private Watch.Watcher controllerWatcher;
    private Watch.Watcher clusterWatcher;
    
    /**
     * Constructor with all dependencies injected.
     */
    @Autowired
    public MultiClusterManager(
            DistributedLockManager lockManager,
            ControllerRegistry controllerRegistry,
            ClusterRegistry clusterRegistry,
            ClusterLifecycleManager lifecycleManager,
            @Value("${controller.id}") String controllerId,
            @Value("${controller.ttl.seconds:60}") int controllerTtlSeconds,
            @Value("${cluster.lock.ttl.seconds:60}") int clusterLockTtlSeconds) {
        
        this.lockManager = lockManager;
        this.controllerRegistry = controllerRegistry;
        this.clusterRegistry = clusterRegistry;
        this.lifecycleManager = lifecycleManager;
        this.controllerId = controllerId;
        this.controllerTtlSeconds = controllerTtlSeconds;
        this.clusterLockTtlSeconds = clusterLockTtlSeconds;
        
        // Initialize Rendezvous Hash policy with capacity of 10 clusters, topK=1 for exclusive ownership
        this.assignmentPolicy = new RendezvousHashPolicy(10, 1);
        
        // Scheduler for async reconciliation
        this.reconcileScheduler = Executors.newScheduledThreadPool(5, r -> {
            Thread t = new Thread(r);
            t.setName("mcm-reconcile-" + t.getId());
            t.setDaemon(true);
            return t;
        });
        
        log.info("MultiClusterManager initialized: controllerId={}", controllerId);
    }
    
    /**
     * Start the multi-cluster manager.
     */
    @PostConstruct
    public void start() {
        log.info("========================================");
        log.info("Starting MultiClusterManager");
        log.info("Controller ID: {}", controllerId);
        log.info("========================================");
        
        try {
            // Step 1: Register this controller
            registration = controllerRegistry.register(controllerId, controllerTtlSeconds);
            
            // Step 2: Discover initial state
            Set<String> clusters = clusterRegistry.listClusters();
            Set<String> controllers = controllerRegistry.listActiveControllers();
            
            log.info("Discovered {} clusters: {}", clusters.size(), clusters);
            log.info("Discovered {} controllers: {}", controllers.size(), controllers);
            
            // Step 3: Initial reconciliation
            reconcile(clusters, controllers);
            
            // Step 4: Watch for changes
            setupWatchers();
            
            log.info("========================================");
            log.info("MultiClusterManager STARTUP COMPLETE");
            log.info("Controller ID: {}", controllerId);
            log.info("Managing {} cluster(s): {}", 
                lifecycleManager.getRunningClusters().size(),
                lifecycleManager.getRunningClusters());
            log.info("========================================");
            
        } catch (Exception e) {
            log.error("Failed to start MultiClusterManager", e);
            throw new RuntimeException("MultiClusterManager startup failed", e);
        }
    }
    
    /**
     * Reconcile cluster assignments based on policy.
     */
    private void reconcile(Set<String> clusters, Set<String> controllers) {
        log.debug("Reconciling: {} clusters, {} controllers", clusters.size(), controllers.size());
        
        // Refresh policy with current state
        assignmentPolicy.refresh(
            controllerId,
            controllers,
            clusters,
            lifecycleManager.getRunningClusters()
        );
        
        // Step 1: Release clusters we shouldn't own anymore
        for (String clusterId : lifecycleManager.getRunningClusters()) {
            if (!clusters.contains(clusterId)) {
                log.info("Cluster {} no longer exists, releasing", clusterId);
                lifecycleManager.stopCluster(clusterId);
            } else if (assignmentPolicy.shouldRelease(clusterId)) {
                log.info("Policy says release cluster {}", clusterId);
                lifecycleManager.stopCluster(clusterId);
            }
        }
        
        // Step 2: Acquire clusters we should own
        for (String clusterId : clusters) {
            if (!lifecycleManager.isClusterRunning(clusterId)) {
                int currentCount = lifecycleManager.getRunningClusters().size();
                if (assignmentPolicy.shouldAttempt(clusterId, currentCount)) {
                    log.info("Policy says attempt cluster {}", clusterId);
                    tryAcquireCluster(clusterId);
                }
            }
        }
        
        log.info("Reconciliation complete. Managing {} clusters: {}",
            lifecycleManager.getRunningClusters().size(),
            lifecycleManager.getRunningClusters());
    }
    
    /**
     * Try to acquire a cluster lock and start managing it.
     */
    private void tryAcquireCluster(String clusterId) {
        try {
            // Acquire lock via DistributedLockManager
            ClusterLock lock = lockManager.acquireLock(clusterId, clusterLockTtlSeconds);
            
            // Start managing via ClusterLifecycleManager
            lifecycleManager.startCluster(clusterId, lock);
            
            log.info("========================================");
            log.info("✓ CLUSTER ACQUIRED AND STARTED");
            log.info("  Cluster ID: {}", clusterId);
            log.info("  Controller: {}", controllerId);
            log.info("  Total Managed: {}", lifecycleManager.getRunningClusters().size());
            log.info("========================================");
            
        } catch (LockException e) {
            log.debug("Could not acquire lock for cluster {}: {}", clusterId, e.getMessage());
        } catch (Exception e) {
            log.warn("Failed to acquire cluster {}", clusterId, e);
        }
    }
    
    /**
     * Setup watchers for membership changes.
     */
    private void setupWatchers() {
        // Watch cluster changes
        clusterWatcher = clusterRegistry.watchClusters(() -> {
            // Offload to reconcile scheduler to avoid blocking etcd event thread
            reconcileScheduler.execute(() -> {
                Set<String> clusters = clusterRegistry.listClusters();
                Set<String> controllers = controllerRegistry.listActiveControllers();
                reconcile(clusters, controllers);
            });
        });
        
        // Watch controller changes
        controllerWatcher = controllerRegistry.watchControllers(() -> {
            // Offload to reconcile scheduler to avoid blocking etcd event thread
            reconcileScheduler.execute(() -> {
                Set<String> clusters = clusterRegistry.listClusters();
                Set<String> controllers = controllerRegistry.listActiveControllers();
                reconcile(clusters, controllers);
            });
        });
        
        log.info("Watchers setup complete");
    }
    
    /**
     * Graceful shutdown.
     */
    @PreDestroy
    public void shutdown() {
        log.info("========================================");
        log.info("Shutting down MultiClusterManager");
        log.info("========================================");
        
        try {
            // Stop all clusters
            lifecycleManager.stopAll();
            
            // Close watchers
            if (clusterWatcher != null) {
                clusterWatcher.close();
            }
            if (controllerWatcher != null) {
                controllerWatcher.close();
            }
            
            // Deregister controller
            if (registration != null) {
                controllerRegistry.deregister(registration);
            }
            
            // Shutdown scheduler
            reconcileScheduler.shutdown();
            if (!reconcileScheduler.awaitTermination(30, TimeUnit.SECONDS)) {
                log.warn("Reconcile scheduler did not terminate in time, forcing shutdown");
                reconcileScheduler.shutdownNow();
            }
            
            log.info("✓ MultiClusterManager shutdown complete");
            
        } catch (Exception e) {
            log.error("Error during shutdown", e);
        }
    }
}
