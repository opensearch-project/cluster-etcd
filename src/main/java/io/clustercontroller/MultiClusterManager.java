package io.clustercontroller;

import io.clustercontroller.multicluster.AssignmentPolicy;
import io.clustercontroller.multicluster.RendezvousHashPolicy;
import io.clustercontroller.store.EtcdMetadataStore;
import io.clustercontroller.store.EtcdPathResolver;
import io.clustercontroller.store.MetadataStore;
import io.clustercontroller.tasks.TaskContext;
import io.etcd.jetcd.*;
import io.etcd.jetcd.common.exception.ClosedClientException;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.lock.LockResponse;
import io.etcd.jetcd.options.GetOption;
import io.etcd.jetcd.options.PutOption;
import io.etcd.jetcd.options.WatchOption;
import io.etcd.jetcd.support.CloseableClient;
import io.etcd.jetcd.support.Observers;
import io.etcd.jetcd.watch.WatchEvent;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Multi-cluster manager that coordinates task execution across multiple clusters.
 * Uses etcd's distributed locking mechanism to ensure each cluster is managed by exactly one controller.
 * 
 * Architecture:
 * - One controller can manage multiple clusters (up to capacity limit)
 * - Each cluster is locked by exactly one controller at a time
 * - Uses lease-based locking with automatic keepalive
 * - Watches for cluster/controller membership changes and reconciles
 */
@Slf4j
public class MultiClusterManager {
    
    // Etcd clients - accessed via reflection from EtcdMetadataStore
    private final Client etcdClient;
    private final KV kvClient;
    private final Lease leaseClient;
    private final Lock lockClient;
    private final Watch watchClient;
    
    // Configuration
    private final String controllerId;
    private final int controllerTtlSeconds;
    private final int clusterLockTtlSeconds;
    private final Duration keepAliveInterval;
    
    // Dependencies
    private final MetadataStore metadataStore;
    private final TaskContext taskContext;
    private final EtcdPathResolver pathResolver;
    private final AssignmentPolicy assignmentPolicy;
    
    // Runtime state
    private final ConcurrentMap<String, ClusterBinding> runningClusters = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean started = new AtomicBoolean(false);
    
    // Controller lease for heartbeat
    private volatile long controllerLeaseId = -1;
    
    /**
     * Binding between a cluster lock and its TaskManager
     */
    @Data
    public static class ClusterBinding {
        private final long leaseId;
        private final TaskManager taskManager;
        private final ByteSequence lockKey;
        private final Watch.Watcher watcher;
        private final ScheduledFuture<?> keepAliveTask;
        private final CloseableClient keepAliveObserver;
    }
    
    /**
     * Constructor for MultiClusterManager
     */
    public MultiClusterManager(
            Client etcdClient,
            MetadataStore metadataStore,
            TaskContext taskContext,
            String controllerId,
            int controllerTtlSeconds,
            int clusterLockTtlSeconds,
            int keepAliveIntervalSeconds) {
        
        this.metadataStore = metadataStore;
        this.taskContext = taskContext;
        this.controllerId = controllerId;
        this.controllerTtlSeconds = controllerTtlSeconds;
        this.clusterLockTtlSeconds = clusterLockTtlSeconds;
        this.keepAliveInterval = Duration.ofSeconds(keepAliveIntervalSeconds);
        this.pathResolver = EtcdPathResolver.getInstance();
        
        // Initialize Rendezvous Hash policy with capacity of 10 clusters, topK=1 for exclusive ownership
        this.assignmentPolicy = new RendezvousHashPolicy(10, 1);
        
        
        // Extract etcd clients from injected Client (clean DI, no reflection!)
        this.etcdClient = etcdClient;
        this.kvClient = etcdClient.getKVClient();
        this.leaseClient = etcdClient.getLeaseClient();
        this.lockClient = etcdClient.getLockClient();
        this.watchClient = etcdClient.getWatchClient();
        
        // Pool size: 1 controller heartbeat + up to 10 cluster health checks + rebalancing tasks
        this.scheduler = Executors.newScheduledThreadPool(15, r -> {
            Thread t = new Thread(r);
            t.setName("multi-cluster-scheduler-" + t.getId());
            t.setDaemon(true);
            return t;
        });
        
        log.info("MultiClusterManager initialized: controllerId={}", controllerId);
    }
    
    /**
     * Start the multi-cluster manager
     */
    public void start() throws Exception {
        if (!started.compareAndSet(false, true)) {
            log.warn("MultiClusterManager already started");
            return;
        }
        
        log.info("Starting MultiClusterManager for controller: {}", controllerId);
        
        // Step 1: Register this controller with heartbeat
        registerController();
        
        // Step 2: Discover clusters and attempt initial acquisition
        Set<String> clusters = listClusters();
        Set<String> controllers = listControllers();
        log.info("Discovered {} clusters: {}", clusters.size(), clusters);
        log.info("Discovered {} controllers: {}", controllers.size(), controllers);
        
        // Step 3: Refresh policy with current state
        assignmentPolicy.refresh(controllerId, controllers, clusters, runningClusters.keySet());
        
        // Step 4: Try to acquire clusters
        reconcile(clusters);
        
        // Step 4: Watch for membership changes
        setupWatchers();
        
        log.info("========================================");
        log.info("MultiClusterManager STARTUP COMPLETE");
        log.info("Controller ID: {}", controllerId);
        log.info("Managing {} cluster(s): {}", runningClusters.size(), runningClusters.keySet());
        log.info("========================================");
    }
    
    /**
     * Stop the multi-cluster manager
     */
    public void stop() {
        if (!started.compareAndSet(true, false)) {
            log.warn("MultiClusterManager not started");
            return;
        }
        
        log.info("Stopping MultiClusterManager...");
        
        // Stop all cluster bindings
        for (String clusterId : new ArrayList<>(runningClusters.keySet())) {
            stopAndCleanup(clusterId);
        }
        
        // Shutdown scheduler
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(10, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
        
        // Revoke controller lease
        if (controllerLeaseId > 0) {
            safeRevoke(controllerLeaseId);
        }
        
        log.info("MultiClusterManager stopped");
    }
    
    /**
     * Register this controller in etcd with heartbeat
     */
    private void registerController() throws Exception {
        // Create controller heartbeat with lease
        controllerLeaseId = leaseClient.grant(controllerTtlSeconds).get(5, TimeUnit.SECONDS).getID();
        
        String heartbeatPath = pathResolver.getControllerHeartbeatPath(controllerId);
        kvClient.put(
            ByteSequence.from(heartbeatPath, UTF_8),
            ByteSequence.from("1", UTF_8),
            PutOption.newBuilder().withLeaseId(controllerLeaseId).build()
        ).get(5, TimeUnit.SECONDS);
        
        // Schedule periodic keepalive for controller lease
        scheduler.scheduleAtFixedRate(
            () -> {
                if (started.get()) {
                    safeKeepAlive(controllerLeaseId);
                }
            },
            keepAliveInterval.toSeconds(),
            keepAliveInterval.toSeconds(),
            TimeUnit.SECONDS
        );
        
        log.info("Controller {} registered with heartbeat (leaseId: {})", controllerId, controllerLeaseId);
    }
    
    /**
     * Reconcile cluster assignments based on policy
     */
    private void reconcile(Set<String> clusters) {
        log.debug("Reconciling cluster assignments. Current: {}, Discovered: {}", 
            runningClusters.keySet(), clusters);
        
        // Step 1: Release clusters we shouldn't own anymore (policy-driven rebalancing)
        for (String ownedCluster : new ArrayList<>(runningClusters.keySet())) {
            if (!clusters.contains(ownedCluster)) {
                log.info("Cluster {} no longer exists, stopping TaskManager", ownedCluster);
                stopAndCleanup(ownedCluster);
            } else if (assignmentPolicy.shouldRelease(ownedCluster)) {
                log.info("Policy says to release cluster {}, stopping TaskManager", ownedCluster);
                stopAndCleanup(ownedCluster);
            }
        }
        
        // Step 2: Attempt to acquire clusters we should own (policy-driven)
        for (String clusterId : clusters) {
            if (!runningClusters.containsKey(clusterId)) {
                if (assignmentPolicy.shouldAttempt(clusterId, runningClusters.size())) {
                    log.info("Policy says to attempt cluster: {}", clusterId);
                    tryAcquireCluster(clusterId);
                } else {
                    log.debug("Policy says NOT to attempt cluster: {}", clusterId);
                }
            }
        }
        
        log.info("Reconciliation complete. Managing {} clusters: {}", 
            runningClusters.size(), runningClusters.keySet());
    }
    
    /**
     * Try to acquire exclusive lock on a cluster and start its TaskManager
     */
    private void tryAcquireCluster(String clusterId) {
        if (runningClusters.containsKey(clusterId)) {
            log.debug("Cluster {} already managed by this controller", clusterId);
            return;
        }
        
        try {
            // Step 1: Create lease for this cluster lock
            long clusterLeaseId = leaseClient.grant(clusterLockTtlSeconds)
                .get(5, TimeUnit.SECONDS)
                .getID();
            
            log.debug("Created lease {} for cluster {}", clusterLeaseId, clusterId);
            
            // Step 2: Start keep-alive for the lease BEFORE acquiring lock
            CloseableClient keepAliveObserver = leaseClient.keepAlive(clusterLeaseId, Observers.observer(response -> {
                // Keep-alive response received
            }));
            
            log.debug("Started keep-alive for lease {}", clusterLeaseId);
            
            // Step 3: Acquire exclusive lock (blocks until owned or fails)
            String lockPath = pathResolver.getClusterLockPath(clusterId);
            LockResponse lockResponse = lockClient.lock(
                ByteSequence.from(lockPath, UTF_8),
                clusterLeaseId
            ).get(5, TimeUnit.SECONDS);
            
            ByteSequence lockKey = lockResponse.getKey();
            log.info("========================================");
            log.info("✓ LOCK ACQUIRED for cluster: {}", clusterId);
            log.info("  Controller: {}", controllerId);
            log.info("  Lock Key: {}", lockKey.toString(UTF_8));
            log.info("  Lease ID: {}", clusterLeaseId);
            log.info("========================================");
            
            // Step 3: Create observability pointer
            String assignmentPath = pathResolver.getControllerAssignmentPath(controllerId, clusterId);
            kvClient.put(
                ByteSequence.from(assignmentPath, UTF_8),
                ByteSequence.from("1", UTF_8),
                PutOption.newBuilder().withLeaseId(clusterLeaseId).build()
            ).get(5, TimeUnit.SECONDS);
            
            // Step 4: Start lock-agnostic TaskManager
            TaskManager taskManager = new TaskManager(
                metadataStore, 
                taskContext, 
                clusterId, 
                10L // Run task loop every 10 seconds
            );
            taskManager.start();
            
            log.info("Started TaskManager for cluster: {}", clusterId);
            
            // Step 5: Setup health check tied to worker liveness
            // Note: Keep-alive is handled automatically by the keepAliveObserver
            ScheduledFuture<?> keepAliveTask = scheduler.scheduleAtFixedRate(() -> {
                if (!taskManager.isRunning()) {
                    log.warn("TaskManager for cluster {} is not running, stopping", clusterId);
                    stopAndCleanup(clusterId);
                }
            }, keepAliveInterval.toSeconds(), keepAliveInterval.toSeconds(), TimeUnit.SECONDS);
            
            // Step 6: Watch the lock key for changes (lease expiration, etc.)
            Watch.Watcher watcher = watchClient.watch(lockKey, watchResponse -> {
                for (WatchEvent event : watchResponse.getEvents()) {
                    switch (event.getEventType()) {
                        case DELETE:
                            log.warn("Lock key deleted for cluster {}, stopping TaskManager", clusterId);
                            stopAndCleanup(clusterId);
                            return;
                        case PUT:
                            log.warn("Lock key modified for cluster {}, stopping TaskManager", clusterId);
                            stopAndCleanup(clusterId);
                            return;
                        default:
                            break;
                    }
                }
            });
            
            // Step 7: Store binding
            ClusterBinding binding = new ClusterBinding(
                clusterLeaseId, 
                taskManager, 
                lockKey, 
                watcher, 
                keepAliveTask,
                keepAliveObserver
            );
            runningClusters.put(clusterId, binding);
            
            log.info("========================================");
            log.info("✓ CLUSTER MANAGEMENT STARTED");
            log.info("  Cluster ID: {}", clusterId);
            log.info("  Controller: {}", controllerId);
            log.info("  TaskManager: RUNNING");
            log.info("  Total Managed Clusters: {}", runningClusters.size());
            log.info("  All Managed Clusters: {}", runningClusters.keySet());
            log.info("========================================");
            
        } catch (TimeoutException e) {
            log.warn("Timeout acquiring cluster {}: {}", clusterId, e.getMessage());
        } catch (ExecutionException e) {
            log.warn("Failed to acquire cluster {}: {}", clusterId, e.getCause().getMessage());
        } catch (Exception e) {
            log.error("Unexpected error acquiring cluster: {}", clusterId, e);
        }
    }
    
    /**
     * Stop TaskManager and cleanup resources for a cluster
     */
    private void stopAndCleanup(String clusterId) {
        ClusterBinding binding = runningClusters.remove(clusterId);
        if (binding == null) {
            log.debug("No binding found for cluster: {}", clusterId);
            return;
        }
        
        log.info("Stopping and cleaning up cluster: {}", clusterId);
        
        // Stop TaskManager
        try {
            binding.getTaskManager().stop();
            log.debug("Stopped TaskManager for cluster: {}", clusterId);
        } catch (Exception e) {
            log.error("Error stopping TaskManager for cluster {}", clusterId, e);
        }
        
        // Cancel keepalive
        try {
            if (binding.getKeepAliveTask() != null) {
                binding.getKeepAliveTask().cancel(true);
            }
        } catch (Exception e) {
            log.error("Error canceling keepalive for cluster {}", clusterId, e);
        }
        
        // Close watcher
        try {
            if (binding.getWatcher() != null) {
                binding.getWatcher().close();
            }
        } catch (Exception e) {
            log.error("Error closing watcher for cluster {}", clusterId, e);
        }
        
        // Close keep-alive observer
        try {
            if (binding.getKeepAliveObserver() != null) {
                binding.getKeepAliveObserver().close();
            }
        } catch (Exception e) {
            log.error("Error closing keep-alive observer for cluster {}", clusterId, e);
        }
        
        // Revoke lease (fast unlock)
        safeRevoke(binding.getLeaseId());
        
        // Clean up assignment pointer
        try {
            String assignmentPath = pathResolver.getControllerAssignmentPath(controllerId, clusterId);
            kvClient.delete(ByteSequence.from(assignmentPath, UTF_8))
                .get(5, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.debug("Error deleting assignment pointer for cluster {}: {}", clusterId, e.getMessage());
        }
        
        log.info("========================================");
        log.info("✗ CLUSTER MANAGEMENT STOPPED");
        log.info("  Cluster ID: {}", clusterId);
        log.info("  Controller: {}", controllerId);
        log.info("  Remaining Managed Clusters: {}", runningClusters.size());
        log.info("  All Managed Clusters: {}", runningClusters.keySet());
        log.info("========================================");
    }
    
    /**
     * Setup watchers for cluster and controller membership changes
     */
    private void setupWatchers() {
        // Watch clusters prefix
        String clustersPrefix = pathResolver.getClustersPrefix();
        watchClient.watch(
            ByteSequence.from(clustersPrefix, UTF_8),
            WatchOption.newBuilder()
                .withPrefix(ByteSequence.from(clustersPrefix, UTF_8))
                .build(),
            watchResponse -> {
                log.debug("Cluster membership changed, reconciling...");
                // Offload to scheduler to avoid blocking etcd's event thread
                scheduler.execute(() -> {
                    try {
                        Set<String> clusters = listClusters();
                        reconcile(clusters);
                    } catch (Exception e) {
                        log.error("Error during reconciliation after cluster change", e);
                    }
                });
            }
        );
        
        // Watch controllers prefix
        String controllersPrefix = pathResolver.getControllersPrefix();
        watchClient.watch(
            ByteSequence.from(controllersPrefix, UTF_8),
            WatchOption.newBuilder()
                .withPrefix(ByteSequence.from(controllersPrefix, UTF_8))
                .build(),
            watchResponse -> {
                log.info("Controller membership changed, rebalancing...");
                // Offload to scheduler to avoid blocking etcd's event thread
                scheduler.execute(() -> {
                    try {
                        Set<String> clusters = listClusters();
                        Set<String> controllers = listControllers();
                        assignmentPolicy.refresh(controllerId, controllers, clusters, runningClusters.keySet());
                        reconcile(clusters);
                    } catch (Exception e) {
                        log.error("Error during rebalancing after controller change", e);
                    }
                });
            }
        );
        
        log.info("Watchers setup complete");
    }
    
    /**
     * List all registered clusters from etcd
     */
    private Set<String> listClusters() throws Exception {
        String clustersPrefix = pathResolver.getClustersPrefix();
        GetResponse response = kvClient.get(
            ByteSequence.from(clustersPrefix, UTF_8),
            GetOption.newBuilder()
                .withPrefix(ByteSequence.from(clustersPrefix, UTF_8))
                .build()
        ).get(5, TimeUnit.SECONDS);
        
        Set<String> clusterIds = new HashSet<>();
        response.getKvs().forEach(kv -> {
            String key = kv.getKey().toString(UTF_8);
            // Extract cluster ID from path: /multi-cluster/clusters/{cluster-id}/...
            String[] parts = key.split("/");
            if (parts.length >= 4) {
                clusterIds.add(parts[3]);
            }
        });
        
        return clusterIds;
    }
    
    /**
     * List all active controllers from etcd
     */
    private Set<String> listControllers() throws Exception {
        String controllersPrefix = pathResolver.getControllersPrefix();
        GetResponse response = kvClient.get(
            ByteSequence.from(controllersPrefix, UTF_8),
            GetOption.newBuilder()
                .withPrefix(ByteSequence.from(controllersPrefix, UTF_8))
                .build()
        ).get(5, TimeUnit.SECONDS);
        
        Set<String> controllerIds = new HashSet<>();
        response.getKvs().forEach(kv -> {
            String key = kv.getKey().toString(UTF_8);
            // Extract controller ID from path: /multi-cluster/controllers/{controller-id}/...
            String[] parts = key.split("/");
            if (parts.length >= 4) {
                controllerIds.add(parts[3]);
            }
        });
        
        return controllerIds;
    }
    
    /**
     * Safely keep a lease alive
     */
    private void safeKeepAlive(long leaseId) {
        try {
            leaseClient.keepAliveOnce(leaseId).get(3, TimeUnit.SECONDS);
            log.trace("Keepalive successful for lease: {}", leaseId);
        } catch (Exception e) {
            log.warn("Failed to keepalive lease {}: {}", leaseId, e.getMessage());
        }
    }
    
    /**
     * Safely revoke a lease
     */
    private void safeRevoke(long leaseId) {
        try {
            leaseClient.revoke(leaseId).get(5, TimeUnit.SECONDS);
            log.debug("Revoked lease: {}", leaseId);
        } catch (Exception e) {
            log.warn("Failed to revoke lease {}: {}", leaseId, e.getMessage());
        }
    }
    
    /**
     * Get the number of clusters currently managed by this controller
     */
    public int getManagedClusterCount() {
        return runningClusters.size();
    }
    
    /**
     * Get the set of cluster IDs currently managed by this controller
     */
    public Set<String> getManagedClusters() {
        return new HashSet<>(runningClusters.keySet());
    }
    
    /**
     * Check if the manager is running
     */
    public boolean isRunning() {
        return started.get();
    }
}
