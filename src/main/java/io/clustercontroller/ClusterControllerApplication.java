package io.clustercontroller;

import io.clustercontroller.allocation.ActualAllocationUpdater;
import io.clustercontroller.allocation.ShardAllocator;
import io.clustercontroller.config.ClusterControllerConfig;
import io.clustercontroller.discovery.Discovery;
import io.clustercontroller.election.LeaderElection;
import io.clustercontroller.indices.IndexManager;
import io.clustercontroller.store.MetadataStore;
import io.clustercontroller.store.EtcdMetadataStore;
import io.clustercontroller.tasks.TaskContext;

import lombok.extern.slf4j.Slf4j;

import static io.clustercontroller.config.Constants.*;

/**
 * Main application class for the Cluster Controller.
 * 
 * This application provides production-ready controller functionality for managing
 * distributed clusters at scale, including shard allocation, cluster coordination,
 * and automated operations backed by pluggable metadata stores.
 * 
 * Required Environment Variables:
 * - NODE_NAME: Unique identifier for this controller instance (required for leader election)
 */
@Slf4j
public class ClusterControllerApplication {

    public static void main(String[] args) {
        log.info("Starting Cluster Controller Application");
        
        try {
            // Create configuration
            // TODO: Pull configuration from environment variables or injected config file
            ClusterControllerConfig config = new ClusterControllerConfig(
                DEFAULT_CLUSTER_NAME, 
                new String[]{DEFAULT_ETCD_ENDPOINT}, 
                DEFAULT_TASK_INTERVAL_SECONDS
            );
            log.info("Loaded configuration for cluster: {}", config.getClusterName());
            
            // Initialize metadata store (singleton)
            MetadataStore metadataStore = EtcdMetadataStore.getInstance(
                config.getClusterName(), 
                config.getEtcdEndpoints()
            );
            metadataStore.initialize();
            
            // Wait until this node becomes leader
            EtcdMetadataStore etcdStore = (EtcdMetadataStore) metadataStore;
            LeaderElection leaderElection = new LeaderElection(etcdStore);
            leaderElection.waitUntilLeader();  // blocks until leader
            
            // Initialize components
            IndexManager indexManager = new IndexManager(metadataStore);
            Discovery discovery = new Discovery(metadataStore);
            ShardAllocator shardAllocator = new ShardAllocator(metadataStore);
            ActualAllocationUpdater actualAllocationUpdater = new ActualAllocationUpdater(metadataStore);
            
            // Create task context for component access
            TaskContext taskContext = new TaskContext(indexManager, discovery, shardAllocator, actualAllocationUpdater);
            
            // Initialize generic task manager
            TaskManager taskManager = new TaskManager(
                metadataStore,
                taskContext,
                config.getTaskIntervalSeconds()
            );
            
            // Add shutdown hook for graceful cleanup
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("Shutting down Cluster Controller");
                taskManager.stop();
            }));
            
            // Start the controller
            taskManager.start();
            log.info("Cluster Controller started successfully");
            
            // Keep the application running
            Thread.currentThread().join();
            
        } catch (Exception e) {
            log.error("Failed to start Cluster Controller: {}", e.getMessage(), e);
            System.exit(1);
        }
    }
    
}
