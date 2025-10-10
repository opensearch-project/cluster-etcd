package io.clustercontroller;

import io.clustercontroller.allocation.ActualAllocationUpdater;
import io.clustercontroller.allocation.ShardAllocator;
import io.clustercontroller.config.ClusterControllerConfig;
import io.clustercontroller.discovery.Discovery;
import io.clustercontroller.health.ClusterHealthManager;
import io.clustercontroller.indices.AliasManager;
import io.clustercontroller.indices.IndexManager;
import io.clustercontroller.orchestration.GoalStateOrchestrator;
import io.clustercontroller.orchestration.GoalStateOrchestrationStrategy;
import io.clustercontroller.templates.TemplateManager;
import io.clustercontroller.store.MetadataStore;
import io.clustercontroller.store.EtcdMetadataStore;
import io.clustercontroller.tasks.TaskContext;
import io.clustercontroller.TaskManager;
import io.clustercontroller.util.EnvironmentUtils;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Primary;

import static io.clustercontroller.config.Constants.*;

/**
 * Main Spring Boot application class for the Cluster Controller with multi-cluster support.
 * 
 * This application provides production-ready controller functionality for managing
 * distributed clusters at scale, including shard allocation, cluster coordination,
 * automated operations, and REST APIs backed by pluggable metadata stores.
 * 
 * The application is cluster-agnostic - cluster context is provided via API calls.
 */
@Slf4j
@SpringBootApplication
@ComponentScan(basePackages = "io.clustercontroller")
public class ClusterControllerApplication {

    public static void main(String[] args) {
        log.info("Starting Multi-Cluster Controller Application with REST APIs");
        
        try {
            SpringApplication.run(ClusterControllerApplication.class, args);
            log.info("Multi-Cluster Controller with REST APIs started successfully");
            
        } catch (Exception e) {
            log.error("Failed to start Cluster Controller: {}", e.getMessage(), e);
            System.exit(1);
        }
    }
    
    @Bean
    @Primary
    public ClusterControllerConfig config() {
        ClusterControllerConfig config = new ClusterControllerConfig();
        log.info("Loaded configuration with cluster: {}", config.getClusterName());
        return config;
    }
    
    /**
     * MetadataStore bean - cluster-agnostic, uses etcd endpoints only
     */
    @Bean
    public MetadataStore metadataStore(ClusterControllerConfig config) {
        log.info("Initializing cluster-agnostic MetadataStore connection to etcd");
        try {
            EtcdMetadataStore store = EtcdMetadataStore.getInstance(config.getEtcdEndpoints());
            store.initialize();
            log.info("MetadataStore initialized successfully");
            return store;
        } catch (Exception e) {
            log.error("Failed to initialize MetadataStore: {}", e.getMessage(), e);
            throw new RuntimeException("MetadataStore initialization failed", e);
        }
    }
    
    /**
     * IndexManager bean for multi-cluster index lifecycle operations.
     */
    @Bean
    public IndexManager indexManager(MetadataStore metadataStore) {
        log.info("Initializing IndexManager for multi-cluster support");
        return new IndexManager(metadataStore);
    }

    @Bean
    public Discovery discovery(MetadataStore metadataStore, ClusterControllerConfig config) {
        log.info("Initializing Discovery for cluster: {}", config.getClusterName());
        return new Discovery(metadataStore, config.getClusterName());
    }

    @Bean
    public ClusterHealthManager clusterHealthManager(MetadataStore metadataStore) {
        log.info("Initializing ClusterHealthManager for multi-cluster support");
        return new ClusterHealthManager(metadataStore);
    }

    @Bean
    public AliasManager aliasManager(MetadataStore metadataStore) {
        log.info("Initializing AliasManager for multi-cluster support");
        return new AliasManager(metadataStore);
    }

    @Bean
    public TemplateManager templateManager(MetadataStore metadataStore) {
        log.info("Initializing TemplateManager for multi-cluster support");
        return new TemplateManager(metadataStore);
    }

    @Bean
    public ShardAllocator shardAllocator(MetadataStore metadataStore) {
        log.info("Initializing ShardAllocator");
        return new ShardAllocator(metadataStore);
    }

    @Bean
    public ActualAllocationUpdater actualAllocationUpdater(MetadataStore metadataStore) {
        log.info("Initializing ActualAllocationUpdater");
        return new ActualAllocationUpdater(metadataStore);
    }

    /**
     * GoalStateOrchestrator bean for orchestrating goal states from planned allocations.
     */
    @Bean
    public GoalStateOrchestrator goalStateOrchestrator(MetadataStore metadataStore) {
        log.info("Initializing GoalStateOrchestrator with RollingUpdateOrchestrationStrategy");
        return new GoalStateOrchestrator(metadataStore);
    }

    /**
     * TaskContext bean to provide dependencies to tasks.
     */
    @Bean
    public TaskContext taskContext(
            ClusterControllerConfig config,
            IndexManager indexManager,
            Discovery discovery,
            ShardAllocator shardAllocator,
            ActualAllocationUpdater actualAllocationUpdater,
            GoalStateOrchestrator goalStateOrchestrator) {
        // Actual constructor order: clusterName, indexManager, shardAllocator, actualAllocationUpdater, goalStateOrchestrator, discovery
        return new TaskContext(
            config.getClusterName(), 
            indexManager, 
            shardAllocator, 
            actualAllocationUpdater, 
            goalStateOrchestrator,
            discovery
        );
    }

    /**
     * MultiClusterManager bean for managing multiple clusters with distributed locking.
     * Replaces the single TaskManager with multi-cluster coordination.
     */
    @Bean
    public MultiClusterManager multiClusterManager(
            MetadataStore metadataStore,
            TaskContext taskContext,
            @Value("${controller.id}") String controllerId,
            @Value("${controller.ttl.seconds:60}") int controllerTtlSeconds,
            @Value("${cluster.lock.ttl.seconds:60}") int clusterLockTtlSeconds,
            @Value("${controller.keepalive.interval.seconds:10}") int keepAliveIntervalSeconds) {
        
        log.info("========================================");
        log.info("Initializing MultiClusterManager");
        log.info("Controller ID: {}", controllerId);
        log.info("Controller TTL: {} seconds", controllerTtlSeconds);
        log.info("Cluster Lock TTL: {} seconds", clusterLockTtlSeconds);
        log.info("KeepAlive Interval: {} seconds", keepAliveIntervalSeconds);
        log.info("========================================");
        
        MultiClusterManager manager = new MultiClusterManager(
            (EtcdMetadataStore) metadataStore,
            taskContext,
            controllerId,
            controllerTtlSeconds,
            clusterLockTtlSeconds,
            keepAliveIntervalSeconds
        );
        
        try {
            manager.start();
            log.info("========================================");
            log.info("MultiClusterManager started successfully!");
            log.info("Controller '{}' is now managing {} cluster(s): {}", 
                controllerId, 
                manager.getManagedClusterCount(),
                manager.getManagedClusters());
            log.info("========================================");
        } catch (Exception e) {
            log.error("Failed to start MultiClusterManager", e);
            throw new RuntimeException("MultiClusterManager startup failed", e);
        }
        
        return manager;
    }

    // TODO: Old single-cluster TaskManager - disabled in favor of MultiClusterManager
    // Uncomment below if you need to revert to single-cluster mode
    /*
    @Bean
    public TaskManager taskManager(MetadataStore metadataStore, TaskContext taskContext, ClusterControllerConfig config) {
        log.info("Initializing TaskManager for cluster: {}", config.getClusterName());
        TaskManager taskManager = new TaskManager(metadataStore, taskContext, config.getClusterName(), config.getTaskIntervalSeconds());
        taskManager.start();
        log.info("TaskManager started with background processing for cluster: {}", config.getClusterName());
        return taskManager;
    }
    */
}