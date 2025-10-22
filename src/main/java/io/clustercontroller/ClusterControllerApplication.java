package io.clustercontroller;

import io.clustercontroller.allocation.ActualAllocationUpdater;
import io.clustercontroller.allocation.ShardAllocator;
import io.clustercontroller.config.ClusterControllerConfig;
import io.clustercontroller.discovery.Discovery;
import io.clustercontroller.health.ClusterHealthManager;
import io.clustercontroller.indices.AliasManager;
import io.clustercontroller.indices.IndexManager;
import io.clustercontroller.orchestration.GoalStateOrchestrator;
import io.clustercontroller.templates.TemplateManager;
import io.clustercontroller.store.MetadataStore;
import io.clustercontroller.store.EtcdMetadataStore;
import io.clustercontroller.tasks.TaskContext;
import io.etcd.jetcd.Client;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Primary;

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
     * Includes template support for automatic application of matching templates during index creation.
     */
    @Bean
    public IndexManager indexManager(MetadataStore metadataStore, TemplateManager templateManager) {
        log.info("Initializing IndexManager for multi-cluster support with template support");
        return new IndexManager(metadataStore, templateManager);
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
            ShardAllocator shardAllocator,
            ActualAllocationUpdater actualAllocationUpdater,
            GoalStateOrchestrator goalStateOrchestrator,
            MetadataStore metadataStore) {
        // Create Discovery instance but don't expose as separate bean
        Discovery discovery = new Discovery(metadataStore, config.getClusterName());
        return new TaskContext(config.getClusterName(), indexManager, shardAllocator, actualAllocationUpdater, goalStateOrchestrator, discovery);
    }

    /**
     * Expose etcd Client for components that need direct access (e.g., MultiClusterManager)
     */
    @Bean
    public Client etcdClient(MetadataStore metadataStore) {
        return ((EtcdMetadataStore) metadataStore).getEtcdClient();
    }
    
    /**
     * MultiClusterManager bean for managing multiple clusters with distributed locking.
     * Replaces the single TaskManager with multi-cluster coordination.
     * 
     * Note: MultiClusterManager now uses @Component and constructor injection,
     * so it will be auto-created by Spring. We only need to ensure all its
     * dependencies are available as beans (which they are via @ComponentScan).
     * 
     * The @PostConstruct method in MultiClusterManager will handle startup.
     */

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