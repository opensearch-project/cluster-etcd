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

import lombok.extern.slf4j.Slf4j;
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
        ClusterControllerConfig config = new ClusterControllerConfig(
            DEFAULT_CLUSTER_NAME, 
            new String[]{DEFAULT_ETCD_ENDPOINT}, 
            DEFAULT_TASK_INTERVAL_SECONDS
        );
        log.info("Loaded configuration with default cluster: {}", config.getClusterName());
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
    public ClusterHealthManager clusterHealthManager(Discovery discovery, MetadataStore metadataStore) {
        log.info("Initializing ClusterHealthManager for multi-cluster support");
        return new ClusterHealthManager(discovery, metadataStore);
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
     * TaskManager bean for scheduling and executing background tasks.
     */
    @Bean
    public TaskManager taskManager(MetadataStore metadataStore, TaskContext taskContext, ClusterControllerConfig config) {
        log.info("Initializing TaskManager for cluster: {}", config.getClusterName());
        TaskManager taskManager = new TaskManager(metadataStore, taskContext, config.getClusterName(), config.getTaskIntervalSeconds());
        taskManager.start();
        log.info("TaskManager started with background processing for cluster: {}", config.getClusterName());
        return taskManager;
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
        return new TaskContext(config.getClusterName(), indexManager, discovery, shardAllocator, actualAllocationUpdater, goalStateOrchestrator);
    }
}