package io.clustercontroller;

import io.clustercontroller.allocation.ActualAllocationUpdater;
import io.clustercontroller.allocation.ShardAllocator;
import io.clustercontroller.config.ClusterControllerConfig;
import io.clustercontroller.discovery.Discovery;
import io.clustercontroller.health.ClusterHealthManager;
import io.clustercontroller.indices.AliasManager;
import io.clustercontroller.indices.IndexManager;
import io.clustercontroller.templates.TemplateManager;
import io.clustercontroller.store.MetadataStore;
import io.clustercontroller.store.EtcdMetadataStore;
import io.clustercontroller.tasks.TaskContext;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Primary;

import static io.clustercontroller.config.Constants.*;

/**
 * Main Spring Boot application class for the Cluster Controller.
 * 
 * This application provides production-ready controller functionality for managing
 * distributed clusters at scale, including shard allocation, cluster coordination,
 * automated operations, and REST APIs backed by pluggable metadata stores.
 */
@Slf4j
@SpringBootApplication
@ComponentScan(basePackages = "io.clustercontroller")
public class ClusterControllerApplication {

    public static void main(String[] args) {
        log.info("Starting Cluster Controller Application with REST APIs");
        
        try {
            SpringApplication.run(ClusterControllerApplication.class, args);
            log.info("Cluster Controller with REST APIs started successfully");
            
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
        log.info("Loaded configuration for cluster: {}", config.getClusterName());
        return config;
    }
    
    @Bean
    public MetadataStore metadataStore(ClusterControllerConfig config) {
        log.info("Initializing MetadataStore connection to etcd");
        try {
            MetadataStore store = EtcdMetadataStore.getInstance(
                config.getClusterName(), 
                config.getEtcdEndpoints()
            );
            store.initialize();
            log.info("MetadataStore initialized successfully");
            return store;
        } catch (Exception e) {
            log.error("Failed to initialize MetadataStore: {}", e.getMessage(), e);
            throw new RuntimeException("MetadataStore initialization failed", e);
        }
    }
    
    @Bean
    public IndexManager indexManager(MetadataStore metadataStore) {
        log.info("Initializing IndexManager");
        return new IndexManager(metadataStore);
    }
    
    @Bean
    public Discovery discovery(MetadataStore metadataStore) {
        log.info("Initializing Discovery");
        return new Discovery(metadataStore);
    }
    
    @Bean
    public ClusterHealthManager clusterHealthManager(Discovery discovery, MetadataStore metadataStore) {
        log.info("Initializing ClusterHealthManager");
        return new ClusterHealthManager(discovery, metadataStore);
    }
    
    @Bean
    public AliasManager aliasManager(MetadataStore metadataStore) {
        log.info("Initializing AliasManager");
        return new AliasManager(metadataStore);
    }
    
    @Bean
    public TemplateManager templateManager(MetadataStore metadataStore) {
        log.info("Initializing TemplateManager");
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
    
    @Bean
    public TaskManager taskManager(MetadataStore metadataStore, 
                                   IndexManager indexManager,
                                   Discovery discovery,
                                   ShardAllocator shardAllocator,
                                   ActualAllocationUpdater actualAllocationUpdater,
                                   ClusterControllerConfig config) {
        
        log.info("Initializing TaskManager with background processing");
        
        TaskContext taskContext = new TaskContext(indexManager, discovery, shardAllocator, actualAllocationUpdater);
        
        TaskManager taskManager = new TaskManager(
            metadataStore,
            taskContext,
            config.getTaskIntervalSeconds()
        );
        
        taskManager.start();
        log.info("TaskManager started with background processing");
        
        return taskManager;
    }
}