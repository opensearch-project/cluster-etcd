package io.clustercontroller.orchestration;

import io.clustercontroller.metrics.MetricsProvider;
import io.clustercontroller.store.MetadataStore;
import lombok.extern.slf4j.Slf4j;

/**
 * Main orchestrator that coordinates goal state updates from planned allocations
 */
@Slf4j
public class GoalStateOrchestrator {
    
    private final MetadataStore metadataStore;
    private final GoalStateOrchestrationStrategy strategy;
    
    public GoalStateOrchestrator(MetadataStore metadataStore, MetricsProvider metricsProvider) {
        this.metadataStore = metadataStore;
        // TODO: Make orchestration strategy configurable via application properties
        this.strategy = new RollingUpdateOrchestrationStrategy(metadataStore, metricsProvider);
    }
    
    /**
     * Orchestrate goal states for all indexes and shards in one controller task iteration
     * 
     * @param clusterId the cluster ID to orchestrate goal states for
     */
    public void orchestrateGoalStates(String clusterId) {
        log.info("Starting goal state orchestration for cluster: {}", clusterId);
        
        try {
            // Apply orchestration strategy
            strategy.orchestrate(clusterId);
            
            log.info("Completed goal state orchestration for cluster: {}", clusterId);
        } catch (Exception e) {
            log.error("Failed to orchestrate goal states for cluster {}: {}", clusterId, e.getMessage(), e);
        }
    }
}
