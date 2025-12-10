package io.clustercontroller.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

/**
 * IndexShardProgress tracks the progress of an index-shard during rolling updates
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class IndexShardProgress {
    
    private String indexShard; // e.g., "index1/shard0"
    private int goalStateUpdatedCount; // Number of nodes with goal state updated
    private int actualStateConvergedCount; // Number of nodes with actual state converged
    private int totalNodes; // Total number of nodes for this index-shard
    private List<String> updatedNodes = new ArrayList<>(); // Nodes whose goal state has been updated
    
    public IndexShardProgress(String indexShard, int goalStateUpdatedCount, int actualStateConvergedCount, int totalNodes) {
        this.indexShard = indexShard;
        this.goalStateUpdatedCount = goalStateUpdatedCount;
        this.actualStateConvergedCount = actualStateConvergedCount;
        this.totalNodes = totalNodes;
        this.updatedNodes = new ArrayList<>();
    }
    
    /**
     * Get the number of nodes currently in transit (goal updated but not converged)
     */
    public int getTransitNodesCount() {
        return goalStateUpdatedCount - actualStateConvergedCount;
    }
    
    /**
     * Get the percentage of nodes currently in transit
     */
    public double getTransitPercentage() {
        return totalNodes > 0 ? (double) getTransitNodesCount() / totalNodes : 0.0;
    }
    
    /**
     * Check if all nodes have converged
     */
    public boolean isConverged() {
        return goalStateUpdatedCount == totalNodes && actualStateConvergedCount == totalNodes;
    }
    
    /**
     * Check if we can update more nodes (under the transit limit)
     */
    public boolean canUpdateMoreNodes(double maxTransitPercentage) {
        return getTransitPercentage() < maxTransitPercentage;
    }
    
    /**
     * Get the number of available slots for updates
     */
    public int getAvailableSlots(double maxTransitPercentage) {
        int maxTransitNodes = (int) Math.ceil(totalNodes * maxTransitPercentage);
        int currentTransitNodes = getTransitNodesCount();
        return Math.max(0, maxTransitNodes - currentTransitNodes);
    }
}
