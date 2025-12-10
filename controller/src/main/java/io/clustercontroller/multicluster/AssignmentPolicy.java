package io.clustercontroller.multicluster;

import java.util.Set;

/**
 * Policy interface for determining cluster assignments across controllers.
 * Implementations decide which controllers should attempt to acquire which clusters.
 */
public interface AssignmentPolicy {
    
    /**
     * Called whenever cluster/controller membership changes or we (re)start.
     * Allows the policy to recalculate assignments based on current state.
     * 
     * @param myId This controller's ID
     * @param allControllers Set of all active controller IDs
     * @param allClusters Set of all registered cluster IDs
     * @param myCurrentAssignments Set of cluster IDs currently managed by this controller
     */
    void refresh(String myId, Set<String> allControllers, Set<String> allClusters, Set<String> myCurrentAssignments);
    
    /**
     * Decide whether this controller should attempt to acquire a specific cluster.
     * 
     * @param clusterId The cluster ID to check
     * @param myRunningCount Number of clusters currently managed by this controller
     * @return true if this controller should try to acquire the cluster, false otherwise
     */
    boolean shouldAttempt(String clusterId, int myRunningCount);
    
    /**
     * Decide whether this controller should release a cluster it currently owns.
     * Used for rebalancing when new controllers join.
     * 
     * @param clusterId The cluster ID to check
     * @return true if this controller should release the cluster, false otherwise
     */
    boolean shouldRelease(String clusterId);
}
