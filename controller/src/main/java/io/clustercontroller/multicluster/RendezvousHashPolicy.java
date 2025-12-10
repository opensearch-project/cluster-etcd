package io.clustercontroller.multicluster;

import lombok.extern.slf4j.Slf4j;

import java.util.HashSet;
import java.util.Set;

/**
 * Rendezvous Hashing (Highest Random Weight) policy for cluster assignment.
 * 
 * This policy provides:
 * - Sticky assignment: Same cluster tends to stay with same controller
 * - Fair distribution: Clusters distributed evenly across controllers
 * - Minimal movement: When controllers join/leave, only affected clusters move
 * - No coordination needed: Each controller independently calculates same result
 * 
 * Algorithm:
 * 1. For each cluster, calculate hash(cluster-id + controller-id) for all controllers
 * 2. Controller with highest hash "wins" that cluster
 * 3. Each controller independently knows if it should own a cluster
 */
@Slf4j
public class RendezvousHashPolicy implements AssignmentPolicy {
    
    private final int capacity;
    private final int topK; // Usually 1 for exclusive ownership
    
    private String myId = "";
    private Set<String> controllers = Set.of();
    private Set<String> clusters = Set.of();
    private final Set<String> myAssignments = new HashSet<>();
    
    /**
     * @param capacity Maximum number of clusters this controller can manage
     * @param topK Number of top-ranked controllers that should attempt acquisition (usually 1)
     */
    public RendezvousHashPolicy(int capacity, int topK) {
        this.capacity = capacity;
        this.topK = topK;
    }
    
    @Override
    public void refresh(String myId, Set<String> allControllers, Set<String> allClusters, Set<String> myCurrentAssignments) {
        this.myId = myId;
        this.controllers = Set.copyOf(allControllers);
        this.clusters = Set.copyOf(allClusters);
        this.myAssignments.clear();
        this.myAssignments.addAll(myCurrentAssignments);
        
        log.info("Policy refresh: myId={}, controllers={}, clusters={}, current assignments={}", 
            myId, controllers.size(), clusters.size(), myAssignments.size());
    }
    
    @Override
    public boolean shouldAttempt(String clusterId, int myRunningCount) {
        // Don't attempt if no controllers or clusters in the system
        if (controllers.isEmpty() || !clusters.contains(clusterId)) {
            return false;
        }
        
        // Don't exceed capacity
        if (myRunningCount >= capacity) {
            log.debug("At capacity ({}/{}), not attempting cluster: {}", myRunningCount, capacity, clusterId);
            return false;
        }
        
        // Calculate if I'm in the topK controllers for this cluster
        long myScore = score(clusterId, myId);
        int myRank = 1;
        
        for (String controllerId : controllers) {
            if (controllerId.equals(myId)) continue;
            
            long theirScore = score(clusterId, controllerId);
            if (unsignedGreater(theirScore, myScore)) {
                myRank++;
                if (myRank > topK) {
                    log.debug("Rank {} > topK {}, not attempting cluster: {}", myRank, topK, clusterId);
                    return false;
                }
            }
        }
        
        log.info("Rank {} <= topK {}, SHOULD attempt cluster: {}", myRank, topK, clusterId);
        return true;
    }
    
    @Override
    public boolean shouldRelease(String clusterId) {
        // If I currently own this cluster, check if I should still own it
        if (!myAssignments.contains(clusterId)) {
            return false; // Don't own it, nothing to release
        }
        
        // Calculate if I'm still in topK for this cluster
        long myScore = score(clusterId, myId);
        int myRank = 1;
        
        for (String controllerId : controllers) {
            if (controllerId.equals(myId)) continue;
            
            long theirScore = score(clusterId, controllerId);
            if (unsignedGreater(theirScore, myScore)) {
                myRank++;
            }
        }
        
        // If I'm no longer in topK, I should release
        if (myRank > topK) {
            log.info("Rank {} > topK {}, SHOULD release cluster: {}", myRank, topK, clusterId);
            return true;
        }
        
        // If I'm over capacity, release clusters where I have the lowest affinity
        // (This handles the case where a single controller is over capacity)
        if (myAssignments.size() > capacity) {
            // Count how many of my assignments have a higher score than this one
            int betterAssignments = 0;
            for (String otherCluster : myAssignments) {
                if (!otherCluster.equals(clusterId)) {
                    long otherScore = score(otherCluster, myId);
                    if (unsignedGreater(otherScore, myScore)) {
                        betterAssignments++;
                    }
                }
            }
            
            // Keep the top 'capacity' clusters, release the rest
            boolean shouldRelease = betterAssignments >= capacity;
            if (shouldRelease) {
                log.info("Over capacity ({}/{}), releasing lower-affinity cluster: {}", 
                    myAssignments.size(), capacity, clusterId);
            }
            return shouldRelease;
        }
        
        return false;
    }
    
    /**
     * Compare two unsigned longs
     */
    private static boolean unsignedGreater(long a, long b) {
        return Long.compareUnsigned(a, b) > 0;
    }
    
    /**
     * Calculate Rendezvous hash score for (cluster, controller) pair.
     * Uses FNV-1a 64-bit hash for lightweight, deterministic hashing.
     * 
     * In production, consider using xxHash or MurmurHash for better distribution.
     */
    private static long score(String clusterId, String controllerId) {
        final String combined = clusterId + "|" + controllerId;
        long hash = 0xcbf29ce484222325L; // FNV-1a 64-bit offset basis
        
        for (int i = 0; i < combined.length(); i++) {
            hash ^= combined.charAt(i);
            hash *= 0x100000001b3L; // FNV-1a 64-bit prime
        }
        
        return hash;
    }
}
