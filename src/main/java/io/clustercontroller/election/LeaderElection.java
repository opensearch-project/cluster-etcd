package io.clustercontroller.election;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.Election;
import io.etcd.jetcd.lease.LeaseGrantResponse;
import io.etcd.jetcd.lease.LeaseKeepAliveResponse;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.clustercontroller.config.Constants.*;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Handles leader election logic for the cluster controller.
 * Extracted to a separate class for easier testing and maintainability.
 */
@Slf4j
public class LeaderElection {
    
    private static final String CONTROLLER_ELECTION_KEY = "/controller-leader-election";
    
    private final Client etcdClient;
    private final String nodeId;
    private final AtomicBoolean isLeader;
    
    /**
     * Constructor for LeaderElection
     * Leader election is at the controller level - the winning controller manages all clusters.
     * 
     * @param etcdClient the etcd client instance
     * @param nodeId the unique identifier for this node
     * @param isLeader atomic boolean to track leader state (shared with caller)
     */
    public LeaderElection(Client etcdClient, String nodeId, AtomicBoolean isLeader) {
        this.etcdClient = etcdClient;
        this.nodeId = nodeId;
        this.isLeader = isLeader;
    }
    
    /**
     * Start the leader election process asynchronously.
     * This method initiates an etcd election campaign and returns immediately.
     * The election runs in the background, and the CompletableFuture completes when leadership is acquired.
     * 
     * @return CompletableFuture that completes with true when this node becomes leader
     */
    public CompletableFuture<Boolean> startElection() {
        log.info("LeaderElection - Starting controller-level leader election for node: {}", nodeId);
        log.info("LeaderElection - Election key: {}", CONTROLLER_ELECTION_KEY);
        
        Election election = etcdClient.getElectionClient();
        CompletableFuture<Boolean> result = new CompletableFuture<>();

        CompletableFuture.runAsync(() -> {
            try {
                log.info("LeaderElection - Node {} attempting to campaign for controller leadership", nodeId);
                
                ByteSequence electionKeyBytes = ByteSequence.from(CONTROLLER_ELECTION_KEY, UTF_8);
                ByteSequence nodeIdBytes = ByteSequence.from(nodeId, UTF_8);

                // Create a lease for the election
                long ttlSeconds = LEADER_ELECTION_TTL_SECONDS;
                LeaseGrantResponse leaseGrant = etcdClient.getLeaseClient()
                        .grant(ttlSeconds)
                        .get();
                long leaseId = leaseGrant.getID();
                
                log.info("LeaderElection - Node {} obtained lease ID: {} with TTL: {}s", nodeId, leaseId, ttlSeconds);

                // Keep the lease alive
                etcdClient.getLeaseClient().keepAlive(leaseId, new StreamObserver<LeaseKeepAliveResponse>() {
                    @Override
                    public void onNext(LeaseKeepAliveResponse res) {
                        log.debug("LeaderElection - Lease keep-alive successful for node {}", nodeId);
                    }
                    
                    @Override
                    public void onError(Throwable t) {
                        log.error("LeaderElection - Lease keep-alive error for node {}: {}", nodeId, t.getMessage());
                        isLeader.set(false);
                        result.completeExceptionally(t);
                    }
                    
                    @Override
                    public void onCompleted() {
                        log.warn("LeaderElection - Lease keep-alive completed for node {}, stepping down from leadership", nodeId);
                        isLeader.set(false);
                    }
                });

                log.info("LeaderElection - Node {} starting campaign with lease ID: {}", nodeId, leaseId);
                
                // Campaign for leadership - this blocks until leadership is acquired
                election.campaign(electionKeyBytes, leaseId, nodeIdBytes)
                        .thenAccept(leaderKey -> {
                            log.info("LeaderElection - ✓ SUCCESS: Node {} has WON the election and is now the LEADER!", nodeId);
                            isLeader.set(true);
                            result.complete(true);
                        })
                        .exceptionally(ex -> {
                            log.error("LeaderElection - Node {} failed during campaign: {}", nodeId, ex.getMessage(), ex);
                            isLeader.set(false);
                            result.completeExceptionally(ex);
                            return null;
                        });

            } catch (Exception e) {
                log.error("LeaderElection - Election error for node {}: {}", nodeId, e.getMessage(), e);
                isLeader.set(false);
                result.completeExceptionally(e);
            }
        });
        
        log.info("LeaderElection - Election initiated asynchronously for node: {}", nodeId);
        return result;
    }
    
    /**
     * Check if this node is currently the leader
     * 
     * @return true if this node is the leader, false otherwise
     */
    public boolean isLeader() {
        return isLeader.get();
    }
}
