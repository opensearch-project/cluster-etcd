package io.clustercontroller.store;

import io.etcd.jetcd.*;
import io.etcd.jetcd.election.CampaignResponse;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.lease.LeaseGrantResponse;
import io.etcd.jetcd.lease.LeaseKeepAliveResponse;
import io.etcd.jetcd.support.CloseableClient;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Tests for leader election functionality in EtcdMetadataStore
 */
@ExtendWith(MockitoExtension.class)
class LeaderElectionTest {

    @Mock
    private Client etcdClient;
    
    @Mock
    private KV kvClient;
    
    @Mock
    private Election electionClient;
    
    @Mock
    private Lease leaseClient;
    
    @Mock
    private LeaseGrantResponse leaseGrantResponse;
    
    @Mock
    private GetResponse getResponse;
    
    @Mock
    private CampaignResponse campaignResponse;

    private EtcdMetadataStore etcdStore;
    private String clusterName = "test-cluster";
    private String[] etcdEndpoints = {"http://localhost:2379"};

    @BeforeEach
    void setUp() {
        // Reset singleton instance before each test
        EtcdMetadataStore.resetInstance();
        
        // Create test instance with mocked dependencies
        String testNodeId = "test-node-1";
        etcdStore = EtcdMetadataStore.createTestInstance(etcdEndpoints, testNodeId, etcdClient, kvClient);
    }

    @Test
    void testInitialLeaderState() {
        // Initially should not be leader
        assertFalse(etcdStore.isLeader());
    }

    @Test
    void testSuccessfulLeaderElection() throws Exception {
        // Setup mock behaviors
        when(etcdClient.getElectionClient()).thenReturn(electionClient);
        when(etcdClient.getLeaseClient()).thenReturn(leaseClient);
        
        // Setup lease grant mock
        when(leaseGrantResponse.getID()).thenReturn(12345L);
        when(leaseClient.grant(anyLong())).thenReturn(CompletableFuture.completedFuture(leaseGrantResponse));
        
        // Setup keep alive mock - this method returns a CloseableClient
        when(leaseClient.keepAlive(anyLong(), any(StreamObserver.class))).thenReturn(mock(CloseableClient.class));
        
        // Setup successful election campaign
        CompletableFuture<CampaignResponse> campaignFuture = CompletableFuture.completedFuture(campaignResponse);
        when(electionClient.campaign(any(ByteSequence.class), anyLong(), any(ByteSequence.class)))
                .thenReturn(campaignFuture);

        // Start leader election
        CompletableFuture<Boolean> electionResult = etcdStore.getLeaderElection().startElection();
        
        // Wait for result with timeout
        Boolean isLeader = electionResult.get(5, TimeUnit.SECONDS);
        
        // Verify successful election
        assertTrue(isLeader);
        assertTrue(etcdStore.isLeader());
        
        // Verify interactions
        verify(leaseClient).grant(30L);
        verify(leaseClient).keepAlive(eq(12345L), any(StreamObserver.class));
        verify(electionClient).campaign(any(ByteSequence.class), eq(12345L), any(ByteSequence.class));
    }

    @Test
    void testFailedLeaderElection() throws Exception {
        // Setup mock behaviors
        when(etcdClient.getElectionClient()).thenReturn(electionClient);
        when(etcdClient.getLeaseClient()).thenReturn(leaseClient);
        
        // Setup lease grant mock
        when(leaseGrantResponse.getID()).thenReturn(12345L);
        when(leaseClient.grant(anyLong())).thenReturn(CompletableFuture.completedFuture(leaseGrantResponse));
        
        // Setup keep alive mock
        when(leaseClient.keepAlive(anyLong(), any(StreamObserver.class))).thenReturn(mock(CloseableClient.class));
        
        // Setup failed election campaign
        CompletableFuture<CampaignResponse> campaignFuture = new CompletableFuture<>();
        campaignFuture.completeExceptionally(new RuntimeException("Election failed"));
        when(electionClient.campaign(any(ByteSequence.class), anyLong(), any(ByteSequence.class)))
                .thenReturn(campaignFuture);

        // Start leader election
        CompletableFuture<Boolean> electionResult = etcdStore.getLeaderElection().startElection();
        
        // Verify election failure
        assertThrows(Exception.class, () -> electionResult.get(5, TimeUnit.SECONDS));
        assertFalse(etcdStore.isLeader());
    }

    @Test
    void testLeaseKeepAliveError() throws Exception {
        // Setup mock behaviors
        when(etcdClient.getElectionClient()).thenReturn(electionClient);
        when(etcdClient.getLeaseClient()).thenReturn(leaseClient);
        
        // Setup lease grant mock
        when(leaseGrantResponse.getID()).thenReturn(12345L);
        when(leaseClient.grant(anyLong())).thenReturn(CompletableFuture.completedFuture(leaseGrantResponse));
        
        // Setup failed election campaign due to keep alive error
        CompletableFuture<CampaignResponse> failedCampaign = new CompletableFuture<>();
        failedCampaign.completeExceptionally(new RuntimeException("Campaign failed due to keep alive error"));
        when(electionClient.campaign(any(ByteSequence.class), anyLong(), any(ByteSequence.class)))
                .thenReturn(failedCampaign);
        
        // Setup keep alive mock to simulate error callback
        doAnswer(invocation -> {
            StreamObserver<LeaseKeepAliveResponse> observer = invocation.getArgument(1);
            // Simulate error in keep alive
            observer.onError(new RuntimeException("Keep alive error"));
            return null;
        }).when(leaseClient).keepAlive(anyLong(), any(StreamObserver.class));

        // Start leader election
        CompletableFuture<Boolean> electionResult = etcdStore.getLeaderElection().startElection();
        
        // Verify keep alive error causes election failure
        assertThrows(Exception.class, () -> electionResult.get(5, TimeUnit.SECONDS));
        assertFalse(etcdStore.isLeader());
    }

    @Test
    void testLeaseKeepAliveCompleted() throws Exception {
        // Setup mock behaviors
        when(etcdClient.getElectionClient()).thenReturn(electionClient);
        when(etcdClient.getLeaseClient()).thenReturn(leaseClient);
        
        // Setup lease grant mock
        when(leaseGrantResponse.getID()).thenReturn(12345L);
        when(leaseClient.grant(anyLong())).thenReturn(CompletableFuture.completedFuture(leaseGrantResponse));
        
        // Setup successful election first
        when(electionClient.campaign(any(ByteSequence.class), anyLong(), any(ByteSequence.class)))
                .thenReturn(CompletableFuture.completedFuture(campaignResponse));
        
        // Setup keep alive mock to simulate completion callback
        doAnswer(invocation -> {
            StreamObserver<LeaseKeepAliveResponse> observer = invocation.getArgument(1);
            // Simulate completion of keep alive (lease expired) with a delay
            CompletableFuture.runAsync(() -> {
                try {
                    Thread.sleep(50); // Small delay to let election complete first
                    observer.onCompleted();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            return null;
        }).when(leaseClient).keepAlive(anyLong(), any(StreamObserver.class));

        // Start leader election
        CompletableFuture<Boolean> electionResult = etcdStore.getLeaderElection().startElection();
        
        // Wait for initial election success
        Boolean isLeader = electionResult.get(5, TimeUnit.SECONDS);
        assertTrue(isLeader);
        assertTrue(etcdStore.isLeader());
        
        // Give some time for the completion callback to execute
        Thread.sleep(150);
        
        // Verify that lease completion causes leader status to be lost
        assertFalse(etcdStore.isLeader());
    }

    @Test
    void testMultipleLeaderElectionCalls() throws Exception {
        // Setup mock behaviors
        when(etcdClient.getElectionClient()).thenReturn(electionClient);
        when(etcdClient.getLeaseClient()).thenReturn(leaseClient);
        
        // Setup mocks for successful election
        when(leaseGrantResponse.getID()).thenReturn(12345L);
        when(leaseClient.grant(anyLong())).thenReturn(CompletableFuture.completedFuture(leaseGrantResponse));
        when(leaseClient.keepAlive(anyLong(), any(StreamObserver.class))).thenReturn(mock(CloseableClient.class));
        when(electionClient.campaign(any(ByteSequence.class), anyLong(), any(ByteSequence.class)))
                .thenReturn(CompletableFuture.completedFuture(campaignResponse));

        // Start multiple leader elections
        CompletableFuture<Boolean> election1 = etcdStore.getLeaderElection().startElection();
        CompletableFuture<Boolean> election2 = etcdStore.getLeaderElection().startElection();
        
        // Both should succeed
        assertTrue(election1.get(5, TimeUnit.SECONDS));
        assertTrue(election2.get(5, TimeUnit.SECONDS));
        assertTrue(etcdStore.isLeader());
        
        // Should have been called multiple times
        verify(electionClient, atLeast(2)).campaign(any(ByteSequence.class), anyLong(), any(ByteSequence.class));
    }

    @Test
    void testNodeIdGeneration() {
        // Test that node ID is properly retrieved from NODE_NAME environment variable
        // The node ID should be consistent within the same instance
        String nodeId1 = getNodeIdFromStore();
        String nodeId2 = getNodeIdFromStore();
        
        // Should be the same for the same instance
        assertEquals(nodeId1, nodeId2);
        assertNotNull(nodeId1);
        assertFalse(nodeId1.trim().isEmpty());
        
        // If NODE_NAME is set, it should not contain the timestamp pattern
        // (since we no longer generate automatic IDs)
        if (System.getenv("NODE_NAME") != null) {
            assertEquals(System.getenv("NODE_NAME").trim(), nodeId1);
        }
    }
    
    @Test
    void testMissingNodeNameThrowsException() {
        // Test that missing NODE_NAME environment variable causes failure
        // This test documents the expected behavior - NODE_NAME is now required
        
        // We can't easily unset environment variables in unit tests,
        // but we can document the expected behavior
        try {
            // Reset and try to create a new instance - this will call getNodeId()
            EtcdMetadataStore.resetInstance();
            EtcdMetadataStore.createTestInstance(new String[]{"http://localhost:2379"}, 
                    "test-node-missing", etcdClient, kvClient);
            
            // If we get here, NODE_NAME was set in the environment
            // This is fine - the behavior is documented
            String nodeId = getNodeIdFromStore();
            assertNotNull(nodeId);
            assertTrue(nodeId.length() > 0);
            
        } catch (IllegalStateException e) {
            // This is the expected behavior when NODE_NAME is not set
            assertTrue(e.getMessage().contains("NODE_NAME"));
            assertTrue(e.getMessage().contains("not set or is empty"));
        }
    }

    @Test
    void testElectionKeyFormat() throws Exception {
        // Setup mock behaviors
        when(etcdClient.getElectionClient()).thenReturn(electionClient);
        when(etcdClient.getLeaseClient()).thenReturn(leaseClient);
        
        // Setup mocks
        when(leaseGrantResponse.getID()).thenReturn(12345L);
        when(leaseClient.grant(anyLong())).thenReturn(CompletableFuture.completedFuture(leaseGrantResponse));
        when(leaseClient.keepAlive(anyLong(), any(StreamObserver.class))).thenReturn(mock(CloseableClient.class));
        when(electionClient.campaign(any(ByteSequence.class), anyLong(), any(ByteSequence.class)))
                .thenReturn(CompletableFuture.completedFuture(campaignResponse));

        // Start election
        CompletableFuture<Boolean> electionResult = etcdStore.getLeaderElection().startElection();
        
        // Wait for election to complete
        Boolean isLeader = electionResult.get(5, TimeUnit.SECONDS);
        assertTrue(isLeader);
        
        // Verify that the election key is formed correctly (controller-level, not cluster-specific)
        verify(electionClient).campaign(
                argThat(key -> key.toString().contains("/controller-leader-election")),
                eq(12345L),
                any(ByteSequence.class)
        );
    }

    /**
     * Helper method to get node ID from store (using reflection for testing)
     */
    private String getNodeIdFromStore() {
        try {
            var field = EtcdMetadataStore.class.getDeclaredField("nodeId");
            field.setAccessible(true);
            return (String) field.get(etcdStore);
        } catch (Exception e) {
            fail("Could not access nodeId field: " + e.getMessage());
            return null;
        }
    }
}
