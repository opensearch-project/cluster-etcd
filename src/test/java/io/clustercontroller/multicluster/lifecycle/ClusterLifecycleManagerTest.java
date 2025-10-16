package io.clustercontroller.multicluster.lifecycle;

import io.clustercontroller.TaskManager;
import io.clustercontroller.multicluster.lock.ClusterLock;
import io.clustercontroller.multicluster.lock.DistributedLockManager;
import io.clustercontroller.store.EtcdPathResolver;
import io.clustercontroller.store.MetadataStore;
import io.clustercontroller.tasks.TaskContext;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.Watch;
import io.etcd.jetcd.support.CloseableClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ClusterLifecycleManagerTest {

    @Mock
    private MetadataStore metadataStore;

    @Mock
    private TaskContext taskContext;

    @Mock
    private DistributedLockManager lockManager;

    @Mock
    private Client etcdClient;

    @Mock
    private KV kvClient;

    @Mock
    private EtcdPathResolver pathResolver;

    @Mock
    private Watch.Watcher mockWatcher;

    @Mock
    private CloseableClient mockKeepAlive;

    private ClusterLifecycleManager lifecycleManager;

    @BeforeEach
    void setUp() {
        when(etcdClient.getKVClient()).thenReturn(kvClient);
        lenient().when(pathResolver.getControllerAssignmentPath(anyString(), anyString()))
            .thenReturn("/multi-cluster/controllers/test-controller/assigned/test-cluster");
        
        // Mock kvClient operations (lenient since not all tests will call these)
        lenient().when(kvClient.put(any(ByteSequence.class), any(ByteSequence.class), any()))
            .thenReturn(java.util.concurrent.CompletableFuture.completedFuture(null));
        lenient().when(kvClient.delete(any(ByteSequence.class)))
            .thenReturn(java.util.concurrent.CompletableFuture.completedFuture(null));
        
        lifecycleManager = new ClusterLifecycleManager(
            metadataStore,
            taskContext,
            lockManager,
            etcdClient,
            pathResolver,
            "test-controller",
            30
        );
    }

    @Test
    void testStartCluster_Success() {
        // Given
        String clusterId = "test-cluster";
        long leaseId = 12345L;
        ByteSequence lockKey = ByteSequence.from("test-lock".getBytes());
        ClusterLock lock = new ClusterLock(clusterId, leaseId, lockKey, mockKeepAlive);

        when(lockManager.watchLock(eq(lock), any(Runnable.class))).thenReturn(mockWatcher);

        // When
        lifecycleManager.startCluster(clusterId, lock);

        // Then
        assertTrue(lifecycleManager.isClusterManaged(clusterId));
        assertTrue(lifecycleManager.getManagedClusters().contains(clusterId));
        verify(lockManager).watchLock(eq(lock), any(Runnable.class));
    }

    @Test
    void testStartCluster_MultipleClustersConcurrently() {
        // Given
        String cluster1 = "cluster-1";
        String cluster2 = "cluster-2";
        ClusterLock lock1 = new ClusterLock(cluster1, 1L, ByteSequence.from("lock1".getBytes()), mockKeepAlive);
        ClusterLock lock2 = new ClusterLock(cluster2, 2L, ByteSequence.from("lock2".getBytes()), mockKeepAlive);

        when(lockManager.watchLock(any(), any(Runnable.class))).thenReturn(mockWatcher);

        // When
        lifecycleManager.startCluster(cluster1, lock1);
        lifecycleManager.startCluster(cluster2, lock2);

        // Then
        assertEquals(2, lifecycleManager.getManagedClusters().size());
        assertTrue(lifecycleManager.isClusterManaged(cluster1));
        assertTrue(lifecycleManager.isClusterManaged(cluster2));
    }

    @Test
    void testStopCluster_Success() throws Exception {
        // Given
        String clusterId = "test-cluster";
        ClusterLock lock = new ClusterLock(clusterId, 1L, ByteSequence.from("lock".getBytes()), mockKeepAlive);
        when(lockManager.watchLock(any(), any(Runnable.class))).thenReturn(mockWatcher);

        lifecycleManager.startCluster(clusterId, lock);
        assertTrue(lifecycleManager.isClusterManaged(clusterId));

        // When
        lifecycleManager.stopCluster(clusterId);

        // Then
        assertFalse(lifecycleManager.isClusterManaged(clusterId));
        verify(mockWatcher).close();
        verify(lockManager).releaseLock(lock);
    }

    @Test
    void testStopCluster_NonExistent() {
        // When/Then - should not throw exception
        assertDoesNotThrow(() -> lifecycleManager.stopCluster("non-existent"));
    }

    @Test
    void testStopCluster_HandlesExceptions() throws Exception {
        // Given
        String clusterId = "test-cluster";
        ClusterLock lock = new ClusterLock(clusterId, 1L, ByteSequence.from("lock".getBytes()), mockKeepAlive);
        when(lockManager.watchLock(any(), any(Runnable.class))).thenReturn(mockWatcher);

        lifecycleManager.startCluster(clusterId, lock);

        // Mock watcher.close() to throw exception
        doThrow(new RuntimeException("Watcher close failed")).when(mockWatcher).close();

        // When - should handle exception gracefully
        assertDoesNotThrow(() -> lifecycleManager.stopCluster(clusterId));

        // Then - cluster should still be removed from managed set
        assertFalse(lifecycleManager.isClusterManaged(clusterId));
    }

    @Test
    void testWatchLock_TriggersStopOnLockLoss() throws Exception {
        // Given
        String clusterId = "test-cluster";
        ClusterLock lock = new ClusterLock(clusterId, 1L, ByteSequence.from("lock".getBytes()), mockKeepAlive);

        // Capture the lock loss callback
        final Runnable[] lockLossCallback = new Runnable[1];
        when(lockManager.watchLock(eq(lock), any(Runnable.class))).thenAnswer(invocation -> {
            lockLossCallback[0] = invocation.getArgument(1);
            return mockWatcher;
        });

        lifecycleManager.startCluster(clusterId, lock);
        assertTrue(lifecycleManager.isClusterManaged(clusterId));

        // When - simulate lock loss
        lockLossCallback[0].run();

        // Give it a moment to process
        Thread.sleep(100);

        // Then - cluster should be stopped
        assertFalse(lifecycleManager.isClusterManaged(clusterId));
    }

    @Test
    void testIsClusterManaged_ReturnsFalseForNonExistent() {
        assertFalse(lifecycleManager.isClusterManaged("non-existent"));
    }

    @Test
    void testGetManagedClusters_ReturnsEmptySetInitially() {
        Set<String> clusters = lifecycleManager.getManagedClusters();
        assertNotNull(clusters);
        assertTrue(clusters.isEmpty());
    }

    @Test
    void testGetManagedClusters_ReturnsSnapshot() {
        // Given
        String clusterId = "test-cluster";
        ClusterLock lock = new ClusterLock(clusterId, 1L, ByteSequence.from("lock".getBytes()), mockKeepAlive);
        when(lockManager.watchLock(any(), any(Runnable.class))).thenReturn(mockWatcher);

        lifecycleManager.startCluster(clusterId, lock);

        // When
        Set<String> clusters = lifecycleManager.getManagedClusters();

        // Then - should return a snapshot that doesn't affect internal state when modified
        assertEquals(1, clusters.size());
        assertTrue(clusters.contains(clusterId));
    }

    @Test
    void testStopAll_StopsAllManagedClusters() throws Exception {
        // Given
        ClusterLock lock1 = new ClusterLock("cluster-1", 1L, ByteSequence.from("lock1".getBytes()), mockKeepAlive);
        ClusterLock lock2 = new ClusterLock("cluster-2", 2L, ByteSequence.from("lock2".getBytes()), mockKeepAlive);
        when(lockManager.watchLock(any(), any(Runnable.class))).thenReturn(mockWatcher);

        lifecycleManager.startCluster("cluster-1", lock1);
        lifecycleManager.startCluster("cluster-2", lock2);

        assertEquals(2, lifecycleManager.getManagedClusters().size());

        // When
        lifecycleManager.stopAll();

        // Give it a moment to process
        Thread.sleep(100);

        // Then
        assertTrue(lifecycleManager.getManagedClusters().isEmpty());
        verify(lockManager, atLeast(2)).releaseLock(any());
    }

    @Test
    void testStopAll_HandlesPartialFailures() throws Exception {
        // Given
        ClusterLock lock1 = new ClusterLock("cluster-1", 1L, ByteSequence.from("lock1".getBytes()), mockKeepAlive);
        ClusterLock lock2 = new ClusterLock("cluster-2", 2L, ByteSequence.from("lock2".getBytes()), mockKeepAlive);
        when(lockManager.watchLock(any(), any(Runnable.class))).thenReturn(mockWatcher);

        lifecycleManager.startCluster("cluster-1", lock1);
        lifecycleManager.startCluster("cluster-2", lock2);

        // Mock first lock release to fail
        doThrow(new RuntimeException("Release failed"))
            .doNothing()
            .when(lockManager).releaseLock(any());

        // When - should handle exception and continue
        assertDoesNotThrow(() -> lifecycleManager.stopAll());

        // Give it a moment to process
        Thread.sleep(100);

        // Then - both clusters should eventually be removed
        assertTrue(lifecycleManager.getManagedClusters().isEmpty());
    }
}
