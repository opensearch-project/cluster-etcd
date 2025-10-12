package io.clustercontroller.multicluster.lock;

import io.clustercontroller.store.EtcdPathResolver;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.support.CloseableClient;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.Lease;
import io.etcd.jetcd.Lock;
import io.etcd.jetcd.Watch;
import io.etcd.jetcd.lease.LeaseGrantResponse;
import io.etcd.jetcd.lock.LockResponse;
import io.etcd.jetcd.options.WatchOption;
import io.etcd.jetcd.watch.WatchResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DistributedLockManagerTest {

    @Mock
    private Client etcdClient;
    
    @Mock
    private Lease leaseClient;
    
    @Mock
    private Lock lockClient;
    
    @Mock
    private Watch watchClient;
    
    @Mock
    private KV kvClient;
    
    @Mock
    private EtcdPathResolver pathResolver;
    
    @Mock
    private CloseableClient keepAliveObserver;
    
    private DistributedLockManager lockManager;

    @BeforeEach
    void setUp() {
        when(etcdClient.getLeaseClient()).thenReturn(leaseClient);
        when(etcdClient.getLockClient()).thenReturn(lockClient);
        when(etcdClient.getWatchClient()).thenReturn(watchClient);
        when(etcdClient.getKVClient()).thenReturn(kvClient);
        
        lockManager = new DistributedLockManager(etcdClient, pathResolver);
    }

    @Test
    void testAcquireLock_Success() throws Exception {
        // Given
        String clusterId = "cluster-1";
        int ttl = 60;
        long leaseId = 12345L;
        String lockPath = "/multi-cluster/locks/clusters/cluster-1";
        ByteSequence lockKey = ByteSequence.from("lock-key", UTF_8);
        
        when(pathResolver.getClusterLockPath(clusterId)).thenReturn(lockPath);
        
        LeaseGrantResponse leaseResponse = mock(LeaseGrantResponse.class);
        when(leaseResponse.getID()).thenReturn(leaseId);
        when(leaseClient.grant(ttl)).thenReturn(CompletableFuture.completedFuture(leaseResponse));
        when(leaseClient.keepAlive(eq(leaseId), any())).thenReturn(keepAliveObserver);
        
        LockResponse lockResponse = mock(LockResponse.class);
        when(lockResponse.getKey()).thenReturn(lockKey);
        when(lockClient.lock(any(ByteSequence.class), eq(leaseId)))
            .thenReturn(CompletableFuture.completedFuture(lockResponse));
        
        // When
        ClusterLock lock = lockManager.acquireLock(clusterId, ttl);
        
        // Then
        assertThat(lock).isNotNull();
        assertThat(lock.getClusterId()).isEqualTo(clusterId);
        assertThat(lock.getLeaseId()).isEqualTo(leaseId);
        assertThat(lock.getLockKey()).isEqualTo(lockKey);
        assertThat(lock.getKeepAliveObserver()).isEqualTo(keepAliveObserver);
        
        verify(leaseClient).grant(ttl);
        verify(leaseClient).keepAlive(eq(leaseId), any());
        verify(lockClient).lock(ByteSequence.from(lockPath, UTF_8), leaseId);
    }

    @Test
    void testAcquireLock_LeaseGrantFails() {
        // Given
        String clusterId = "cluster-1";
        int ttl = 60;
        
        when(pathResolver.getClusterLockPath(clusterId)).thenReturn("/lock/path");
        when(leaseClient.grant(ttl))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Lease grant failed")));
        
        // When/Then
        assertThatThrownBy(() -> lockManager.acquireLock(clusterId, ttl))
            .isInstanceOf(LockException.class)
            .hasMessageContaining("Failed to acquire lock");
    }

    @Test
    void testAcquireLock_LockAcquisitionFails() {
        // Given
        String clusterId = "cluster-1";
        int ttl = 60;
        long leaseId = 12345L;
        
        when(pathResolver.getClusterLockPath(clusterId)).thenReturn("/lock/path");
        
        LeaseGrantResponse leaseResponse = mock(LeaseGrantResponse.class);
        when(leaseResponse.getID()).thenReturn(leaseId);
        when(leaseClient.grant(ttl)).thenReturn(CompletableFuture.completedFuture(leaseResponse));
        when(leaseClient.keepAlive(eq(leaseId), any())).thenReturn(keepAliveObserver);
        
        when(lockClient.lock(any(ByteSequence.class), eq(leaseId)))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Lock failed")));
        
        // When/Then
        assertThatThrownBy(() -> lockManager.acquireLock(clusterId, ttl))
            .isInstanceOf(LockException.class)
            .hasMessageContaining("Failed to acquire lock");
    }

    @Test
    void testReleaseLock_Success() {
        // Given
        String clusterId = "cluster-1";
        long leaseId = 12345L;
        ByteSequence lockKey = ByteSequence.from("lock-key", UTF_8);
        
        ClusterLock lock = new ClusterLock(clusterId, leaseId, lockKey, keepAliveObserver);
        
        when(lockClient.unlock(lockKey)).thenReturn(CompletableFuture.completedFuture(null));
        when(leaseClient.revoke(leaseId)).thenReturn(CompletableFuture.completedFuture(null));
        
        // When
        lockManager.releaseLock(lock);
        
        // Then
        verify(keepAliveObserver).close();
        verify(lockClient).unlock(lockKey);
        verify(leaseClient).revoke(leaseId);
    }

    @Test
    void testReleaseLock_HandlesExceptions() {
        // Given
        String clusterId = "cluster-1";
        long leaseId = 12345L;
        ByteSequence lockKey = ByteSequence.from("lock-key", UTF_8);
        
        ClusterLock lock = new ClusterLock(clusterId, leaseId, lockKey, keepAliveObserver);
        
        doThrow(new RuntimeException("Close failed")).when(keepAliveObserver).close();
        when(lockClient.unlock(lockKey))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Unlock failed")));
        when(leaseClient.revoke(leaseId))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Revoke failed")));
        
        // When/Then - Should not throw, just log errors
        assertThatCode(() -> lockManager.releaseLock(lock))
            .doesNotThrowAnyException();
    }

    @Test
    void testWatchLock_TriggersCallbackOnLockLoss() {
        // Given
        String clusterId = "cluster-1";
        long leaseId = 12345L;
        ByteSequence lockKey = ByteSequence.from("lock-key", UTF_8);
        String lockPath = "/multi-cluster/locks/clusters/cluster-1";
        
        ClusterLock lock = new ClusterLock(clusterId, leaseId, lockKey, keepAliveObserver);
        
        when(pathResolver.getClusterLockPath(clusterId)).thenReturn(lockPath);
        
        // Capture the watch callback
        @SuppressWarnings("unchecked")
        Consumer<WatchResponse>[] callbackCaptor = new Consumer[1];
        
        Watch.Watcher mockWatcher = mock(Watch.Watcher.class);
        when(watchClient.watch(any(ByteSequence.class), any(WatchOption.class), any(Consumer.class)))
            .thenAnswer(invocation -> {
                callbackCaptor[0] = invocation.getArgument(2);
                return mockWatcher;
            });
        
        Runnable onLockLost = mock(Runnable.class);
        
        // When
        Watch.Watcher watcher = lockManager.watchLock(lock, onLockLost);
        
        // Simulate lock key deletion
        WatchResponse watchResponse = mock(WatchResponse.class);
        callbackCaptor[0].accept(watchResponse);
        
        // Then
        assertThat(watcher).isNotNull();
        verify(watchClient).watch(
            eq(ByteSequence.from(lockPath, UTF_8)),
            any(WatchOption.class),
            any(Consumer.class)
        );
        verify(onLockLost).run();
    }
}

