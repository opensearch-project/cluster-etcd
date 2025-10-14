package io.clustercontroller.multicluster.registry;

import io.clustercontroller.store.EtcdPathResolver;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.support.CloseableClient;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.Lease;
import io.etcd.jetcd.Watch;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.kv.PutResponse;
import io.etcd.jetcd.lease.LeaseGrantResponse;
import io.etcd.jetcd.options.GetOption;
import io.etcd.jetcd.options.PutOption;
import io.etcd.jetcd.options.WatchOption;
import io.etcd.jetcd.watch.WatchResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ControllerRegistryTest {

    @Mock
    private Client etcdClient;
    
    @Mock
    private KV kvClient;
    
    @Mock
    private Lease leaseClient;
    
    @Mock
    private Watch watchClient;
    
    @Mock
    private EtcdPathResolver pathResolver;
    
    @Mock
    private CloseableClient keepAliveObserver;
    
    private ControllerRegistry registry;

    @BeforeEach
    void setUp() {
        when(etcdClient.getKVClient()).thenReturn(kvClient);
        when(etcdClient.getLeaseClient()).thenReturn(leaseClient);
        when(etcdClient.getWatchClient()).thenReturn(watchClient);
        
        registry = new ControllerRegistry(etcdClient, pathResolver);
    }

    @Test
    void testRegister_Success() throws Exception {
        // Given
        String controllerId = "controller-1";
        int ttl = 60;
        long leaseId = 12345L;
        String heartbeatPath = "/multi-cluster/controllers/controller-1/heartbeat";
        
        when(pathResolver.getControllerHeartbeatPath(controllerId)).thenReturn(heartbeatPath);
        
        LeaseGrantResponse leaseResponse = mock(LeaseGrantResponse.class);
        when(leaseResponse.getID()).thenReturn(leaseId);
        when(leaseClient.grant(ttl)).thenReturn(CompletableFuture.completedFuture(leaseResponse));
        when(leaseClient.keepAlive(eq(leaseId), any())).thenReturn(keepAliveObserver);
        
        PutResponse putResponse = mock(PutResponse.class);
        when(kvClient.put(any(ByteSequence.class), any(ByteSequence.class), any(PutOption.class)))
            .thenReturn(CompletableFuture.completedFuture(putResponse));
        
        // When
        ControllerRegistration registration = registry.register(controllerId, ttl);
        
        // Then
        assertThat(registration).isNotNull();
        assertThat(registration.getControllerId()).isEqualTo(controllerId);
        assertThat(registration.getLeaseId()).isEqualTo(leaseId);
        assertThat(registration.getKeepAliveObserver()).isEqualTo(keepAliveObserver);
        
        verify(leaseClient).grant(ttl);
        verify(leaseClient).keepAlive(eq(leaseId), any());
        verify(kvClient).put(
            eq(ByteSequence.from(heartbeatPath, UTF_8)),
            any(ByteSequence.class),  // Heartbeat value is a timestamp, not a constant
            any(PutOption.class)
        );
    }

    @Test
    void testDeregister_Success() {
        // Given
        String controllerId = "controller-1";
        long leaseId = 12345L;
        ControllerRegistration registration = new ControllerRegistration(controllerId, leaseId, keepAliveObserver);
        
        when(leaseClient.revoke(leaseId)).thenReturn(CompletableFuture.completedFuture(null));
        
        // When
        registry.deregister(registration);
        
        // Then
        verify(keepAliveObserver).close();
        verify(leaseClient).revoke(leaseId);
    }

    @Test
    void testListActiveControllers_ReturnsControllerIds() throws Exception {
        // Given
        String prefix = "/multi-cluster/controllers/";
        when(pathResolver.getControllersPrefix()).thenReturn(prefix);
        
        KeyValue kv1 = mock(KeyValue.class);
        when(kv1.getKey()).thenReturn(ByteSequence.from("/multi-cluster/controllers/controller-1/heartbeat", UTF_8));
        
        KeyValue kv2 = mock(KeyValue.class);
        when(kv2.getKey()).thenReturn(ByteSequence.from("/multi-cluster/controllers/controller-2/heartbeat", UTF_8));
        
        KeyValue kv3 = mock(KeyValue.class);
        when(kv3.getKey()).thenReturn(ByteSequence.from("/multi-cluster/controllers/controller-3/assigned/cluster-1", UTF_8));
        
        GetResponse getResponse = mock(GetResponse.class);
        when(getResponse.getKvs()).thenReturn(List.of(kv1, kv2, kv3));
        
        when(kvClient.get(any(ByteSequence.class), any(GetOption.class)))
            .thenReturn(CompletableFuture.completedFuture(getResponse));
        
        // When
        Set<String> controllers = registry.listActiveControllers();
        
        // Then
        assertThat(controllers).containsExactlyInAnyOrder("controller-1", "controller-2");
        verify(kvClient).get(eq(ByteSequence.from(prefix, UTF_8)), any(GetOption.class));
    }

    @Test
    void testListActiveControllers_ReturnsEmptyOnError() {
        // Given
        String prefix = "/multi-cluster/controllers/";
        when(pathResolver.getControllersPrefix()).thenReturn(prefix);
        
        when(kvClient.get(any(ByteSequence.class), any(GetOption.class)))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("etcd error")));
        
        // When
        Set<String> controllers = registry.listActiveControllers();
        
        // Then
        assertThat(controllers).isEmpty();
    }

    @Test
    void testWatchControllers_TriggersCallbackOnChange() {
        // Given
        String prefix = "/multi-cluster/controllers/";
        when(pathResolver.getControllersPrefix()).thenReturn(prefix);
        
        // Capture the watch callback
        @SuppressWarnings("unchecked")
        Consumer<WatchResponse>[] callbackCaptor = new Consumer[1];
        
        Watch.Watcher mockWatcher = mock(Watch.Watcher.class);
        when(watchClient.watch(any(ByteSequence.class), any(WatchOption.class), (Consumer<WatchResponse>) any()))
            .thenAnswer(invocation -> {
                callbackCaptor[0] = invocation.getArgument(2);
                return mockWatcher;
            });
        
        Runnable onMembershipChange = mock(Runnable.class);
        
        // When
        Watch.Watcher watcher = registry.watchControllers(onMembershipChange);
        
        // Simulate controller change
        WatchResponse watchResponse = mock(WatchResponse.class);
        callbackCaptor[0].accept(watchResponse);
        
        // Then
        assertThat(watcher).isNotNull();
        verify(watchClient).watch(
            eq(ByteSequence.from(prefix, UTF_8)),
            any(WatchOption.class),
            (Consumer<WatchResponse>) any()
        );
        verify(onMembershipChange).run();
    }
}

