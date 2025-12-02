package io.clustercontroller.multicluster.registry;

import io.clustercontroller.store.EtcdPathResolver;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.Watch;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.options.GetOption;
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
class ClusterRegistryTest {

    @Mock
    private Client etcdClient;
    
    @Mock
    private KV kvClient;
    
    @Mock
    private Watch watchClient;
    
    @Mock
    private EtcdPathResolver pathResolver;
    
    private ClusterRegistry registry;

    @BeforeEach
    void setUp() {
        when(etcdClient.getKVClient()).thenReturn(kvClient);
        when(etcdClient.getWatchClient()).thenReturn(watchClient);
        
        registry = new ClusterRegistry(etcdClient, pathResolver);
    }

    @Test
    void testListClusters_ReturnsClusterIds() throws Exception {
        // Given
        String prefix = "/multi-cluster/clusters/";
        when(pathResolver.getClustersPrefix()).thenReturn(prefix);
        
        KeyValue kv1 = mock(KeyValue.class);
        when(kv1.getKey()).thenReturn(ByteSequence.from("/multi-cluster/clusters/cluster-1/metadata", UTF_8));
        
        KeyValue kv2 = mock(KeyValue.class);
        when(kv2.getKey()).thenReturn(ByteSequence.from("/multi-cluster/clusters/cluster-2/metadata", UTF_8));
        
        KeyValue kv3 = mock(KeyValue.class);
        when(kv3.getKey()).thenReturn(ByteSequence.from("/multi-cluster/clusters/cluster-3/metadata", UTF_8));
        
        GetResponse getResponse = mock(GetResponse.class);
        when(getResponse.getKvs()).thenReturn(List.of(kv1, kv2, kv3));
        
        when(kvClient.get(any(ByteSequence.class), any(GetOption.class)))
            .thenReturn(CompletableFuture.completedFuture(getResponse));
        
        // When
        Set<String> clusters = registry.listClusters();
        
        // Then
        assertThat(clusters).containsExactlyInAnyOrder("cluster-1", "cluster-2", "cluster-3");
        verify(kvClient).get(eq(ByteSequence.from(prefix, UTF_8)), any(GetOption.class));
    }

    @Test
    void testListClusters_ReturnsEmptyOnError() {
        // Given
        String prefix = "/multi-cluster/clusters/";
        when(pathResolver.getClustersPrefix()).thenReturn(prefix);
        
        when(kvClient.get(any(ByteSequence.class), any(GetOption.class)))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("etcd error")));
        
        // When
        Set<String> clusters = registry.listClusters();
        
        // Then
        assertThat(clusters).isEmpty();
    }

    @Test
    void testListClusters_HandlesInvalidPaths() throws Exception {
        // Given
        String prefix = "/multi-cluster/clusters/";
        when(pathResolver.getClustersPrefix()).thenReturn(prefix);
        
        KeyValue kv1 = mock(KeyValue.class);
        when(kv1.getKey()).thenReturn(ByteSequence.from("/multi-cluster/clusters/cluster-1/metadata", UTF_8));
        
        KeyValue kv2 = mock(KeyValue.class);
        when(kv2.getKey()).thenReturn(ByteSequence.from("/invalid/path", UTF_8)); // Invalid path
        
        KeyValue kv3 = mock(KeyValue.class);
        when(kv3.getKey()).thenReturn(ByteSequence.from("/multi-cluster/clusters/", UTF_8)); // Too short
        
        GetResponse getResponse = mock(GetResponse.class);
        when(getResponse.getKvs()).thenReturn(List.of(kv1, kv2, kv3));
        
        when(kvClient.get(any(ByteSequence.class), any(GetOption.class)))
            .thenReturn(CompletableFuture.completedFuture(getResponse));
        
        // When
        Set<String> clusters = registry.listClusters();
        
        // Then - Should only return valid cluster-1, ignore invalid paths
        assertThat(clusters).containsExactly("cluster-1");
    }

    @Test
    void testWatchClusters_TriggersCallbackOnChange() {
        // Given
        String prefix = "/multi-cluster/clusters/";
        when(pathResolver.getClustersPrefix()).thenReturn(prefix);
        
        // Capture the watch callback
        @SuppressWarnings("unchecked")
        Consumer<WatchResponse>[] callbackCaptor = new Consumer[1];
        
        Watch.Watcher mockWatcher = mock(Watch.Watcher.class);
        when(watchClient.watch(any(ByteSequence.class), any(WatchOption.class), any(Consumer.class)))
            .thenAnswer(invocation -> {
                callbackCaptor[0] = invocation.getArgument(2);
                return mockWatcher;
            });
        
        Runnable onClusterChange = mock(Runnable.class);
        
        // When
        Watch.Watcher watcher = registry.watchClusters(onClusterChange);
        
        // Simulate cluster change
        WatchResponse watchResponse = mock(WatchResponse.class);
        callbackCaptor[0].accept(watchResponse);
        
        // Then
        assertThat(watcher).isNotNull();
        verify(watchClient).watch(
            eq(ByteSequence.from(prefix, UTF_8)),
            any(WatchOption.class),
            any(Consumer.class)
        );
        verify(onClusterChange).run();
    }
}

