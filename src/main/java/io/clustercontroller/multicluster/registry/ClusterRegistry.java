package io.clustercontroller.multicluster.registry;

import io.clustercontroller.store.EtcdPathResolver;
import io.etcd.jetcd.*;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.options.GetOption;
import io.etcd.jetcd.options.WatchOption;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Manages cluster discovery and monitoring.
 */
@Component
@Slf4j
public class ClusterRegistry {
    
    private final KV kvClient;
    private final Watch watchClient;
    private final EtcdPathResolver pathResolver;
    
    @Autowired
    public ClusterRegistry(Client etcdClient, EtcdPathResolver pathResolver) {
        this.kvClient = etcdClient.getKVClient();
        this.watchClient = etcdClient.getWatchClient();
        this.pathResolver = pathResolver;
        
        log.info("ClusterRegistry initialized");
    }
    
    /**
     * List all registered clusters.
     */
    public Set<String> listClusters() {
        try {
            String prefix = pathResolver.getClustersPrefix();
            GetResponse response = kvClient.get(
                ByteSequence.from(prefix, UTF_8),
                GetOption.newBuilder()
                    .withPrefix(ByteSequence.from(prefix, UTF_8))
                    .build()
            ).get(5, TimeUnit.SECONDS);
            
            Set<String> clusters = new HashSet<>();
            for (KeyValue kv : response.getKvs()) {
                String path = kv.getKey().toString(UTF_8);
                // Extract cluster ID from path: /multi-cluster/clusters/{id}/metadata
                String[] parts = path.split("/");
                if (parts.length >= 4) {
                    clusters.add(parts[3]);
                }
            }
            
            return clusters;
            
        } catch (Exception e) {
            log.error("Error listing clusters", e);
            return Set.of();
        }
    }
    
    /**
     * Watch for cluster membership changes.
     */
    public Watch.Watcher watchClusters(Consumer<Set<String>> onClusterChange) {
        String prefix = pathResolver.getClustersPrefix();
        return watchClient.watch(
            ByteSequence.from(prefix, UTF_8),
            WatchOption.newBuilder()
                .withPrefix(ByteSequence.from(prefix, UTF_8))
                .build(),
            watchResponse -> {
                log.debug("Cluster membership changed");
                onClusterChange.accept(listClusters());
            }
        );
    }
}

