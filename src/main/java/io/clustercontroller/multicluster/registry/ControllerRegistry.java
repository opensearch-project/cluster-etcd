package io.clustercontroller.multicluster.registry;

import io.clustercontroller.store.EtcdPathResolver;
import io.etcd.jetcd.*;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.options.GetOption;
import io.etcd.jetcd.options.PutOption;
import io.etcd.jetcd.options.WatchOption;
import io.etcd.jetcd.support.CloseableClient;
import io.etcd.jetcd.support.Observers;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Manages controller registration, heartbeat, and discovery.
 */
@Component
@Slf4j
public class ControllerRegistry {
    
    private static final int ETCD_OPERATION_TIMEOUT_SECONDS = 5;
    private static final String HEARTBEAT_SUFFIX = "/heartbeat";
    
    private final KV kvClient;
    private final Lease leaseClient;
    private final Watch watchClient;
    private final EtcdPathResolver pathResolver;
    
    @Autowired
    public ControllerRegistry(Client etcdClient, EtcdPathResolver pathResolver) {
        this.kvClient = etcdClient.getKVClient();
        this.leaseClient = etcdClient.getLeaseClient();
        this.watchClient = etcdClient.getWatchClient();
        this.pathResolver = pathResolver;
        
        log.info("ControllerRegistry initialized");
    }
    
    /**
     * Register this controller with heartbeat.
     */
    public ControllerRegistration register(String controllerId, int ttlSeconds) {
        try {
            log.info("Registering controller: {}", controllerId);
            
            // Create lease with keep-alive
            long leaseId = leaseClient.grant(ttlSeconds)
                .get(ETCD_OPERATION_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .getID();
            
            CloseableClient keepAlive = leaseClient.keepAlive(
                leaseId,
                Observers.observer(response -> {
                    // Keep-alive response received
                })
            );
            
            // Put heartbeat key with timestamp for debugging/monitoring
            // TODO: If etcd payload size is a concern, use constant "1" instead of timestamp
            String heartbeatPath = pathResolver.getControllerHeartbeatPath(controllerId);
            String heartbeatValue = String.valueOf(System.currentTimeMillis());
            kvClient.put(
                ByteSequence.from(heartbeatPath, UTF_8),
                ByteSequence.from(heartbeatValue, UTF_8),
                PutOption.newBuilder().withLeaseId(leaseId).build()
            ).get(ETCD_OPERATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            
            log.info("✓ Controller registered: {} (leaseId: {})", controllerId, leaseId);
            
            return new ControllerRegistration(controllerId, leaseId, keepAlive);
            
        } catch (Exception e) {
            log.error("Failed to register controller: {}", controllerId, e);
            throw new RuntimeException("Failed to register controller: " + controllerId, e);
        }
    }
    
    /**
     * Deregister controller and revoke lease.
     */
    public void deregister(ControllerRegistration registration) {
        try {
            log.info("Deregistering controller: {}", registration.getControllerId());
            
            if (registration.getKeepAliveObserver() != null) {
                registration.getKeepAliveObserver().close();
            }
            
            leaseClient.revoke(registration.getLeaseId())
                .get(ETCD_OPERATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            
            log.info("✓ Controller deregistered: {}", registration.getControllerId());
            
        } catch (Exception e) {
            log.error("Error deregistering controller: {}", registration.getControllerId(), e);
        }
    }
    
    /**
     * List all active controllers.
     */
    public Set<String> listActiveControllers() {
        try {
            String prefix = pathResolver.getControllersPrefix();
            GetResponse response = kvClient.get(
                ByteSequence.from(prefix, UTF_8),
                GetOption.newBuilder()
                    .withPrefix(ByteSequence.from(prefix, UTF_8))
                    .build()
            ).get(ETCD_OPERATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            
            Set<String> controllers = new HashSet<>();
            for (KeyValue kv : response.getKvs()) {
                String path = kv.getKey().toString(UTF_8);
                // Extract controller ID from path: /multi-cluster/controllers/{id}/heartbeat
                // Only include paths that end with /heartbeat
                if (path.endsWith(HEARTBEAT_SUFFIX)) {
                    String[] parts = path.split("/");
                    if (parts.length >= 4) {
                        controllers.add(parts[3]);
                    }
                }
            }
            
            return controllers;
            
        } catch (Exception e) {
            log.error("Error listing controllers", e);
            return Set.of();
        }
    }
    
    /**
     * Watch for controller membership changes.
     * Note: Callback should NOT do blocking operations - offload to another thread!
     */
    public Watch.Watcher watchControllers(Runnable onMembershipChange) {
        String prefix = pathResolver.getControllersPrefix();
        return watchClient.watch(
            ByteSequence.from(prefix, UTF_8),
            WatchOption.newBuilder()
                .withPrefix(ByteSequence.from(prefix, UTF_8))
                .build(),
            watchResponse -> {
                log.info("Controller membership changed");
                onMembershipChange.run();  // Just notify, don't do blocking work here
            }
        );
    }
}

