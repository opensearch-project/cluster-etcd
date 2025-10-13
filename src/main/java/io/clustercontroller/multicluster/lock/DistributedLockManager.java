package io.clustercontroller.multicluster.lock;

import io.clustercontroller.store.EtcdPathResolver;
import io.etcd.jetcd.*;
import io.etcd.jetcd.lock.LockResponse;
import io.etcd.jetcd.support.CloseableClient;
import io.etcd.jetcd.support.Observers;
import io.etcd.jetcd.watch.WatchEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Manages distributed locks for cluster coordination using etcd.
 * Handles lock acquisition, release, and lease management.
 */
@Component
@Slf4j
public class DistributedLockManager {
    
    private static final int ETCD_OPERATION_TIMEOUT_SECONDS = 5;
    
    private final Lock lockClient;
    private final Lease leaseClient;
    private final Watch watchClient;
    private final EtcdPathResolver pathResolver;
    
    @Autowired
    public DistributedLockManager(Client etcdClient, EtcdPathResolver pathResolver) {
        this.lockClient = etcdClient.getLockClient();
        this.leaseClient = etcdClient.getLeaseClient();
        this.watchClient = etcdClient.getWatchClient();
        this.pathResolver = pathResolver;
        
        log.info("DistributedLockManager initialized");
    }
    
    /**
     * Acquire exclusive lock on a cluster.
     * 
     * @param clusterId The cluster to lock
     * @param ttlSeconds Time-to-live for the lease in seconds
     * @return ClusterLock with lease and keep-alive observer
     * @throws LockException if acquisition fails or times out
     */
    public ClusterLock acquireLock(String clusterId, int ttlSeconds) throws LockException {
        try {
            log.debug("Attempting to acquire lock for cluster: {}", clusterId);
            
            // Step 1: Create lease
            long leaseId = leaseClient.grant(ttlSeconds)
                .get(ETCD_OPERATION_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .getID();
            
            log.debug("Created lease {} for cluster {}", leaseId, clusterId);
            
            // Step 2: Start keep-alive BEFORE acquiring lock (critical for etcd Lock API!)
            CloseableClient keepAlive = leaseClient.keepAlive(
                leaseId,
                Observers.observer(response -> {
                    // Keep-alive response received
                })
            );
            
            log.debug("Started keep-alive for lease {}", leaseId);
            
            // Step 3: Acquire exclusive lock
            String lockPath = pathResolver.getClusterLockPath(clusterId);
            LockResponse lockResponse = lockClient.lock(
                ByteSequence.from(lockPath, UTF_8),
                leaseId
            ).get(ETCD_OPERATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            
            ByteSequence lockKey = lockResponse.getKey();
            
            log.info("✓ Lock acquired for cluster: {} (leaseId: {}, lockKey: {})", 
                clusterId, leaseId, lockKey.toString(UTF_8));
            
            return new ClusterLock(clusterId, leaseId, lockKey, keepAlive);
            
        } catch (TimeoutException e) {
            log.debug("Timeout acquiring lock for cluster: {}", clusterId);
            throw new LockException("Timeout acquiring lock for cluster: " + clusterId, e);
        } catch (Exception e) {
            log.warn("Failed to acquire lock for cluster: {}", clusterId, e);
            throw new LockException("Failed to acquire lock for cluster: " + clusterId, e);
        }
    }
    
    /**
     * Release lock and revoke lease.
     * 
     * @param lock The lock to release
     */
    public void releaseLock(ClusterLock lock) {
        if (lock == null) {
            return;
        }
        
        try {
            log.debug("Releasing lock for cluster: {}", lock.getClusterId());
            
            // Step 1: Close keep-alive observer
            if (lock.getKeepAliveObserver() != null) {
                lock.getKeepAliveObserver().close();
            }
            
            // Step 2: Revoke lease (fast unlock)
            leaseClient.revoke(lock.getLeaseId())
                .get(ETCD_OPERATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            
            log.info("✓ Lock released for cluster: {}", lock.getClusterId());
            
        } catch (Exception e) {
            log.error("Error releasing lock for cluster: {}", lock.getClusterId(), e);
        }
    }
    
    /**
     * Watch a lock key for changes (deletion/modification indicates lock lost).
     * 
     * Primary case: DELETE - lease expired, lock automatically released by etcd
     * Defensive case: PUT - manual intervention or unexpected key modification
     * 
     * @param lock The lock to watch
     * @param onLockLost Callback to invoke when lock is lost
     * @return Watcher that can be closed to stop watching
     */
    public Watch.Watcher watchLock(ClusterLock lock, Runnable onLockLost) {
        return watchClient.watch(lock.getLockKey(), watchResponse -> {
            for (WatchEvent event : watchResponse.getEvents()) {
                // DELETE is the expected event when lease expires
                // PUT is a safety net for unexpected modifications
                if (event.getEventType() == WatchEvent.EventType.DELETE ||
                    event.getEventType() == WatchEvent.EventType.PUT) {
                    log.warn("Lock lost for cluster: {} (event: {})", 
                        lock.getClusterId(), event.getEventType());
                    onLockLost.run();
                    break;
                }
            }
        });
    }
}
