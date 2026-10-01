/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.publish;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.clusterless.contract.Heartbeat;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.monitor.fs.FsInfo;
import org.opensearch.monitor.fs.FsProbe;
import org.opensearch.monitor.jvm.JvmStats;
import org.opensearch.monitor.os.OsProbe;
import org.opensearch.monitor.os.OsStats;

import java.util.Objects;

/**
 * Capacity read from the node itself.
 *
 * <p>Absolute bytes for disk, not a percentage: a decider comparing two nodes needs the denominator, and
 * merging "how much work" with "how much capacity" early throws one of them away. Everything here is what
 * allocation needs and nothing more — doc counts and per-shard statistics are an observability concern and
 * would put an unbounded, fast-changing field into a document with a 4 KiB budget.
 *
 * <p>Every probe is individually guarded. A heartbeat is what liveness is computed from, so a filesystem
 * call that blocks or throws must cost a figure, never the heartbeat that carries it.
 */
public final class LocalCapacityProbe implements CapacityProbe {

    private static final Logger logger = LogManager.getLogger(LocalCapacityProbe.class);

    private final NodeEnvironment nodeEnvironment;

    public LocalCapacityProbe(NodeEnvironment nodeEnvironment) {
        this.nodeEnvironment = Objects.requireNonNull(nodeEnvironment, "nodeEnvironment");
    }

    @Override
    public Heartbeat.Capacity capacity() {
        long diskTotal = 0;
        long diskAvailable = 0;
        try {
            FsInfo fsInfo = new FsProbe(nodeEnvironment, null).stats(null);
            for (FsInfo.Path path : fsInfo) {
                diskTotal += path.getTotal().getBytes();
                diskAvailable += path.getAvailable().getBytes();
            }
        } catch (Exception e) {
            logger.debug("clusterless: could not read filesystem statistics", e);
        }

        int heapUsedPercent = 0;
        try {
            heapUsedPercent = JvmStats.jvmStats().getMem().getHeapUsedPercent();
        } catch (Exception e) {
            logger.debug("clusterless: could not read JVM statistics", e);
        }

        int cpuUsedPercent = 0;
        double load1m = 0;
        try {
            OsStats osStats = OsProbe.getInstance().osStats();
            if (osStats.getCpu() != null) {
                cpuUsedPercent = osStats.getCpu().getPercent();
                double[] loadAverage = osStats.getCpu().getLoadAverage();
                // Absent on some platforms, and -1 where the probe could not read it. Either way the
                // honest answer is zero rather than a negative load, which nothing downstream expects.
                if (loadAverage != null && loadAverage.length > 0 && loadAverage[0] >= 0) {
                    load1m = loadAverage[0];
                }
            }
        } catch (Exception e) {
            logger.debug("clusterless: could not read OS statistics", e);
        }

        // A negative CPU percentage is OpenSearch's own "not measured yet", and it reaches an allocation
        // decider as a node that is less than idle.
        return new Heartbeat.Capacity(diskTotal, diskAvailable, Math.max(heapUsedPercent, 0), Math.max(cpuUsedPercent, 0), load1m);
    }
}
