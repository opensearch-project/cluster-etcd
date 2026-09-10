package org.opensearch.supervisor.model.task;

import org.opensearch.supervisor.model.Json;

import java.util.Comparator;
import java.util.Objects;

/**
 * {@code tenant/<t>/tasks/index-management/<uuid>} — the work queue, and the one subtree two services write.
 *
 * <p>The control plane creates a record per index write and never revisits it: nothing sets {@code state} to
 * anything but {@code pending} and nothing deletes one, so {@code cluster.pending_tasks} reports a pile that
 * grows without bound. {@code KEY_LAYOUT.md} calls completing and reaping these the supervisor's half of the
 * handoff, which is why R5.5's "own prefixes only" gives way here — see {@code SUPERVISOR_KEYS.md} §2.
 *
 * <p><b>A task is a hint, and nothing here is required for correctness.</b> With this subtree empty the
 * supervisor still converges, from the index documents alone — R2.1, and the reason a lost, duplicated or
 * out-of-order task cannot break anything. What this queue buys is that the pile drains and that repeated
 * failures become visible.
 *
 * @param sequence per control plane <em>process</em>, restarting at zero, so it orders tasks within one
 *     instance and means nothing across instances
 * @param submittedAt millisecond resolution, with ties constant because one wildcard request submits many
 *     tasks in the same millisecond
 * @param attempts how many times a supervisor has taken this up. R4.2: past a limit it becomes visible
 *     rather than looping silently.
 */
public record TaskRecord(
    String taskId,
    TaskType type,
    String index,
    String indexUuid,
    TaskState state,
    long submittedAt,
    long sequence,
    String holder,
    long epoch,
    long leaseExpiresAt,
    int attempts,
    String lastError
) {

    public static final int MAX_LAST_ERROR_LENGTH = 512;

    public TaskRecord {
        Objects.requireNonNull(taskId, "taskId");
        type = type == null ? TaskType.UNKNOWN : type;
        state = state == null ? TaskState.PENDING : state;
        if (lastError != null && lastError.length() > MAX_LAST_ERROR_LENGTH) {
            lastError = lastError.substring(0, MAX_LAST_ERROR_LENGTH);
        }
    }

    /**
     * Neither {@code sequence} nor {@code submittedAt} is a reliable global ordering — R4.5 — so this is
     * explicitly <b>arbitrary but stable</b>. That is all fairness needs; nothing here needs causality,
     * because a task carries no information the index documents do not.
     */
    public static final Comparator<TaskRecord> STABLE_ORDER = Comparator
        .comparingLong(TaskRecord::submittedAt)
        .thenComparing(TaskRecord::taskId);

    public boolean isClaimExpired(long nowMillis) {
        return leaseExpiresAt <= nowMillis;
    }

    public boolean isHeldBy(String supervisorId) {
        return state == TaskState.CLAIMED && supervisorId.equals(holder);
    }

    public TaskRecord claimedBy(String supervisorId, long epoch, long nowMillis, long leaseMillis) {
        return new TaskRecord(
            taskId, type, index, indexUuid, TaskState.CLAIMED, submittedAt, sequence,
            supervisorId, epoch, nowMillis + leaseMillis, attempts + 1, lastError
        );
    }

    public TaskRecord completed(long nowMillis) {
        return new TaskRecord(
            taskId, type, index, indexUuid, TaskState.COMPLETED, submittedAt, sequence,
            holder, epoch, nowMillis, attempts, null
        );
    }

    public TaskRecord failed(String reason, long nowMillis) {
        return new TaskRecord(
            taskId, type, index, indexUuid, TaskState.FAILED, submittedAt, sequence,
            holder, epoch, nowMillis, attempts, reason
        );
    }

    /** When a terminal record became terminal, which is what the retention window is measured from. */
    public long terminalAt() {
        return leaseExpiresAt;
    }

    public byte[] toBytes() {
        return Json.write(this, "task record");
    }

    public static TaskRecord from(byte[] bytes) {
        return Json.read(bytes, TaskRecord.class, "task record");
    }
}
