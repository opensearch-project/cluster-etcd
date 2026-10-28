package org.opensearch.supervisor.model.supervisor;

import org.opensearch.supervisor.model.Json;
import org.opensearch.supervisor.model.PoolId;

import java.util.List;
import java.util.Objects;

/**
 * {@code supervisor/members/<supervisor-id>} — who is alive, what they hold, and how much they can take.
 *
 * <p>Renewed every pass. This is the whole of the membership protocol: no gossip, no consensus, no
 * failure detector beyond records that stop being renewed. An expired member's capacity simply leaves the
 * fair-share denominator, and the housekeeping holder reaps the record after a long grace period.
 *
 * @param capacityWeight configured, not measured. It is how a supervisor on a bigger machine is given more
 *     pools without a measurement loop whose output would then have to be trusted.
 * @param weightHeld the sum of the weights of the pools this member holds, published so that every other
 *     member can compute fair share without owning anything it does not own.
 */
public record MemberRecord(
    String supervisorId,
    long startedAt,
    long expiresAt,
    long pass,
    double capacityWeight,
    List<PoolId> poolsHeld,
    double weightHeld
) {

    public MemberRecord {
        Objects.requireNonNull(supervisorId, "supervisorId");
        poolsHeld = poolsHeld == null ? List.of() : List.copyOf(poolsHeld);
        if (capacityWeight <= 0) {
            throw new IllegalArgumentException(
                "capacity weight must be positive, was " + capacityWeight
                    + "; a member with no capacity would take no pools and still divide the fair share"
            );
        }
    }

    public byte[] toBytes() {
        return Json.write(this, "member record");
    }

    public static MemberRecord from(byte[] bytes) {
        return Json.read(bytes, MemberRecord.class, "member record");
    }
}
