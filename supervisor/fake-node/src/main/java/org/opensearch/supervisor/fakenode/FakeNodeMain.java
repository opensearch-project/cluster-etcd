package org.opensearch.supervisor.fakenode;

/**
 * Entry point for the data plane test double.
 *
 * <p>Milestone 2 fills this in: register, heartbeat on an interval, maintain a shard inventory, and obey
 * the assignment document. Implementing it from {@code NODE_CONTRACT.md} alone is the first real test of
 * whether that contract is implementable, so it should be written against the document rather than against
 * whatever the supervisor happens to do.
 */
public final class FakeNodeMain {

    private FakeNodeMain() {}

    public static void main(String[] args) {
        System.out.println("fake-node — not implemented yet; see NODE_CONTRACT.md and DESIGN.md §11.");
    }
}
