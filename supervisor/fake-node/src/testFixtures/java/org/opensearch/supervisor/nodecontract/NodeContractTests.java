package org.opensearch.supervisor.nodecontract;

import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Runs {@link NodeContractChecks} under JUnit 5, one reported test per check.
 *
 * <p>Extend this from a JUnit 5 project — {@code fake-node} does. The data plane's build runs JUnit 4
 * through {@code randomizedtesting}, so it calls {@link NodeContractChecks#runAll} instead; the checks
 * themselves are shared and carry no annotations, which is what lets one list serve two runners.
 *
 * <p>Previously this class held the checks directly, as {@code @Test} methods on an abstract base. That
 * cannot work across the two: annotations one side's runner does not recognise produce no error, just zero
 * tests discovered and a green build. Keeping the checks as data and the runners thin is what makes
 * "add a check once, both implementations run it" true rather than intended.
 */
public abstract class NodeContractTests {

    /**
     * The node under test, writing into the store the harness supplies.
     *
     * <p>The store is supplied rather than obtained because the harness wraps it to observe write order. An
     * adapter that has to plant something a supervisor would have written should do so through
     * {@link RecordingStore#asSupervisor()}, so the harness's own writes are not attributed to the node.
     */
    protected abstract ContractNode newNode(RecordingStore store);

    @TestFactory
    List<DynamicTest> nodeContract() {
        List<DynamicTest> tests = new ArrayList<>();
        for (NodeContractChecks.Check check : NodeContractChecks.all()) {
            // A fresh harness per check, built inside the test body rather than outside it: several checks
            // assert that something was *not* rewritten, which means nothing if an earlier check already
            // wrote it.
            tests.add(DynamicTest.dynamicTest(check.name(), () -> check.body().accept(NodeContractChecks.harness(this::newNode))));
        }
        return tests;
    }
}
