package org.opensearch.supervisor.app;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RunnerTests {

    private ByteArrayOutputStream captured;
    private PrintStream original;

    @BeforeEach
    void captureLogs() {
        captured = new ByteArrayOutputStream();
        original = System.out;
        Log.stream(new PrintStream(captured, true, StandardCharsets.UTF_8));
        Log.threshold(Log.Level.DEBUG);
    }

    @AfterEach
    void restoreLogs() {
        Log.stream(original);
        Log.threshold(Log.Level.INFO);
    }

    private String logs() {
        return captured.toString(StandardCharsets.UTF_8);
    }

    /** Adds the options every test needs, without clobbering one a test has set for itself. */
    private static Options options(String... extra) {
        java.util.List<String> args = new java.util.ArrayList<>(java.util.List.of(extra));
        if (args.stream().noneMatch(arg -> arg.startsWith("--store="))) {
            args.add("--store=file:///tmp/unused");
        }
        if (args.stream().noneMatch(arg -> arg.startsWith("--interval="))) {
            args.add("--interval=1ms");
        }
        if (args.stream().noneMatch(arg -> arg.startsWith("--shutdown-budget="))) {
            args.add("--shutdown-budget=200ms");
        }
        return Options.parse(args.toArray(String[]::new), Map.of());
    }

    /** A pass that does whatever the test tells it to. */
    private static final class FakeCycle implements Cycle {
        final AtomicInteger ticks = new AtomicInteger();
        final AtomicBoolean shutdownCalled = new AtomicBoolean();
        volatile int failUntilTick = 0;
        volatile boolean failForever;
        volatile Runnable onTick;

        @Override
        public void tick() {
            int n = ticks.incrementAndGet();
            if (onTick != null) {
                onTick.run();
            }
            if (failForever || n <= failUntilTick) {
                throw new IllegalStateException("store unavailable",
                    new java.net.ConnectException("connection refused"));
            }
        }

        @Override
        public void shutdown() {
            shutdownCalled.set(true);
        }

        @Override
        public String describe() {
            return "ticks=" + ticks.get();
        }
    }

    @Test
    void aFailingPassDoesNotStopTheLoop() {
        // Level-triggered reconciliation means a pass that threw left nothing to unwind, so exiting would turn a
        // transient store timeout into an outage of the management plane.
        FakeCycle cycle = new FakeCycle();
        cycle.failUntilTick = 2;

        int code = new Runner(cycle, options("--max-passes=5")).run();

        assertEquals(5, cycle.ticks.get(), "the loop kept going");
        assertEquals(0, code, "some passes succeeded, so this is not a failed process");
        assertTrue(logs().contains("the next pass will recompute from observed state"), logs());
    }

    @Test
    void theRootCauseIsInTheLineRatherThanTheOutermostWrapper() {
        // "reconciliation pass failed" is the least informative sentence available; "connection refused" is the
        // one somebody can act on, and it is three layers down.
        FakeCycle cycle = new FakeCycle();
        cycle.failUntilTick = 1;

        new Runner(cycle, options("--max-passes=1")).run();

        assertTrue(logs().contains("connection refused"), logs());
    }

    @Test
    void repeatedFailuresEscalateAndSayThatPeersWillTakeOver() {
        // The important half of the failure policy: nothing here has to detect a wedged process, because lease
        // renewal happens inside the pass. A supervisor whose passes all throw stops renewing, so its peers
        // claim its pools without anyone deciding to give up.
        FakeCycle cycle = new FakeCycle();
        cycle.failForever = true;

        int code = new Runner(cycle, options("--max-passes=4")).run();

        assertEquals(1, code, "every pass failed, which is what a supervising process should act on");
        assertTrue(logs().contains("leases will expire and peers will take over"), logs());
        assertTrue(logs().contains("level=error"), logs());
    }

    @Test
    void recoveryIsAnnouncedSoTheAbsenceOfErrorsIsNotTheOnlySignal() {
        FakeCycle cycle = new FakeCycle();
        cycle.failUntilTick = 3;

        new Runner(cycle, options("--max-passes=5")).run();

        assertTrue(logs().contains("passes are succeeding again"), logs());
        assertTrue(logs().contains("after_failures=3"), logs());
    }

    @Test
    void aFewFailuresStayAtWarnRatherThanCryingWolf() {
        FakeCycle cycle = new FakeCycle();
        cycle.failUntilTick = 1;

        new Runner(cycle, options("--max-passes=2")).run();

        assertTrue(logs().contains("level=warn"), logs());
        assertFalse(logs().contains("level=error"), logs());
    }

    @Test
    void everyPassSucceedingIsExitCodeZeroAndLeasesAreReleased() {
        FakeCycle cycle = new FakeCycle();

        int code = new Runner(cycle, options("--max-passes=3")).run();

        assertEquals(0, code);
        assertEquals(3, cycle.ticks.get());
        assertTrue(cycle.shutdownCalled.get(), "a clean stop must give up its leases");
        assertTrue(logs().contains("released every lease"), logs());
    }

    @Test
    void anOverrunningPassIsReportedAndTheNextOneStartsImmediately() {
        // Fixed delay rather than fixed rate. Queuing passes behind a slow store is how a struggling system
        // becomes a dead one.
        FakeCycle cycle = new FakeCycle();
        cycle.onTick = () -> {
            try {
                Thread.sleep(30);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        };

        new Runner(cycle, options("--interval=5ms", "--max-passes=2")).run();

        assertTrue(logs().contains("pass took longer than the interval"), logs());
    }

    @Test
    void stoppingDuringTheIntervalDoesNotWaitTheIntervalOut() {
        // The bug this pins: the sleep recomputed its deadline after being woken, so it parked again and a stop
        // still cost a full interval. With a long interval that is a shutdown that looks hung.
        FakeCycle cycle = new FakeCycle();
        Runner runner = new Runner(cycle, options("--interval=1h"));
        cycle.onTick = () -> new Thread(runner::stop).start();

        long startedAt = System.nanoTime();
        int code = runner.run();
        long tookMillis = (System.nanoTime() - startedAt) / 1_000_000;

        assertEquals(0, code);
        assertEquals(1, cycle.ticks.get(), "it finished the pass it was in and stopped");
        assertTrue(tookMillis < 30_000, "took " + tookMillis + "ms, which means the stop was not noticed");
    }

    @Test
    void aShutdownThatWillNotCompleteDoesNotKeepTheProcessAlive() {
        // A store that has stopped answering must not turn a stop into a hang. The leases expire on their own,
        // which is exactly the case the TTL exists for.
        Cycle wedged = new Cycle() {
            @Override
            public void tick() {}

            @Override
            public void shutdown() {
                try {
                    Thread.sleep(Long.MAX_VALUE);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }

            @Override
            public String describe() {
                return "wedged";
            }
        };

        long startedAt = System.nanoTime();
        int code = new Runner(wedged, options("--max-passes=1", "--shutdown-budget=200ms")).run();
        long tookMillis = (System.nanoTime() - startedAt) / 1_000_000;

        assertEquals(0, code);
        // The log line is the real assertion, because it is deterministic. The time bound is deliberately loose:
        // it is here to catch "waits for ever", and a tight bound on a shared machine would flake without ever
        // catching anything a loose one misses.
        assertTrue(logs().contains("gave up waiting for a clean shutdown"), logs());
        assertTrue(tookMillis < 30_000, "took " + tookMillis + "ms, so the budget was not honoured at all");
    }

    @Test
    void anErrorRatherThanAnExceptionIsStillReportedByAProcessStillAbleToReport() {
        // Throwable rather than Exception in the catch. A NoClassDefFoundError from a store jar is precisely the
        // failure that should produce a line explaining itself, and letting it escape kills the loop first.
        Cycle broken = new Cycle() {
            @Override
            public void tick() {
                throw new NoClassDefFoundError("io/etcd/jetcd/Client");
            }

            @Override
            public void shutdown() {}

            @Override
            public String describe() {
                return "broken";
            }
        };

        int code = new Runner(broken, options("--max-passes=1")).run();

        assertEquals(1, code);
        assertTrue(logs().contains("jetcd"), logs());
    }
}
