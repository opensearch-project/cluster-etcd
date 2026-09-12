package tools;

import org.junit.platform.engine.discovery.ClassNameFilter;
import org.junit.platform.engine.discovery.DiscoverySelectors;
import org.junit.platform.launcher.Launcher;
import org.junit.platform.launcher.LauncherDiscoveryRequest;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;
import org.junit.platform.launcher.core.LauncherFactory;
import org.junit.platform.launcher.listeners.SummaryGeneratingListener;
import org.junit.platform.launcher.listeners.TestExecutionSummary;

import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * A hand-rolled JUnit Platform launcher, because Gradle cannot run in this sandbox: it fails creating its
 * file-lock service, since binding a socket is denied. See {@code WORKING_NOTES.md}.
 *
 * <p>This is not a replacement for {@code ./gradlew test} and must not become one. It runs the same tests
 * against the same classpath, on the same JDK 21 toolchain, so that work can proceed here; the Gradle
 * build remains the authority.
 *
 * <p>Usage: {@code JUnitRunner <classpath-root> [<classpath-root> ...] [-- <class-name-pattern> ...]}
 */
public final class JUnitRunner {

    public static void main(String[] args) {
        Set<Path> roots = new LinkedHashSet<>();
        List<String> patterns = new ArrayList<>();

        boolean afterSeparator = false;
        for (String arg : args) {
            if (arg.equals("--")) {
                afterSeparator = true;
                continue;
            }
            if (afterSeparator) {
                patterns.add(arg);
            } else {
                Path root = Path.of(arg);
                if (Files.isDirectory(root)) {
                    roots.add(root);
                }
            }
        }

        if (roots.isEmpty()) {
            System.err.println("no classpath roots with compiled tests; nothing to run");
            System.exit(2);
        }

        LauncherDiscoveryRequestBuilder builder = LauncherDiscoveryRequestBuilder.request()
            .selectors(DiscoverySelectors.selectClasspathRoots(roots));
        if (patterns.isEmpty() == false) {
            builder.filters(ClassNameFilter.includeClassNamePatterns(patterns.toArray(new String[0])));
        }
        LauncherDiscoveryRequest request = builder.build();

        Launcher launcher = LauncherFactory.create();
        SummaryGeneratingListener listener = new SummaryGeneratingListener();
        launcher.execute(request, listener);

        TestExecutionSummary summary = listener.getSummary();
        PrintWriter out = new PrintWriter(System.out, true);
        summary.printTo(out);
        if (summary.getTotalFailureCount() > 0) {
            summary.printFailuresTo(out, 20);
        }

        // A run that discovered nothing is a failure. Silently passing zero tests is how a broken
        // classpath looks exactly like a green build, and this runner exists precisely because the real
        // build cannot be run here to contradict it.
        if (summary.getTestsFoundCount() == 0) {
            System.err.println("FAILED: no tests were discovered");
            System.exit(1);
        }
        System.exit(summary.getTotalFailureCount() > 0 ? 1 : 0);
    }

    private JUnitRunner() {}
}
