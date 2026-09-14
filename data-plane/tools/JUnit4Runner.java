package tools;

import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * A hand-rolled JUnit 4 launcher, because Gradle cannot run in this sandbox: it fails creating its
 * file-lock service, since binding a socket is denied. See {@code WORKING_NOTES.md}.
 *
 * <p><b>JUnit 4, not the JUnit Platform</b>, and that is the point rather than an accident. This plugin's
 * tests extend {@code OpenSearchTestCase}, which extends Lucene's {@code LuceneTestCase} and runs under
 * {@code @RunWith(RandomizedRunner.class)} — and the OpenSearch plugin build runs them with JUnit 4 too.
 * The sibling supervisor repository is a plain JVM project on JUnit 5 and keeps its own platform launcher.
 * Running the same framework the real build runs is the whole value of this script: a launcher that
 * discovered tests Gradle would not, or vice versa, would report a green build for a suite nobody executes.
 *
 * <p>{@code JUnitCore} honours {@code @RunWith}, so {@code RandomizedRunner} drives the tests here exactly
 * as it does under Gradle, including the per-method seeds and the thread-leak checks.
 *
 * <p>Usage: {@code JUnit4Runner <classpath-root> [<classpath-root> ...] [-- <class-name-pattern> ...]}
 */
public final class JUnit4Runner {

    /** Only this repository's classes are inspected; see {@link #carriesATest}. */
    private static final String PACKAGE = "org.opensearch.clusterless";

    public static void main(String[] args) throws Exception {
        Set<Path> roots = new LinkedHashSet<>();
        List<Pattern> patterns = new ArrayList<>();

        boolean afterSeparator = false;
        for (String arg : args) {
            if (arg.equals("--")) {
                afterSeparator = true;
                continue;
            }
            if (afterSeparator) {
                patterns.add(Pattern.compile(arg));
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

        List<Class<?>> testClasses = new ArrayList<>();
        for (Path root : roots) {
            testClasses.addAll(discover(root, patterns));
        }
        testClasses.sort((left, right) -> left.getName().compareTo(right.getName()));

        // Fails rather than passes, deliberately. Silently running nothing looks exactly like a green build,
        // and there is no working Gradle here to contradict it -- which is precisely the failure this whole
        // exercise turned up in the real build.
        if (testClasses.isEmpty()) {
            System.err.println("discovered zero test classes under " + roots + "; refusing to report success");
            System.exit(2);
        }

        System.out.println("running " + testClasses.size() + " test class(es)");
        Result result = new JUnitCore().run(testClasses.toArray(new Class<?>[0]));

        System.out.println();
        System.out.println("Test run finished after " + result.getRunTime() + " ms");
        System.out.println("[ " + result.getRunCount() + " tests run ]");
        System.out.println("[ " + result.getIgnoreCount() + " tests ignored ]");
        System.out.println("[ " + result.getFailureCount() + " tests failed ]");

        if (result.wasSuccessful() == false) {
            System.out.println();
            System.out.println("Failures (" + result.getFailureCount() + "):");
            for (Failure failure : result.getFailures()) {
                System.out.println("  " + failure.getTestHeader());
                System.out.println("    => " + failure.getMessage());
                String trace = failure.getTrace();
                if (trace != null) {
                    // The first few frames are the useful ones; RandomizedRunner's own frames are not.
                    Stream.of(trace.split("\n")).limit(12).forEach(line -> System.out.println("       " + line));
                }
            }
            System.exit(1);
        }
    }

    /**
     * Every concrete class under a root that carries a test.
     *
     * <p>Two ways to be one, because OpenSearch uses both. An annotated {@code @Test} method is the ordinary
     * JUnit 4 form; a {@code public void testSomething()} method with no arguments is the JUnit 3 form, which
     * {@code LuceneTestCase} still supports through {@code @TestMethodProviders} and which is the convention
     * across the OpenSearch codebase. Looking only for the annotation would silently skip every test in this
     * repository.
     */
    private static List<Class<?>> discover(Path root, List<Pattern> patterns) throws Exception {
        List<Class<?>> found = new ArrayList<>();
        try (Stream<Path> files = Files.walk(root)) {
            for (Path file : files.filter(path -> path.toString().endsWith(".class")).toList()) {
                String className = root.relativize(file)
                    .toString()
                    .replace(".class", "")
                    .replace(java.io.File.separatorChar, '.');
                if (className.contains("$")) {
                    continue;
                }
                if (patterns.isEmpty() == false && patterns.stream().noneMatch(pattern -> pattern.matcher(className).matches())) {
                    continue;
                }
                Class<?> candidate = Class.forName(className, false, JUnit4Runner.class.getClassLoader());
                if (Modifier.isAbstract(candidate.getModifiers())) {
                    continue;
                }
                if (carriesATest(candidate)) {
                    found.add(candidate);
                }
            }
        }
        return found;
    }

    /**
     * Walks this repository's own classes only, and deliberately uses {@code getDeclaredMethods} rather than
     * {@code getMethods}.
     *
     * <p>{@code getMethods} resolves the signature types of every inherited public method, and
     * {@code OpenSearchTestCase} inherits a lot: asking for them pulled in log4j-core, joda-time and more,
     * failing discovery with a {@code NoClassDefFoundError} before a single test ran. Declared methods on the
     * classes in this repository need nothing but this repository.
     */
    private static boolean carriesATest(Class<?> candidate) {
        for (Class<?> clazz = candidate; clazz != null && clazz.getName().startsWith(PACKAGE); clazz = clazz.getSuperclass()) {
            for (Method method : clazz.getDeclaredMethods()) {
                if (isATest(method)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean isATest(Method method) {
        if (method.isAnnotationPresent(org.junit.Test.class)) {
            return true;
        }
        return method.getName().startsWith("test")
            && method.getParameterCount() == 0
            && method.getReturnType() == void.class
            && Modifier.isPublic(method.getModifiers())
            && Modifier.isStatic(method.getModifiers()) == false;
    }
}
