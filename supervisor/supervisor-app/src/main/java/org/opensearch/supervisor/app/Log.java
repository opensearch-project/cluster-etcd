package org.opensearch.supervisor.app;

import java.io.PrintStream;
import java.time.Instant;
import java.util.Locale;

/**
 * Logfmt to one stream, and no logging dependency.
 *
 * <p>Three decisions, each small and each with a reason.
 *
 * <p><b>No library.</b> Not asceticism — the supervisor's real observability surface is the projections it
 * writes into the store, which is what {@code R7} asks for and what an operator queries. Logs here are for the
 * one audience projections cannot serve: somebody watching a process that will not start, or one whose passes
 * are failing before they can project anything. That audience needs a timestamp, a level and a line, and adding
 * a dependency to provide them would mean this module could not be built in an environment where the dependency
 * is unavailable — which is exactly the environment this was written in.
 *
 * <p><b>Logfmt, not JSON.</b> The phase this exists for is somebody at a terminal correlating supervisor output
 * against a data node's. Logfmt reads at a glance and still parses. If a deployment wants JSON it is one method.
 *
 * <p><b>One stream.</b> Errors go to stdout with everything else rather than stderr, because two streams lose
 * the interleaving, and the ordering of "pass 41 succeeded / pass 42 threw / lease lost" is most of the
 * information. A supervisor that fails to start exits non-zero; that is what a supervising process should watch,
 * not which file descriptor a line arrived on.
 */
public final class Log {

    public enum Level {
        ERROR, WARN, INFO, DEBUG;

        static Level parse(String raw) {
            try {
                return valueOf(raw.trim().toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(
                    "log level [" + raw + "] is not one of error, warn, info, debug");
            }
        }
    }

    private static volatile Level threshold = Level.INFO;
    private static volatile PrintStream out = System.out;

    private final String name;

    private Log(String name) {
        this.name = name;
    }

    public static Log of(String name) {
        return new Log(name);
    }

    public static void threshold(Level level) {
        threshold = level;
    }

    /** For tests, which need to read what was written. */
    public static void stream(PrintStream stream) {
        out = stream;
    }

    public boolean isEnabled(Level level) {
        return level.ordinal() <= threshold.ordinal();
    }

    /**
     * @param pairs alternating key and value. An odd count is a programming error and is reported in the line
     *     rather than thrown: losing a log line to an exception, in the code path that exists to explain
     *     failures, would be a poor trade.
     */
    public void error(String message, Object... pairs) {
        write(Level.ERROR, message, null, pairs);
    }

    public void error(String message, Throwable thrown, Object... pairs) {
        write(Level.ERROR, message, thrown, pairs);
    }

    public void warn(String message, Object... pairs) {
        write(Level.WARN, message, null, pairs);
    }

    public void warn(String message, Throwable thrown, Object... pairs) {
        write(Level.WARN, message, thrown, pairs);
    }

    public void info(String message, Object... pairs) {
        write(Level.INFO, message, null, pairs);
    }

    public void debug(String message, Object... pairs) {
        write(Level.DEBUG, message, null, pairs);
    }

    private void write(Level level, String message, Throwable thrown, Object... pairs) {
        if (isEnabled(level) == false) {
            return;
        }
        StringBuilder line = new StringBuilder(128);
        line.append("ts=").append(Instant.now())
            .append(" level=").append(level.name().toLowerCase(Locale.ROOT))
            .append(" logger=").append(name)
            .append(" msg=").append(quote(message));

        if (pairs.length % 2 != 0) {
            line.append(" log_error=").append(quote("odd number of key/value pairs: " + pairs.length));
        }
        for (int i = 0; i + 1 < pairs.length; i += 2) {
            line.append(' ').append(pairs[i]).append('=').append(quote(String.valueOf(pairs[i + 1])));
        }
        if (thrown != null) {
            line.append(" exception=").append(quote(thrown.getClass().getName()))
                .append(" cause=").append(quote(rootCauseOf(thrown)));
        }

        PrintStream stream = out;
        // One synchronized write, so two threads cannot interleave halves of a line. The loop is single
        // threaded, but the shutdown hook is not, and a torn line during shutdown is exactly when it would
        // matter most.
        synchronized (Log.class) {
            stream.println(line);
            if (thrown != null && isEnabled(Level.DEBUG)) {
                thrown.printStackTrace(stream);
            }
            stream.flush();
        }
    }

    /**
     * The deepest message in the chain.
     *
     * <p>Store failures arrive wrapped several layers deep and the outermost message is usually the least
     * informative of them — "reconciliation pass failed" over "connection refused" — so the line carries the
     * bottom of the chain and a stack trace only appears at debug.
     */
    private static String rootCauseOf(Throwable thrown) {
        Throwable cause = thrown;
        StringBuilder chain = new StringBuilder();
        while (cause != null) {
            if (cause.getMessage() != null) {
                if (chain.length() > 0) {
                    chain.append(": ");
                }
                chain.append(cause.getMessage());
            }
            cause = cause.getCause() == cause ? null : cause.getCause();
        }
        return chain.length() == 0 ? thrown.toString() : chain.toString();
    }

    private static String quote(String value) {
        if (value == null) {
            return "\"\"";
        }
        boolean needsQuotes = value.isEmpty();
        for (int i = 0; i < value.length() && needsQuotes == false; i++) {
            char c = value.charAt(i);
            needsQuotes = c == ' ' || c == '"' || c == '=' || c == '\n' || c == '\r' || c == '\\';
        }
        if (needsQuotes == false) {
            return value;
        }
        StringBuilder quoted = new StringBuilder(value.length() + 8).append('"');
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '"' -> quoted.append("\\\"");
                case '\\' -> quoted.append("\\\\");
                case '\n' -> quoted.append("\\n");
                case '\r' -> quoted.append("\\r");
                default -> quoted.append(c);
            }
        }
        return quoted.append('"').toString();
    }
}
