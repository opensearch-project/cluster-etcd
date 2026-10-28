/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.rest;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Locale;

/**
 * OpenSearch's time values, as {@code TimeValue.parseTimeValue} reads them, branch for branch and with its
 * messages: a number followed by {@code nanos}, {@code micros}, {@code ms}, {@code s}, {@code m}, {@code h} or
 * {@code d}, or a bare {@code 0} or {@code -1}. {@code -1}, with or without a unit, means "no limit".
 */
public final class TimeValues {

    private TimeValues() {}

    /**
     * @param setting named in the error, as upstream names the parameter
     * @return the duration, or {@code null} for {@code -1}
     * @throws IllegalArgumentException with upstream's message, for anything it would reject
     */
    public static Duration parse(String value, String setting) {
        String normalized = value.toLowerCase(Locale.ROOT).trim();
        if (normalized.endsWith("nanos")) {
            return of(value, normalized, "nanos", ChronoUnit.NANOS, setting);
        } else if (normalized.endsWith("micros")) {
            return of(value, normalized, "micros", ChronoUnit.MICROS, setting);
        } else if (normalized.endsWith("ms")) {
            return of(value, normalized, "ms", ChronoUnit.MILLIS, setting);
        } else if (normalized.endsWith("s")) {
            return of(value, normalized, "s", ChronoUnit.SECONDS, setting);
        } else if (value.endsWith("m")) {
            // Case-sensitive upstream, the one unit that is: "M" would mean months.
            return of(value, normalized, "m", ChronoUnit.MINUTES, setting);
        } else if (normalized.endsWith("h")) {
            return of(value, normalized, "h", ChronoUnit.HOURS, setting);
        } else if (normalized.endsWith("d")) {
            return of(value, normalized, "d", ChronoUnit.DAYS, setting);
        } else if (normalized.matches("-0*1")) {
            return null;
        } else if (normalized.matches("0+")) {
            return Duration.ZERO;
        }
        throw new IllegalArgumentException("failed to parse setting [" + setting + "] with value [" + value
            + "] as a time value: unit is missing or unrecognized");
    }

    /**
     * {@code TimeValue.toString()}: the largest unit the value reaches, to one decimal place that is truncated
     * rather than rounded and dropped when it is zero. {@code 0} is {@code 0s}; {@code 3200ms} is {@code 3.2s};
     * {@code 15ms} stays {@code 15ms}.
     */
    public static String format(Duration duration) {
        long nanos = duration.toNanos();
        if (nanos == 0) {
            return "0s";
        }
        double value = nanos;
        String suffix = "nanos";
        if (nanos >= ChronoUnit.DAYS.getDuration().toNanos()) {
            value = nanos / (double) ChronoUnit.DAYS.getDuration().toNanos();
            suffix = "d";
        } else if (nanos >= ChronoUnit.HOURS.getDuration().toNanos()) {
            value = nanos / (double) ChronoUnit.HOURS.getDuration().toNanos();
            suffix = "h";
        } else if (nanos >= ChronoUnit.MINUTES.getDuration().toNanos()) {
            value = nanos / (double) ChronoUnit.MINUTES.getDuration().toNanos();
            suffix = "m";
        } else if (nanos >= ChronoUnit.SECONDS.getDuration().toNanos()) {
            value = nanos / (double) ChronoUnit.SECONDS.getDuration().toNanos();
            suffix = "s";
        } else if (nanos >= ChronoUnit.MILLIS.getDuration().toNanos()) {
            value = nanos / (double) ChronoUnit.MILLIS.getDuration().toNanos();
            suffix = "ms";
        } else if (nanos >= ChronoUnit.MICROS.getDuration().toNanos()) {
            value = nanos / (double) ChronoUnit.MICROS.getDuration().toNanos();
            suffix = "micros";
        }
        // Strings.format1Decimals: one digit after the point, truncated, and none if that digit is zero.
        String text = String.valueOf(value);
        int point = text.indexOf('.') + 1;
        int exponent = text.indexOf('E');
        String whole = text.substring(0, point - 1);
        char fraction = text.charAt(point);
        String tail = exponent == -1 ? "" : text.substring(exponent);
        return (fraction == '0' ? whole : text.substring(0, point) + fraction) + tail + suffix;
    }

    private static Duration of(String value, String normalized, String suffix, ChronoUnit unit, String setting) {
        String number = normalized.substring(0, normalized.length() - suffix.length()).trim();
        long amount;
        try {
            amount = Long.parseLong(number);
        } catch (NumberFormatException e) {
            try {
                Double.parseDouble(number);
            } catch (NumberFormatException notANumber) {
                throw new IllegalArgumentException("failed to parse [" + value + "]", e);
            }
            throw new IllegalArgumentException("failed to parse [" + value + "], fractional time values are not supported", e);
        }
        if (amount == -1) {
            return null;
        }
        if (amount < -1) {
            throw new IllegalArgumentException("failed to parse setting [" + setting + "] with value [" + value
                + "] as a time value: negative durations are not supported");
        }
        return Duration.of(amount, unit);
    }
}
