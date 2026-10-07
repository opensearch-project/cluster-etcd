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
