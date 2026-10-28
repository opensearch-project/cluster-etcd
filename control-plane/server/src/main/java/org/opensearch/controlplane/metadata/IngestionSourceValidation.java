/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.metadata;

import java.util.Locale;
import java.util.Optional;
import java.util.Set;

/**
 * The value checks OpenSearch makes on a pull-based index's ingestion settings.
 *
 * <p>Without them a bad configuration is acknowledged here, placed by the supervisor, and then refused by every node
 * that tries to create the shard, so the index never serves and the client was told it succeeded. Each rule and its
 * message is the server's own, read from OpenSearch 3.10.0-SNAPSHOT: the {@code IndexMetadata} validators for
 * {@code pointer.init.reset} and {@code all_active}, {@code Booleans.parseBoolean},
 * {@code IngestionErrorStrategy.ErrorStrategy.parseFromString} and {@code IngestionMessageMapper.MapperType.fromString}.
 *
 * <p>Source parameters ({@code index.ingestion_source.param.*}) are not checked. The server does not check them at
 * create either, and which ones a source needs belongs to its plugin.
 */
public final class IngestionSourceValidation {

    static final String TYPE = "index.ingestion_source.type";
    static final String ALL_ACTIVE = "index.ingestion_source.all_active";
    static final String ERROR_STRATEGY = "index.ingestion_source.error_strategy";
    static final String MAPPER_TYPE = "index.ingestion_source.mapper_type";
    static final String POINTER_INIT_RESET = "index.ingestion_source.pointer.init.reset";
    static final String POINTER_INIT_RESET_VALUE = "index.ingestion_source.pointer.init.reset.value";
    private static final String REPLICATION_TYPE = "index.replication.type";

    private static final Set<String> RESET_STATES = Set.of("latest", "earliest", "reset_by_offset", "reset_by_timestamp");
    private static final Set<String> RESET_STATES_NEEDING_VALUE = Set.of("reset_by_offset", "reset_by_timestamp");
    private static final Set<String> ERROR_STRATEGIES = Set.of("drop", "block");
    private static final Set<String> MAPPER_TYPES = Set.of("default", "raw_payload", "field_mapping");

    private IngestionSourceValidation() {}

    /**
     * The first problem with an index being created, or empty if there is none or the index is not pull-based.
     *
     * @param defaultReplicationType the replication type of an index that names none, which the server takes as
     *     {@code DOCUMENT}; it decides whether {@code all_active} is required or forbidden
     */
    public static Optional<String> checkCreate(Settings settings, String defaultReplicationType) {
        if (isPullBased(settings) == false) {
            return Optional.empty();
        }

        Optional<String> allActiveValue = settings.get(ALL_ACTIVE);
        if (allActiveValue.isPresent() && isBoolean(allActiveValue.get()) == false) {
            return Optional.of(notABoolean(allActiveValue.get()));
        }
        boolean allActive = allActiveValue.map(Boolean::parseBoolean).orElse(false);
        String replication = settings.get(REPLICATION_TYPE).orElse(defaultReplicationType);
        if ("SEGMENT".equalsIgnoreCase(replication) && allActive) {
            return Optional.of("Replication type SEGMENT is not supported in pull-based ingestion when " + ALL_ACTIVE
                + " is enabled");
        }
        if ("DOCUMENT".equalsIgnoreCase(replication) && allActive == false) {
            return Optional.of("Replication type DOCUMENT is not supported in pull-based ingestion when " + ALL_ACTIVE
                + " is not enabled");
        }

        Optional<String> errorStrategy = checkErrorStrategy(settings);
        if (errorStrategy.isPresent()) {
            return errorStrategy;
        }

        Optional<String> mapperType = settings.get(MAPPER_TYPE);
        if (mapperType.isPresent() && MAPPER_TYPES.contains(mapperType.get().toLowerCase(Locale.ROOT)) == false) {
            return Optional.of("Unknown ingestion mapper type: " + mapperType.get()
                + ". Valid values are: default, raw_payload, field_mapping");
        }

        Optional<String> reset = settings.get(POINTER_INIT_RESET);
        if (reset.isPresent()) {
            String state = reset.get().toLowerCase(Locale.ROOT);
            if (RESET_STATES.contains(state) == false) {
                return Optional.of("Invalid value for " + POINTER_INIT_RESET + " [" + reset.get() + "]");
            }
            if (RESET_STATES_NEEDING_VALUE.contains(state) && settings.get(POINTER_INIT_RESET_VALUE).orElse("").isEmpty()) {
                return Optional.of("Setting " + POINTER_INIT_RESET_VALUE + " should be set when resetting consumer position");
            }
        }
        return Optional.empty();
    }

    /**
     * The first problem with an update to existing indices. Only {@code error_strategy} is checked: it is the one
     * dynamic setting here, and an update to any of the others is refused before its value matters.
     */
    public static Optional<String> checkUpdate(Settings updates) {
        return checkErrorStrategy(updates);
    }

    private static Optional<String> checkErrorStrategy(Settings settings) {
        Optional<String> strategy = settings.get(ERROR_STRATEGY);
        if (strategy.isPresent() && ERROR_STRATEGIES.contains(strategy.get().toLowerCase(Locale.ROOT)) == false) {
            return Optional.of("Invalid ingestion errorStrategy: " + strategy.get());
        }
        return Optional.empty();
    }

    private static boolean isPullBased(Settings settings) {
        return "none".equals(settings.get(TYPE).orElse("none")) == false;
    }

    private static boolean isBoolean(String value) {
        return "true".equals(value) || "false".equals(value);
    }

    private static String notABoolean(String value) {
        return "Failed to parse value [" + value + "] as only [true] or [false] are allowed.";
    }
}
