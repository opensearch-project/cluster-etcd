/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.template;

import org.opensearch.controlplane.capability.SettingsRegistry;
import org.opensearch.controlplane.handler.OperationHandler;

import java.util.List;

/** The twelve template handlers: four operations across three kinds. */
public final class Templates {

    /** Where legacy {@code _template} definitions live; {@code cluster.state} renders them under {@code metadata.templates}. */
    public static final String LEGACY_KEY_PREFIX = "templates/legacy/";

    private Templates() {}

    public static List<OperationHandler> handlers(SettingsRegistry registry) {
        return List.of(TemplateKind.legacy(), TemplateKind.index(), TemplateKind.component()).stream()
            .flatMap(kind -> List.<OperationHandler>of(
                new PutTemplateHandler(kind, registry),
                new GetTemplateHandler(kind),
                new DeleteTemplateHandler(kind),
                new ExistsTemplateHandler(kind)
            ).stream())
            .toList();
    }

    /** The two simulation endpoints, which are pure functions over the composition engine. */
    public static List<OperationHandler> simulationHandlers() {
        return List.of(new SimulateTemplateHandler(false), new SimulateTemplateHandler(true));
    }
}
