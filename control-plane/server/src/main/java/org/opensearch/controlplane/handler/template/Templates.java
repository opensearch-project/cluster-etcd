/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.template;

import org.opensearch.controlplane.handler.OperationHandler;

import java.util.List;

/** The twelve template handlers: four operations across three kinds. */
public final class Templates {

    private Templates() {}

    public static List<OperationHandler> handlers() {
        return List.of(TemplateKind.legacy(), TemplateKind.index(), TemplateKind.component()).stream()
            .flatMap(kind -> List.<OperationHandler>of(
                new PutTemplateHandler(kind),
                new GetTemplateHandler(kind),
                new DeleteTemplateHandler(kind),
                new ExistsTemplateHandler(kind)
            ).stream())
            .toList();
    }
}
