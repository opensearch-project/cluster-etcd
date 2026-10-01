/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.pipeline;

class IngestPipelineHandlerTests extends PipelineHandlerContractTests {

    @Override
    protected String urlPrefix() {
        return "/_ingest/pipeline";
    }

    @Override
    protected PipelineKind kind() {
        return PipelineKind.ingest();
    }

    @Override
    protected String requiredProperty() {
        return "processors";
    }
}
