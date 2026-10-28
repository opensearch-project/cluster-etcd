/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.pipeline;

class SearchPipelineHandlerTests extends PipelineHandlerContractTests {

    @Override
    protected String urlPrefix() {
        return "/_search/pipeline";
    }

    @Override
    protected PipelineKind kind() {
        return PipelineKind.search();
    }

    /** A search pipeline's three processor lists are all optional, so {@code {}} is valid. */
    @Override
    protected String requiredProperty() {
        return null;
    }
}
