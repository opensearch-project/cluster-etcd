/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane;

import org.opensearch.controlplane.handler.OperationContextFactory;
import org.opensearch.controlplane.store.InMemoryKeyValueStore;
import org.opensearch.controlplane.store.KeyValueStore;
import org.opensearch.controlplane.tenant.SingleTenantResolver;
import org.opensearch.controlplane.tenant.TenantResolver;

/** Context factories for tests. */
public final class TestContexts {

    private TestContexts() {}

    /** Single tenant, throwaway store. For tests that never touch storage. */
    public static OperationContextFactory factory() {
        return factory(new InMemoryKeyValueStore());
    }

    /** Single tenant over the given root store. */
    public static OperationContextFactory factory(KeyValueStore rootStore) {
        return new OperationContextFactory(rootStore, new SingleTenantResolver());
    }

    /** Multi-tenant over the given root store. */
    public static OperationContextFactory factory(KeyValueStore rootStore, TenantResolver resolver) {
        return new OperationContextFactory(rootStore, resolver);
    }
}
