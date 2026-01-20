/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.cluster.etcd;

import io.etcd.jetcd.Client;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public class ETCDClientHolder implements AutoCloseable {
    private final Logger logger = LogManager.getLogger(ETCDClientHolder.class);
    private final Supplier<Client> clientSupplier;
    private final AtomicReference<Client> currentClient = new AtomicReference<>();
    private volatile boolean closed = false;

    public ETCDClientHolder(Supplier<Client> clientSupplier) {
        this.clientSupplier = clientSupplier;
        currentClient.set(clientSupplier.get());
    }

    public Client getClient() {
        if (closed) {
            throw new IllegalStateException("Client is already closed");
        }
        return currentClient.get();
    }

    public void resetClient() {
        if (closed) {
            throw new IllegalStateException("Client is already closed");
        }
        try {
            Client client = currentClient.getAndSet(clientSupplier.get());
            client.close();
        } catch (Exception e) {
            logger.warn("Error closing client", e);
        }
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        Client client = currentClient.getAndSet(null);
        client.close();
    }
}
