/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.store;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;

/**
 * {@code file:///var/lib/control-plane} — the local-filesystem store.
 *
 * <p>Bundled with the SPI rather than shipped as its own jar, because it needs nothing beyond the JDK
 * and because a control plane with no backend configured should still be able to start somewhere.
 */
public final class FileSystemKeyValueStoreProvider implements KeyValueStoreProvider {

    @Override
    public String scheme() {
        return "file";
    }

    @Override
    public KeyValueStore open(URI uri) throws IOException {
        // Accepts both file:///abs/path and the shorter file:relative/path, since an operator writing
        // the latter means the obvious thing and URI.getPath() returns null for it.
        String path = uri.getPath() != null && uri.getPath().isEmpty() == false
            ? uri.getPath()
            : uri.getSchemeSpecificPart();
        if (path == null || path.isBlank()) {
            throw new IOException("store [" + uri + "] needs a directory: " + usage());
        }
        return new FileSystemKeyValueStore(Path.of(path));
    }

    @Override
    public String usage() {
        return "file:///path/to/dir   local filesystem, one file per key";
    }
}
