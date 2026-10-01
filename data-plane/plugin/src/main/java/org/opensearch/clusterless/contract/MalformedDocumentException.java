/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.contract;

/**
 * A document that could not be parsed at all.
 *
 * <p>Thrown rather than swallowed, because only the caller knows what to do with it — and for this plugin the
 * answers genuinely differ. A node reading its own <b>assignment</b> must treat an unreadable document as
 * <em>no instruction</em>, never as an empty one: an empty assignment means "drop every shard you hold", and
 * inferring that from a parse failure would let one corrupt write empty a node. An unreadable <b>index
 * document</b> costs that one index a pass. Neither is a reason to stop the pass.
 */
public class MalformedDocumentException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public MalformedDocumentException(String what, String because) {
        super("could not parse " + what + ": " + because);
    }

    public MalformedDocumentException(String what, Throwable cause) {
        super("could not parse " + what + ": " + cause.getMessage(), cause);
    }
}
