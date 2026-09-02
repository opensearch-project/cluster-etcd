/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.rest;

/**
 * The parameters the spec attaches to every operation, and where each is handled.
 *
 * <p>Collected in one place because they are cross-cutting: a handler should never have to think about
 * them, and a new handler should inherit them for free. Codegen gathers the same set into
 * {@code GeneratedRoutes.GLOBAL_QUERY_PARAMS} from the spec's {@code x-global} markers.
 *
 * <table border="1">
 *   <caption>Support</caption>
 *   <tr><th>parameter</th><th>where</th></tr>
 *   <tr><td>{@link #PRETTY}</td><td>{@link Json#response} — two-space indent, matching the server</td></tr>
 *   <tr><td>{@link #FILTER_PATH}</td><td>{@link Json#response} via {@link FilterPath}, success responses only</td></tr>
 *   <tr><td>{@link #SOURCE}</td><td>{@code Router.dispatch}, with {@link #SOURCE_CONTENT_TYPE}</td></tr>
 *   <tr><td>{@link #ERROR_TRACE}</td><td>{@link ErrorResponses#internalError}, for genuine exceptions</td></tr>
 *   <tr><td>{@link #HUMAN}</td><td><b>deliberately unsupported</b> — see below</td></tr>
 * </table>
 *
 * <h2>Why {@code human} is not supported</h2>
 *
 * <p>Not an omission. Reformatting durations and byte sizes for human eyes is a client-side
 * presentation concern, and a response format should stay machine-parseable; a parameter that changes
 * value representations mid-API works against that. The view is shared within the OpenSearch community
 * and the parameter is a candidate for deprecation, so declining it from the start leaves this service
 * ahead rather than behind.
 *
 * <p>It is accepted and ignored rather than rejected, because the control plane should be robust to
 * callers that send it. Sending {@code human=true} gets a correct machine-readable response, not an
 * error.
 */
public final class GlobalParameters {

    public static final String PRETTY = "pretty";
    public static final String FILTER_PATH = "filter_path";
    public static final String ERROR_TRACE = "error_trace";
    public static final String SOURCE = "source";

    /** Required alongside {@link #SOURCE}; a real cluster rejects one without the other. */
    public static final String SOURCE_CONTENT_TYPE = "source_content_type";

    /** Accepted and ignored. See the class javadoc. */
    public static final String HUMAN = "human";

    private GlobalParameters() {}
}
