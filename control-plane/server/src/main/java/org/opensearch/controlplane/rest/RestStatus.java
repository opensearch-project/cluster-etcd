/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.rest;

/**
 * The subset of HTTP status codes this service produces, named as in OpenSearch's
 * {@code RestStatus} so ported code reads the same.
 */
public enum RestStatus {
    OK(200),
    CREATED(201),
    ACCEPTED(202),
    NO_CONTENT(204),
    BAD_REQUEST(400),
    UNAUTHORIZED(401),
    FORBIDDEN(403),
    NOT_FOUND(404),
    METHOD_NOT_ALLOWED(405),
    CONFLICT(409),
    REQUEST_ENTITY_TOO_LARGE(413),
    UNSUPPORTED_MEDIA_TYPE(415),
    INTERNAL_SERVER_ERROR(500),
    NOT_IMPLEMENTED(501),
    SERVICE_UNAVAILABLE(503);

    private final int code;

    RestStatus(int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }
}
