/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.template;

import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * The three kinds of template, and everything that differs between them.
 *
 * <p>Unlike ingest and search pipelines — which turned out to be the same API twice — these three are
 * genuinely different in ways that matter, so the shared code is thinner and the parameterisation
 * wider. All differences below were verified against OpenSearch 3.9.0.
 *
 * <table border="1">
 *   <caption>What differs</caption>
 *   <tr><th></th><th>legacy {@code _template}</th><th>{@code _index_template}</th><th>{@code _component_template}</th></tr>
 *   <tr><td>GET shape</td><td>name-keyed map</td><td>array wrapper</td><td>array wrapper</td></tr>
 *   <tr><td>GET missing</td><td>404 with bare <code>{}</code></td><td>404 resource_not_found</td><td>404 resource_not_found</td></tr>
 *   <tr><td>required</td><td>index_patterns</td><td>index_patterns</td><td>template</td></tr>
 *   <tr><td>missing-required error</td><td>action_request_validation_exception</td><td>illegal_argument_exception</td><td>illegal_argument_exception</td></tr>
 *   <tr><td>settings/mappings live</td><td>at the root</td><td>under {@code template}</td><td>under {@code template}</td></tr>
 *   <tr><td>honours {@code flat_settings}</td><td>yes</td><td>no</td><td>no</td></tr>
 * </table>
 *
 * <p>DELETE of a missing template is the one thing all three share: {@code index_template_missing_exception}
 * with {@code index_template [x] missing}, even for component templates.
 *
 * <p>The {@code flat_settings} row is not an oversight being reproduced blindly: the parameter is
 * declared on {@code indices.get_template} in the spec and not on the other two, and a live 3.9.0
 * cluster behaves accordingly — {@code GET /_template/x?flat_settings=true} answers
 * {@code index.number_of_replicas}, while the same parameter on {@code GET /_index_template/x} is
 * ignored and the response stays nested.
 *
 * @param keyPrefix       where definitions live
 * @param requiredProperty the body property the server insists on
 * @param legacyValidation whether a missing required property reports through the legacy path
 * @param notFoundNoun    the noun in a GET-missing message, or {@code null} for the bare-{@code {}} style
 * @param wrapperKey      the array wrapper's key, or {@code null} for the name-keyed style
 * @param itemKey         the per-item key inside the wrapper
 * @param templateBlockKey where {@code settings}/{@code mappings}/{@code aliases} sit within a
 *                        definition, or {@code null} when they sit at its root
 * @param honoursFlatSettings whether {@code flat_settings=true} changes this kind's GET rendering
 */
record TemplateKind(
    String keyPrefix,
    String requiredProperty,
    boolean legacyValidation,
    String notFoundNoun,
    String wrapperKey,
    String itemKey,
    String templateBlockKey,
    boolean honoursFlatSettings,
    String putGroup,
    String getGroup,
    String deleteGroup,
    String existsGroup
) {

    /** {@code /_template} — the pre-composable form, still supported and still different. */
    static TemplateKind legacy() {
        return new TemplateKind(
            Templates.LEGACY_KEY_PREFIX, "index_patterns", true, null, null, null, null, true,
            "indices.put_template", "indices.get_template",
            "indices.delete_template", "indices.exists_template"
        );
    }

    /** {@code /_index_template} — composable, references component templates via {@code composed_of}. */
    static TemplateKind index() {
        return new TemplateKind(
            "templates/index/", "index_patterns", false, "index template", "index_templates", "index_template",
            "template", false,
            "indices.put_index_template", "indices.get_index_template",
            "indices.delete_index_template", "indices.exists_index_template"
        );
    }

    /** {@code /_component_template} — reusable fragments; note these live in the cluster namespace. */
    static TemplateKind component() {
        return new TemplateKind(
            "templates/component/", "template", false, "component template", "component_templates", "component_template",
            "template", false,
            "cluster.put_component_template", "cluster.get_component_template",
            "cluster.delete_component_template", "cluster.exists_component_template"
        );
    }

    /** Legacy GET answers a name-keyed map; the newer kinds answer an array under a wrapper key. */
    boolean isNameKeyedResponse() {
        return wrapperKey == null;
    }

    /**
     * The block holding {@code settings}, {@code mappings} and {@code aliases} within a definition.
     *
     * <p>Legacy templates carry them at the root; the newer kinds nest them under {@code template}.
     * Everything that reads or rewrites a template's contents goes through here rather than knowing
     * which kind it is holding.
     *
     * @return the block, or {@code null} when the definition has none
     */
    ObjectNode blockOf(ObjectNode definition) {
        if (templateBlockKey == null) {
            return definition;
        }
        return definition.get(templateBlockKey) instanceof ObjectNode block ? block : null;
    }
}
