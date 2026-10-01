/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.handler.cluster;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.opensearch.controlplane.handler.OperationContext;
import org.opensearch.controlplane.handler.OperationHandler;
import org.opensearch.controlplane.rest.ErrorResponses;
import org.opensearch.controlplane.rest.Json;
import org.opensearch.controlplane.rest.RestRequest;
import org.opensearch.controlplane.rest.RestResponse;
import org.opensearch.controlplane.rest.RestStatus;
import org.opensearch.controlplane.store.KeyValueStore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

/**
 * Weighted shard routing: {@code GET}, {@code PUT} and {@code DELETE} on
 * {@code /_cluster/routing/awareness/{attribute}/weights}.
 *
 * <p>Weights bias search traffic across the values of an awareness attribute — sending a tenth of the
 * queries to a struggling zone rather than none or all. Setting them is a metadata write that completes
 * in its own request, which is what makes this the control plane's, unlike the decommission API next
 * door whose {@code PUT} starts a node drain.
 *
 * <h2>One document, not one per attribute</h2>
 *
 * <p>The API reads as though {@code {attribute}} identifies a resource. It does not.
 * {@code WeightedRoutingMetadata} holds a single attribute name and its weights, so:
 *
 * <ul>
 *   <li>a {@code PUT} for {@code rack} <b>replaces</b> weights previously set for {@code zone} rather
 *       than sitting alongside them;</li>
 *   <li>a {@code GET} ignores its own path parameter when looking anything up — with weights stored for
 *       {@code zone}, {@code GET /_cluster/routing/awareness/rack/weights} returns them. The attribute is
 *       validated, then discarded;</li>
 *   <li>only {@code DELETE} compares the path attribute against the stored one, and answers 404 when
 *       they differ.</li>
 * </ul>
 *
 * <p>All three verified against 3.9.0. This is reproduced rather than corrected: the single-document
 * shape is what the version counter counts, and a client that has learned to work with it would break
 * against a service that quietly made {@code {attribute}} real.
 *
 * <h2>Verified behaviour</h2>
 *
 * <ul>
 *   <li><b>Weights are strings on the wire and doubles underneath.</b> {@code "1"} in comes back as
 *       {@code "1.0"}. A JSON <em>number</em> is rejected at parse time, and a non-numeric string escapes
 *       as a raw {@code number_format_exception}.</li>
 *   <li><b>{@code _version} is a JSON number on {@code PUT} and either on {@code DELETE}.</b> Not a
 *       quirk worth smoothing over — it follows from {@code PUT} parsing the body with {@code mapOrdered}
 *       and {@code DELETE} with {@code mapStrings}, which coerces every scalar to a string first. A
 *       string version on {@code PUT} is a parse error; a numeric one on {@code DELETE} is accepted.</li>
 *   <li><b>A successful write stores {@code sent + 1}</b>, so the first write sends {@code -1} and the
 *       stored version becomes {@code 0}. A {@code PUT} whose weights match what is already there is
 *       acknowledged <em>without</em> bumping the version.</li>
 *   <li><b>{@code DELETE} empties the document rather than removing it</b>, and bumps the version doing
 *       so — including when the weights were already empty. So it is not idempotent in the version, and
 *       a second {@code DELETE} needs the new version.</li>
 *   <li><b>{@code GET} on a never-written cluster is {@code 200 {}}</b>, an entirely empty object, while
 *       after a delete it is {@code {"weights":{},"_version":N,"discovered_cluster_manager":true}}.</li>
 *   <li><b>{@code DELETE} does not validate the awareness attribute</b> where {@code GET} and {@code PUT}
 *       do: an unconfigured name gives 404, not the 400 the others give.</li>
 * </ul>
 *
 * <h2>Considered divergences</h2>
 *
 * <p><b>A {@code PUT} with no body is a 400, not a 500.</b> OpenSearch never parses one, leaves its
 * request object null, and then dereferences it in {@code validate()} — the response is a 500 whose
 * reason is a raw {@code NullPointerException} message naming a server-internal class. Reproducing a
 * crash is not compatibility. This answers {@code 400 parse_exception}, {@code request body is required},
 * matching the repository handlers.
 *
 * <p><b>The response's weight keys are sorted</b> rather than in Java hash order, for the reason given on
 * {@code Settings}: JSON object order is not semantic and the alternative is reproducing
 * {@code HashMap} iteration. Note this does <em>not</em> extend to the two error messages that embed a
 * rendered map — those are strings a client may compare, so {@link #renderForMessage} keeps hash order
 * deliberately.
 *
 * <p>Both recorded in the README's considered divergences.
 *
 * <h2>What is not validated, and why</h2>
 *
 * <p>A real cluster rejects a {@code PUT} that leaves any <em>discovered</em> attribute value without a
 * weight — any value some routing node actually carries. That set comes from the live node list, which
 * this service does not have and cannot infer: it is the third tier of the capabilities question,
 * reported nowhere it can read.
 *
 * <p>It is also the wrong thing to put in {@code capabilities.yaml}, unlike processor types. Node
 * attribute values change as nodes join and leave, so a file generated once would go stale while
 * claiming to be authoritative — and the failure mode is rejecting a legitimate weight update, which is
 * worse than not checking. When the management plane projects live node state into the shared store, this
 * check becomes a store read like any other; until then the {@code forced} half of the same rule is
 * enforced, since {@code cluster.routing.allocation.awareness.force.<attribute>.values} is a setting this
 * service owns. See {@link AwarenessConfiguration}.
 *
 * <p>The decommission checks are skipped for a scope reason rather than a data one: decommissioning is
 * management-plane work and no decommission can be in progress here, so the condition cannot arise.
 */
public final class WeightedRoutingHandlers {

    private static final String WEIGHTS = "weights";
    private static final String VERSION = "_version";
    private static final String DISCOVERED_CLUSTER_MANAGER = "discovered_cluster_manager";

    private WeightedRoutingHandlers() {}

    public static List<OperationHandler> handlers() {
        return List.of(new Get(), new Put(), new Delete());
    }

    /**
     * {@code GET /_cluster/routing/awareness/{attribute}/weights}.
     *
     * <p>Validates the attribute, then ignores it — see the note on the enclosing class.
     */
    static final class Get implements OperationHandler {

        @Override
        public String operationGroup() {
            return "cluster.get_weighted_routing";
        }

        @Override
        public RestResponse handle(RestRequest request, OperationContext context) {
            String attribute = request.pathParam("attribute");
            AwarenessConfiguration awareness = AwarenessConfiguration.read(context.store());
            if (awareness.allows(attribute) == false) {
                return invalidAwarenessAttribute(request, attribute);
            }

            Optional<WeightedRoutingStore.Document> stored = new WeightedRoutingStore(context.store()).read();
            if (stored.isEmpty()) {
                // Never written: an empty object, with no _version and no discovered_cluster_manager.
                return Json.response(RestStatus.OK, Json.object(), request);
            }

            WeightedRoutingStore.Document document = stored.get();
            ObjectNode body = Json.object();
            ObjectNode weights = body.putObject(WEIGHTS);
            new TreeMap<>(document.weights()).forEach((value, weight) -> weights.put(value, Double.toString(weight)));
            body.put(VERSION, document.version());
            // Always true, and not a stub. The field reports whether a cluster manager has been
            // discovered, i.e. whether there is an authority for this metadata. Here the control plane is
            // that authority and it just answered the request, so the condition it describes holds by
            // construction -- there is no election that could leave it unsatisfied.
            body.put(DISCOVERED_CLUSTER_MANAGER, true);
            return Json.response(RestStatus.OK, body, request);
        }
    }

    /** {@code PUT /_cluster/routing/awareness/{attribute}/weights}. */
    static final class Put implements OperationHandler {

        @Override
        public String operationGroup() {
            return "cluster.put_weighted_routing";
        }

        @Override
        public RestResponse handle(RestRequest request, OperationContext context) {
            String attribute = request.pathParam("attribute");
            if (request.hasBody() == false) {
                // Considered divergence: OpenSearch NPEs into a 500 here. See the class javadoc.
                return ErrorResponses.parseFailure(request, "request body is required");
            }

            JsonNode body;
            try {
                body = Json.parse(request.body());
            } catch (IOException e) {
                return ErrorResponses.parseFailure(request, "failed to parse weighted routing request");
            }
            if (body == null || body.isObject() == false) {
                return ErrorResponses.parseFailure(request, "failed to parse weighted routing request");
            }
            if (body.isEmpty()) {
                return ErrorResponses.parseFailure(request, "Empty request body");
            }

            Parsed parsed;
            try {
                parsed = parse(body, attribute);
            } catch (ParseFailure failure) {
                return failure.toResponse(request);
            }

            RestResponse rejection = validate(request, attribute, parsed);
            if (rejection != null) {
                return rejection;
            }

            AwarenessConfiguration awareness = AwarenessConfiguration.read(context.store());
            if (awareness.allows(attribute) == false) {
                return invalidAwarenessAttribute(request, attribute);
            }

            // The forced half of ensureWeightsSetForAllDiscoveredAndForcedAwarenessValues; the discovered
            // half needs live node state. See the class javadoc.
            Set<String> required = new HashSet<>(awareness.forcedValues(attribute));
            for (String value : required) {
                if (parsed.weights().containsKey(value) == false) {
                    return ErrorResponses.unsupportedWeightedRoutingState(
                        request,
                        "weight for [" + value + "] is not set and it is part of forced awareness value "
                            + "or a routing node has this attribute."
                    );
                }
            }
            long requiredZeroes = required.stream().filter(value -> parsed.weights().get(value) == 0).count();
            if (requiredZeroes > required.size() / 2) {
                // Note "discovered attribute values" -- one word longer than the request-level message for
                // the same condition, because a different method raised it.
                return ErrorResponses.validationFailed(
                    request,
                    "There are too many discovered attribute values [" + renderForMessage(parsed.weights())
                        + "] given zero weight [" + requiredZeroes
                        + "]. Maximum expected number of routing weights having zero weight is ["
                        + required.size() / 2 + "]"
                );
            }

            WeightedRoutingStore store = new WeightedRoutingStore(context.store());
            Optional<WeightedRoutingStore.Document> stored = store.read();
            long currentVersion = stored.map(WeightedRoutingStore.Document::version)
                .orElse(WeightedRoutingStore.INITIAL_VERSION);
            if (parsed.version() != currentVersion) {
                return ErrorResponses.weightedRoutingVersionConflict(request, parsed.version(), currentVersion);
            }

            if (stored.isPresent()
                && stored.get().attribute().equals(attribute)
                && stored.get().weights().equals(parsed.weights())) {
                // Unchanged: acknowledged, but the version deliberately does not move. Verified.
                return Json.acknowledged(request);
            }

            long revision = stored.map(WeightedRoutingStore.Document::revision).orElse(KeyValueStore.ABSENT);
            if (store.write(attribute, parsed.weights(), parsed.version() + 1, revision) == false) {
                return ErrorResponses.concurrentUpdate(request, "_cluster/routing/awareness/weights");
            }
            return Json.acknowledged(request);
        }

        /**
         * Reproduces {@code ClusterPutWeightedRoutingRequest.setWeightedRouting}, whose messages depend on
         * where in the body it gave up.
         *
         * <p>Field order matters, because the first offending field wins: {@code {"foo":1,"_version":"x"}}
         * reports {@code foo} and {@code {"_version":"x","foo":1}} reports the version. Jackson preserves
         * object order, so iterating the parsed tree reproduces the streaming parser's sequence.
         */
        private static Parsed parse(JsonNode body, String attribute) {
            HashMap<String, Double> weights = new HashMap<>();
            long version = WeightedRoutingStore.VERSION_UNSET;
            // The server reports the *previously* parsed weight when a weight is not a string, because the
            // variable holding it lives outside the loop and is never cleared. Null until one parses, which
            // is why a first bad weight reports "[null]". Faithfully odd.
            Double lastWeight = null;

            for (Map.Entry<String, JsonNode> field : body.properties()) {
                String name = field.getKey();
                JsonNode value = field.getValue();
                if (VERSION.equals(name)) {
                    if (value.isNumber() == false) {
                        throw new ParseFailure(
                            "failed to parse weighted routing request [" + attribute + "], unknown type");
                    }
                    version = value.longValue();
                    continue;
                }
                if (WEIGHTS.equals(name) == false) {
                    throw new ParseFailure("failed to parse weighted routing request object [" + name + "]");
                }
                if (value.isObject() == false) {
                    // Two spaces after "object", from the server's format string. Not a typo here.
                    throw new ParseFailure(
                        "failed to parse weighted routing request object  [weights], expected object");
                }
                for (Map.Entry<String, JsonNode> weight : value.properties()) {
                    if (weight.getValue().isTextual() == false) {
                        throw new ParseFailure(
                            "failed to parse weighted routing request attribute [" + lastWeight + "], unknown type");
                    }
                    lastWeight = parseWeight(weight.getValue().textValue());
                    weights.put(weight.getKey(), lastWeight);
                }
            }
            return new Parsed(weights, version);
        }

        private static double parseWeight(String text) {
            try {
                return Double.parseDouble(text);
            } catch (NumberFormatException notANumber) {
                // Escapes the server's parser unwrapped, so the client sees the JDK's own message.
                throw ParseFailure.notANumber(text);
            }
        }

        /**
         * Reproduces {@code ClusterPutWeightedRoutingRequest.validate}, which accumulates.
         *
         * <p>Two of its branches are unreachable over REST and are omitted rather than written as dead
         * code: a null weight and a non-numeric one both fail in the parser first, which is why a weight
         * of {@code "abc"} is a {@code number_format_exception} and never {@code Weight is not a number}.
         */
        private static RestResponse validate(RestRequest request, String attribute, Parsed parsed) {
            List<String> errors = new ArrayList<>(3);
            if (attribute == null || attribute.isEmpty()) {
                errors.add("Attribute name is missing");
            }
            if (parsed.weights().isEmpty()) {
                errors.add("Weights are missing");
            }
            if (parsed.version() == WeightedRoutingStore.VERSION_UNSET) {
                errors.add("Version is missing");
            }
            if (errors.isEmpty() == false) {
                return ErrorResponses.validationFailed(request, errors.toArray(String[]::new));
            }

            long zeroes = parsed.weights().values().stream().filter(weight -> weight == 0).count();
            if (zeroes > parsed.weights().size() / 2) {
                return ErrorResponses.validationFailed(
                    request,
                    "There are too many attribute values [" + renderForMessage(parsed.weights())
                        + "] given zero weight [" + zeroes
                        + "]. Maximum expected number of routing weights having zero weight is ["
                        + parsed.weights().size() / 2 + "]"
                );
            }
            return null;
        }

        private record Parsed(HashMap<String, Double> weights, long version) {}
    }

    /**
     * {@code DELETE /_cluster/routing/awareness/weights} and
     * {@code DELETE /_cluster/routing/awareness/{attribute}/weights}.
     *
     * <p>The second route is one OpenSearch registers and the API specification does not, so only the
     * first is generated today and {@link RestRequest#pathParam} returns null for the attribute. The
     * attribute-matching behaviour below is implemented anyway: it is what makes the no-attribute form
     * correct — matching everything is a special case of matching — and it means adding the route to the
     * spec needs no change here.
     */
    static final class Delete implements OperationHandler {

        @Override
        public String operationGroup() {
            return "cluster.delete_weighted_routing";
        }

        @Override
        public RestResponse handle(RestRequest request, OperationContext context) {
            String attribute = request.pathParam("attribute");

            long requestedVersion = WeightedRoutingStore.VERSION_UNSET;
            if (request.hasBody()) {
                JsonNode body;
                try {
                    body = Json.parse(request.body());
                } catch (IOException e) {
                    return ErrorResponses.parseFailure(
                        request, "failed to parse delete weighted routing request body");
                }
                if (body == null || body.isObject() == false) {
                    return ErrorResponses.parseFailure(
                        request, "failed to parse delete weighted routing request body");
                }
                if (body.isEmpty()) {
                    return ErrorResponses.parseFailure(request, "Empty request body");
                }
                try {
                    requestedVersion = parseVersion(body);
                } catch (ParseFailure failure) {
                    return failure.toResponse(request);
                }
            }

            WeightedRoutingStore store = new WeightedRoutingStore(context.store());
            Optional<WeightedRoutingStore.Document> stored = store.read();
            long currentVersion = stored.map(WeightedRoutingStore.Document::version)
                .orElse(WeightedRoutingStore.INITIAL_VERSION);
            // Before the not-found check, so a delete with no body on a cluster that has never had weights
            // reports the version mismatch (-2 against -1) rather than "nothing to delete". Verified.
            if (requestedVersion != currentVersion) {
                return ErrorResponses.weightedRoutingVersionConflict(request, requestedVersion, currentVersion);
            }

            if (stored.isEmpty() || (attribute != null && attribute.equals(stored.get().attribute()) == false)) {
                return ErrorResponses.resourceNotFound(
                    request,
                    "weighted routing metadata does not have weights set for awareness attribute " + attribute
                );
            }

            // Emptied, not removed, and the version moves even when there was nothing to clear.
            if (store.write("", Map.of(), currentVersion + 1, stored.get().revision()) == false) {
                return ErrorResponses.concurrentUpdate(request, "_cluster/routing/awareness/weights");
            }
            return Json.acknowledged(request);
        }

        /**
         * Reproduces {@code ClusterDeleteWeightedRoutingRequest.setRequestBody}, but note the coercion it
         * sits behind: the REST layer calls {@code mapStrings()}, so by the time the body is parsed every
         * scalar is already a string. That is the whole of why {@code _version} may be a JSON number here
         * and may not on {@code PUT}.
         */
        private static long parseVersion(JsonNode body) {
            long version = WeightedRoutingStore.VERSION_UNSET;
            for (Map.Entry<String, JsonNode> field : body.properties()) {
                if (VERSION.equals(field.getKey()) == false) {
                    throw new ParseFailure(
                        "failed to parse delete weighted routing request body [" + field.getKey()
                            + "], unknown type");
                }
                JsonNode value = field.getValue();
                if (value.isContainerNode() || value.isNull()) {
                    // mapStrings() only coerces scalars; anything else never reaches the version branch.
                    throw new ParseFailure("failed to parse delete weighted routing request body");
                }
                try {
                    version = Long.parseLong(value.asText());
                } catch (NumberFormatException notANumber) {
                    throw ParseFailure.notANumber(value.asText());
                }
            }
            return version;
        }
    }

    /** {@code 400} for an attribute that is not a configured awareness attribute. */
    private static RestResponse invalidAwarenessAttribute(RestRequest request, String attribute) {
        return ErrorResponses.validationFailed(
            request, "invalid awareness attribute " + attribute + " requested for weighted routing");
    }

    /**
     * Weights as OpenSearch embeds them in an error message: {@code {a=0.0, b=1.0}}.
     *
     * <p>{@link HashMap#toString()} on purpose. The server renders the same map the same way, so hash
     * iteration order is part of the message, and a sorted rendering would differ from a real cluster's
     * for any key set whose hash order is not alphabetical. The JSON responses sort instead — order is not
     * semantic there, and it is inside a string here.
     */
    private static String renderForMessage(HashMap<String, Double> weights) {
        return weights.toString();
    }

    /** Carries a message from the parsers up to the handler, which turns it into the right response. */
    private static final class ParseFailure extends RuntimeException {

        private final boolean numberFormat;

        ParseFailure(String reason) {
            this(reason, false);
        }

        /** A value that had to parse as a number and did not; reported with the JDK's own wording. */
        static ParseFailure notANumber(String input) {
            return new ParseFailure(input, true);
        }

        private ParseFailure(String reason, boolean numberFormat) {
            super(reason, null, false, false);
            this.numberFormat = numberFormat;
        }

        RestResponse toResponse(RestRequest request) {
            return numberFormat
                ? ErrorResponses.numberFormat(request, getMessage())
                : ErrorResponses.parseFailure(request, getMessage());
        }
    }
}
