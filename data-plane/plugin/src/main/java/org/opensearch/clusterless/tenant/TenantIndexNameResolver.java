/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.clusterless.tenant;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.clusterless.ClusterlessIndexSettings;
import org.opensearch.clusterless.contract.TenantId;
import org.opensearch.common.regex.Regex;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Turns the index names a tenant uses into the names this node's cluster state carries.
 *
 * <p>A tenant asks for {@code my-index}. The fabricated index is called {@code default.my-index.c526cf4262},
 * because a {@code ClusterState} has one flat index namespace and index names are only unique within a tenant —
 * see {@code IndexNaming}. Without something in between, every client of every tenant has to know a name
 * containing a hash of its own tenant id, which is both unusable and a leak of how the plumbing works.
 *
 * <p>The mapping is a <b>lookup, never a parse</b>. {@code IndexNaming} is deliberately not reversible and says
 * why: an escaping scheme that could be decoded would invite reading identity back out of a name. What makes the
 * lookup possible is that the fabrication already carries identity in index settings —
 * {@code index.clusterless.tenant} and {@code index.clusterless.name} — which were added so that the reaper could
 * find an index's document in the store. The same two settings answer this question, from local state, with no
 * read of anything.
 *
 * <h2>It is also the tenant scope, and that is the more important half</h2>
 *
 * <p>{@code GET /_search} with no index resolves to <em>every</em> index in the state. On a node serving several
 * tenants that is a cross-tenant read, and no amount of careful naming prevents it. The resolvers are what
 * OpenSearch runs on that path — an empty expression list becomes {@code _all} and then goes through every
 * resolver, custom ones first — so this is the point at which {@code _all} can be narrowed to the tenant that
 * asked. Which is why this class expands wildcards rather than leaving them to
 * {@code WildcardExpressionResolver}: not for convenience, but because the alternative resolves them against the
 * whole namespace.
 *
 * <p><b>What it does not do.</b> With no resolvable tenant, expressions are left exactly as they were — so an
 * unqualified name simply does not resolve, and a fabricated name still works for an operator who types one. It
 * follows that a node serving several tenants with nothing in front of it to set the header is <em>not</em>
 * isolated, and this class cannot make it so: refusing every unqualified request would be a policy decision about
 * unauthenticated callers, which belongs with the auth layer and an {@code ActionFilter}, not with name
 * resolution. Either put an auth layer in front, or give the node {@code clusterless.tenant} and let it serve one.
 *
 * <h2>Why this extension point, with a caveat about it</h2>
 *
 * <p>{@code ClusterPlugin.getIndexNameCustomResolvers()} exists, is unused upstream, and places custom resolvers
 * <em>first</em> — ahead of wildcard and date-math expansion. That ordering is what makes this work at all.
 *
 * <p>It is also a large hole in the abstract: a plugin that silently rewrites index names can send a request
 * anywhere. The reason it is defensible here is that the rewrite is keyed on the tenant, and the tenant is part of
 * every match — so a mapping cannot cross tenants even if the name is ambiguous, and an unresolvable tenant maps
 * nothing at all. Those two properties are what the tests in {@code TenantIndexNameResolverTests} are mostly
 * about.
 */
public final class TenantIndexNameResolver implements IndexNameExpressionResolver.ExpressionResolver {

    private static final Logger logger = LogManager.getLogger(TenantIndexNameResolver.class);

    private final TenantResolver tenants;

    public TenantIndexNameResolver(TenantResolver tenants) {
        this.tenants = Objects.requireNonNull(tenants, "tenants");
    }

    @Override
    public List<String> resolve(IndexNameExpressionResolver.Context context, List<String> expressions) {
        Optional<TenantId> tenant = tenants.current();
        if (tenant.isEmpty()) {
            // Not an error and not a default. See the class comment: an unqualified name failing to resolve is
            // the safe outcome, and a fabricated name typed in full still works.
            return expressions;
        }
        Map<String, String> byFriendlyName = indicesOf(context.getState().metadata(), tenant.get());
        if (byFriendlyName.isEmpty()) {
            // The tenant has no indices on this node. Returning the expressions unchanged would let a name that
            // happens to match another tenant's fabricated index resolve to it, so they are dropped instead --
            // which the server reports as "no indices exist" when the request does not allow that.
            return List.of();
        }

        List<String> resolved = new ArrayList<>(expressions.size());
        for (String expression : expressions) {
            resolve(expression, byFriendlyName, resolved);
        }
        logger.trace("clusterless: resolved {} to {} for tenant [{}]", expressions, resolved, tenant.get());
        return resolved;
    }

    private void resolve(String expression, Map<String, String> byFriendlyName, List<String> resolved) {
        if (expression == null || expression.isEmpty()) {
            return;
        }

        // An exclusion keeps its sign and has its name mapped, so that -my-index excludes the right index rather
        // than nothing. Handled here because the resolvers that would otherwise interpret it run after this one.
        if (expression.charAt(0) == '-' && expression.length() > 1) {
            List<String> excluded = new ArrayList<>();
            resolve(expression.substring(1), byFriendlyName, excluded);
            excluded.forEach(name -> resolved.add("-" + name));
            return;
        }

        if (Metadata.ALL.equals(expression) || "*".equals(expression)) {
            resolved.addAll(byFriendlyName.values());
            return;
        }

        String exact = byFriendlyName.get(expression);
        if (exact != null) {
            resolved.add(exact);
            return;
        }

        if (Regex.isSimpleMatchPattern(expression)) {
            byFriendlyName.forEach((friendly, fabricated) -> {
                if (Regex.simpleMatch(expression, friendly)) {
                    resolved.add(fabricated);
                }
            });
            return;
        }

        // Not one of this tenant's index names. Left alone rather than dropped, because it may be a fabricated
        // name typed in full, an alias, or something a later resolver understands -- and because a name this node
        // has never heard of should produce the server's own "no such index" rather than a silence from here.
        resolved.add(expression);
    }

    /**
     * This tenant's indices, by the name the tenant knows them as.
     *
     * <p>Ordered by the tenant's own name, so that {@code _all} resolves to the same list every time and one a
     * person can read. Not insertion order: {@code Metadata} does not iterate in any order this can rely on, so
     * ordering here rather than inheriting it is the difference between deterministic and accidentally stable.
     */
    private static Map<String, String> indicesOf(Metadata metadata, TenantId tenant) {
        Map<String, String> byFriendlyName = new java.util.TreeMap<>();
        Set<String> ambiguous = new LinkedHashSet<>();
        for (IndexMetadata index : metadata) {
            String owner = ClusterlessIndexSettings.TENANT.get(index.getSettings());
            if (owner == null || owner.equals(tenant.value()) == false) {
                continue;
            }
            String friendly = ClusterlessIndexSettings.TENANT_INDEX_NAME.get(index.getSettings());
            if (friendly == null || friendly.isEmpty()) {
                continue;
            }
            String previous = byFriendlyName.put(friendly, index.getIndex().getName());
            if (previous != null) {
                ambiguous.add(friendly);
            }
        }
        for (String friendly : ambiguous) {
            // One name, two live indices for one tenant: the same name deleted and recreated, with this node still
            // retaining the metadata of the old one so that its files are not destroyed. Resolving it would pick
            // one of them by scan order, and picking the wrong one answers from an index the tenant believes is
            // gone -- so it resolves to neither and says so.
            byFriendlyName.remove(friendly);
            logger.warn(
                "clusterless: tenant [{}] has more than one index named [{}] in this node's state, so the name is "
                    + "ambiguous and will not resolve; use the full index name",
                tenant,
                friendly
            );
        }
        return byFriendlyName;
    }
}
