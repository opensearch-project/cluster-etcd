/*
 * Copyright OpenSearch Software Foundation
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.controlplane.metadata;

import java.util.Set;

/**
 * Which mapping parameters a mapping update may change, and which are fixed once a field exists.
 *
 * <p>Transcribed from OpenSearch 3.9.0's own declarations. Every {@code Parameter} a field mapper
 * declares carries an {@code updateable} flag — {@code ParametrizedFieldMapper.Parameter} turns it into
 * a merge validator, {@code (previous, toMerge) -> updateable || Objects.equals(previous, toMerge)} —
 * so the lists below are that flag, read off every mapper under {@code server}, {@code modules} and
 * {@code plugins} rather than inferred from behaviour. 55 of the 87 declaration sites are immutable.
 *
 * <p>The division is not arbitrary, which is what makes it safe to carry: <b>a parameter is updateable
 * exactly when changing it cannot invalidate data already on disk.</b> {@code meta} is annotation,
 * {@code properties} and {@code fields} are additive, {@code search_analyzer} applies at query time,
 * and {@code ignore_above} / {@code ignore_malformed} / {@code coerce} affect only documents not yet
 * indexed. Everything that decides how bytes were written — {@code index}, {@code doc_values},
 * {@code store}, {@code index_options}, {@code similarity}, {@code normalizer} — is fixed.
 *
 * <h2>Why a static list here and not for settings</h2>
 *
 * <p>The same "hardcoded registry drifts" objection that keeps {@code include_defaults} and processor
 * validation out of this service applies in principle, and is much weaker in practice: this list has
 * been stable for years, it is small, and it is mechanically checkable against the OpenSearch source.
 * A settings registry, by contrast, grows with every plugin. Re-derive with a grep for
 * {@code Parameter.} declarations when the pinned OpenSearch version moves.
 *
 * <p><b>An unrecognised parameter is treated as immutable.</b> It means a field type this list does not
 * know, most likely from a plugin, and refusing the update is the answer that cannot corrupt an index.
 * It is the one place this errs towards a false rejection, deliberately.
 */
public final class MappingParameters {

    /**
     * Updateable on every field type that declares them.
     *
     * <p>{@code type} is deliberately absent even though a mapper declares it updateable: a field's
     * type is guarded by a separate check in {@code FieldMapper.merge} and a change is always rejected,
     * with its own message. See {@link #TYPE}.
     */
    private static final Set<String> UPDATEABLE = Set.of(
        "boost",
        "coerce",
        "eager_global_ordinals",
        "fielddata",
        "fielddata_frequency_filter",
        "fields",
        "ignore_above",
        "ignore_malformed",
        "max_input_length",
        "meta",
        "prefilter_field",
        "properties",
        "script",
        "search_analyzer",
        "search_quote_analyzer",
        "split_queries_on_whitespace",
        "use_similarity"
    );

    /** The field's type. Never updateable, and reported differently from a parameter conflict. */
    public static final String TYPE = "type";

    private MappingParameters() {}

    /**
     * Whether {@code parameter} may be changed on an existing field of type {@code fieldType}.
     *
     * <p>Three parameters disagree between field types, and each is resolved by the type rather than
     * being forced one way:
     *
     * <ul>
     *   <li>{@code analyzer} — fixed for {@code text} and {@code completion} ({@code TextParams},
     *       {@code CompletionFieldMapper}), updateable for {@code token_count}.</li>
     *   <li>{@code format} — fixed for {@code date} and {@code range}, updateable for
     *       {@code derived}.</li>
     *   <li>{@code norms} — updateable for {@code text} ({@code TextParams}), fixed for
     *       {@code match_only_text}.</li>
     * </ul>
     *
     * @param fieldType the field's declared type, or {@code null} when it declares none
     */
    public static boolean isUpdateable(String parameter, String fieldType) {
        return switch (parameter) {
            case TYPE -> false;
            case "analyzer" -> "token_count".equals(fieldType);
            case "format" -> "derived".equals(fieldType);
            case "norms" -> "match_only_text".equals(fieldType) == false;
            // Unrecognised means a field type this list does not know; immutable is the safe answer.
            default -> UPDATEABLE.contains(parameter);
        };
    }
}
