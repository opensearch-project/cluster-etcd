package io.clustercontroller.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

import java.util.HashMap;
import java.util.Map;

/**
 * Represents the type mapping for an index.
 * Defines the structure and data types of documents in the index.
 * 
 * Based on OpenSearch's TypeMapping structure which includes field definitions,
 * dynamic settings, runtime fields, and metadata field configurations.
 * 
 * @see <a href="https://opensearch.org/docs/latest/field-types/">OpenSearch Field Types</a>
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class TypeMapping {
    
    /**
     * The properties (field definitions) for the index.
     * Maps field names to their type definitions.
     * 
     * Example:
     * {
     *   "title": { "type": "text" },
     *   "age": { "type": "integer" },
     *   "created_at": { "type": "date" }
     * }
     */
    @JsonProperty("properties")
    private Map<String, Object> properties = new HashMap<>();
    
    /**
     * Dynamic mapping configuration.
     * Controls how new fields are handled when they are not explicitly defined.
     * 
     * Values: "true" (default), "false", "strict"
     * - true: new fields are automatically added to the mapping
     * - false: new fields are ignored
     * - strict: throws an exception if new fields are encountered
     */
    @JsonProperty("dynamic")
    private Object dynamic;
    
    /**
     * Runtime fields that are evaluated at query time.
     * These fields are not indexed but computed on-the-fly.
     */
    @JsonProperty("runtime")
    private Map<String, Object> runtime;
    
    /**
     * Source field configuration.
     * Controls how the original JSON document is stored.
     */
    @JsonProperty("_source")
    private Map<String, Object> source;
    
    /**
     * Routing configuration for the document.
     * Determines which shard a document is stored in.
     */
    @JsonProperty("_routing")
    private Map<String, Object> routing;
    
    /**
     * Metadata field configuration.
     * Additional metadata about the document.
     */
    @JsonProperty("_meta")
    private Map<String, Object> meta;
    
    /**
     * Field names configuration.
     * Controls the _field_names field.
     */
    @JsonProperty("_field_names")
    private Map<String, Object> fieldNames;
    
    /**
     * Date detection configuration.
     * Controls automatic date detection in dynamic mapping.
     */
    @JsonProperty("date_detection")
    private Boolean dateDetection;
    
    /**
     * Numeric detection configuration.
     * Controls automatic numeric detection in dynamic mapping.
     */
    @JsonProperty("numeric_detection")
    private Boolean numericDetection;
    
    /**
     * Dynamic date formats.
     * Formats to use when detecting date fields dynamically.
     */
    @JsonProperty("dynamic_date_formats")
    private Object dynamicDateFormats;
    
    /**
     * Dynamic templates for controlling how new fields are mapped.
     */
    @JsonProperty("dynamic_templates")
    private Object dynamicTemplates;
    
    /**
     * Constructor with properties only (most common use case).
     * 
     * @param properties the field properties/definitions
     */
    public TypeMapping(Map<String, Object> properties) {
        this.properties = properties != null ? properties : new HashMap<>();
    }
    
    /**
     * Adds a field property to the mapping.
     * 
     * @param fieldName the name of the field
     * @param fieldDefinition the field definition (type, analyzer, etc.)
     * @return this TypeMapping instance for method chaining
     */
    public TypeMapping addProperty(String fieldName, Map<String, Object> fieldDefinition) {
        if (this.properties == null) {
            this.properties = new HashMap<>();
        }
        this.properties.put(fieldName, fieldDefinition);
        return this;
    }
    
    /**
     * Adds a simple field with just a type.
     * 
     * @param fieldName the name of the field
     * @param fieldType the type of the field (e.g., "text", "keyword", "integer", "date")
     * @return this TypeMapping instance for method chaining
     */
    public TypeMapping addSimpleProperty(String fieldName, String fieldType) {
        if (this.properties == null) {
            this.properties = new HashMap<>();
        }
        Map<String, Object> fieldDef = new HashMap<>();
        fieldDef.put("type", fieldType);
        this.properties.put(fieldName, fieldDef);
        return this;
    }
}

