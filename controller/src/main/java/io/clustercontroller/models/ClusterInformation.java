package io.clustercontroller.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Model representing OpenSearch Cluster Information response.
 * 
 * Based on OpenSearch Cluster Information API:
 * https://docs.opensearch.org/latest/api-reference/cluster-api/info/
 * 
 * This is returned by the root endpoint (/) and provides version, build details,
 * and cluster identification information.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ClusterInformation {
    
    /**
     * The name of the node that served the request.
     */
    @JsonProperty("name")
    private String name;
    
    /**
     * The name of the cluster.
     */
    @JsonProperty("cluster_name")
    private String clusterName;
    
    /**
     * The universally unique identifier (UUID) of the cluster.
     */
    @JsonProperty("cluster_uuid")
    private String clusterUuid;
    
    /**
     * Version and build metadata.
     */
    @JsonProperty("version")
    private Version version;
    
    /**
     * The tagline string.
     */
    @JsonProperty("tagline")
    private String tagline = "The OpenSearch Project: https://opensearch.org/";
    
    /**
     * Nested class representing version and build information.
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Version {
        
        /**
         * The distribution identifier, typically "opensearch".
         */
        @JsonProperty("distribution")
        private String distribution;
        
        /**
         * The OpenSearch version number (e.g., "3.2.0").
         */
        @JsonProperty("number")
        private String number;
        
        /**
         * The distribution type (e.g., "tar", "rpm", "deb").
         */
        @JsonProperty("build_type")
        private String buildType;
        
        /**
         * The commit hash the build was created from.
         */
        @JsonProperty("build_hash")
        private String hash;
        
        /**
         * The build timestamp in ISO 8601 format.
         */
        @JsonProperty("build_date")
        private String buildDate;
        
        /**
         * Whether the build is a snapshot build.
         */
        @JsonProperty("build_snapshot")
        private Boolean buildSnapshot;
        
        /**
         * The Lucene version used by this build.
         */
        @JsonProperty("lucene_version")
        private String luceneVersion;
        
        /**
         * The minimum compatible transport protocol version.
         */
        @JsonProperty("minimum_wire_compatibility_version")
        private String minimumWireCompatibilityVersion;
        
        /**
         * The minimum index version that can be read.
         */
        @JsonProperty("minimum_index_compatibility_version")
        private String minimumIndexCompatibilityVersion;
    }
}
