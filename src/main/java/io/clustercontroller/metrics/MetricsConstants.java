package io.clustercontroller.metrics;

/**
 * Constants for metrics names and tags used in the Cluster Controller.
 */
public class MetricsConstants {
    public final static String ROLLING_UPDATE_PROGRESS_PERCENTAGE_METRIC_NAME = "rolling_update_progress_percentage";
    public final static String ROLLING_UPDATE_TRANSIT_NODES_PERCENTAGE_METRIC_NAME = "rolling_update_transit_nodes_percentage";
    public final static String PLANNED_INGEST_SU_ALLOCATION_METRIC_NAME = "planned_ingest_su_allocation";
    public final static String PLANNED_SEARCH_SU_ALLOCATION_METRIC_NAME = "planned_search_su_allocation";
    public final static String UPDATE_ACTUAL_ALLOCATION_FAILURES_METRIC_NAME = "update_actual_allocation_failures_count";
    public final static String DISCOVERY_CLEANED_STALE_SEARCH_UNITS_COUNT_METRIC_NAME = "discovery_cleaned_stale_search_units_count";
    public final static String CLUSTER_ID_TAG = "clusterId";
    public final static String INDEX_NAME_TAG = "indexName";
    public final static String SHARD_ID_TAG = "shardId";
    public final static String ROLE_TAG = "role";

    private MetricsConstants() {}
}
