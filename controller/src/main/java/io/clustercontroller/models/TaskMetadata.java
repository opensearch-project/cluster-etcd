package io.clustercontroller.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.time.OffsetDateTime;

import static io.clustercontroller.config.Constants.*;

/**
 * Task metadata representing task state and information stored in metadata store.
 */
@Data
public class TaskMetadata {
    
    @JsonProperty("name")
    private String name;
    
    @JsonProperty("status")
    private String status;
    
    @JsonProperty("priority")
    private int priority; // 0 = highest priority
    
    @JsonProperty("schedule")
    private String schedule;
    
    @JsonProperty("input")
    private String input;
    
    @JsonProperty("output")
    private String output;
    
    @JsonProperty("last_updated")
    private OffsetDateTime lastUpdated;
    
    @JsonProperty("created_at")
    private OffsetDateTime createdAt;
    
    public TaskMetadata() {
        this.createdAt = OffsetDateTime.now();
        this.lastUpdated = OffsetDateTime.now();
        this.status = TASK_STATUS_PENDING;
    }
    
    public TaskMetadata(String name, int priority) {
        this();
        this.name = name;
        this.priority = priority;
    }
}

