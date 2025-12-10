package io.clustercontroller.tasks;

/**
 * Interface for executable tasks in the cluster controller.
 * Each task type implements its own execution logic.
 */
public interface Task {
    
    /**
     * Execute the task
     * 
     * @param context TaskContext containing cluster-agnostic services
     * @param clusterId The cluster identifier for this task execution
     * @return Task execution result status
     */
    String execute(TaskContext context, String clusterId);
    
    /**
     * Get task name/identifier
     */
    String getName();
    
    /**
     * Get task priority (0 = highest)
     */
    int getPriority();
    
    /**
     * Get task input data
     */
    String getInput();
    
    /**
     * Get task schedule type
     */
    String getSchedule();
}

