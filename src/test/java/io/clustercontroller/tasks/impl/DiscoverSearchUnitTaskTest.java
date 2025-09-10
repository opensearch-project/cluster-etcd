package io.clustercontroller.tasks.impl;

import io.clustercontroller.discovery.Discovery;
import io.clustercontroller.tasks.TaskContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static io.clustercontroller.config.Constants.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Tests for DiscoverSearchUnitTask.
 */
class DiscoverSearchUnitTaskTest {
    
    @Mock
    private TaskContext taskContext;
    
    @Mock
    private Discovery discovery;
    
    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        when(taskContext.getDiscovery()).thenReturn(discovery);
    }
    
    @Test
    void testDiscoverSearchUnitTaskExecution() throws Exception {
        String taskName = "discover-search-unit-task";
        String input = "";
        DiscoverSearchUnitTask task = new DiscoverSearchUnitTask(taskName, 1, input, TASK_SCHEDULE_REPEAT);
        
        String result = task.execute(taskContext);
        
        assertThat(result).isEqualTo(TASK_STATUS_COMPLETED);
        verify(discovery).discoverSearchUnits();
    }
    
    @Test
    void testDiscoverSearchUnitTaskProperties() {
        String taskName = "discover-search-unit-task";
        String input = "";
        int priority = 2;
        String schedule = TASK_SCHEDULE_REPEAT;
        
        DiscoverSearchUnitTask task = new DiscoverSearchUnitTask(taskName, priority, input, schedule);
        
        assertThat(task.getName()).isEqualTo(taskName);
        assertThat(task.getPriority()).isEqualTo(priority);
        assertThat(task.getInput()).isEqualTo(input);
        assertThat(task.getSchedule()).isEqualTo(schedule);
    }
    
    @Test
    void testDiscoverSearchUnitTaskExecutionFailure() throws Exception {
        String taskName = "discover-search-unit-task";
        String input = "";
        DiscoverSearchUnitTask task = new DiscoverSearchUnitTask(taskName, 1, input, TASK_SCHEDULE_REPEAT);
        
        doThrow(new RuntimeException("Discovery failed")).when(discovery).discoverSearchUnits();
        
        String result = task.execute(taskContext);
        
        assertThat(result).isEqualTo(TASK_STATUS_FAILED);
        verify(discovery).discoverSearchUnits();
    }
}
