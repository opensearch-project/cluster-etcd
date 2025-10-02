package io.clustercontroller.tasks.impl;

import io.clustercontroller.orchestration.GoalStateOrchestrator;
import io.clustercontroller.tasks.TaskContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static io.clustercontroller.config.Constants.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class GoalStateOrchestratorTaskTest {

    @Mock
    private TaskContext taskContext;

    @Mock
    private GoalStateOrchestrator goalStateOrchestrator;

    private GoalStateOrchestratorTask task;

    @BeforeEach
    void setUp() {
        task = new GoalStateOrchestratorTask("test-task", 1, "test-input", "repeat");
    }

    @Test
    void testExecuteSuccess() throws Exception {
        // Given
        String clusterId = "test-cluster";
        when(taskContext.getClusterName()).thenReturn(clusterId);
        when(taskContext.getGoalStateOrchestrator()).thenReturn(goalStateOrchestrator);

        // When
        String result = task.execute(taskContext);

        // Then
        assertThat(result).isEqualTo(TASK_STATUS_COMPLETED);
        verify(taskContext).getClusterName();
        verify(taskContext).getGoalStateOrchestrator();
        verify(goalStateOrchestrator).orchestrateGoalStates(clusterId);
    }

    @Test
    void testExecuteWithOrchestratorException() throws Exception {
        // Given
        String clusterId = "test-cluster";
        when(taskContext.getClusterName()).thenReturn(clusterId);
        when(taskContext.getGoalStateOrchestrator()).thenReturn(goalStateOrchestrator);
        doThrow(new RuntimeException("Orchestration error")).when(goalStateOrchestrator).orchestrateGoalStates(clusterId);

        // When
        String result = task.execute(taskContext);

        // Then
        assertThat(result).isEqualTo(TASK_STATUS_FAILED);
        verify(taskContext).getClusterName();
        verify(taskContext).getGoalStateOrchestrator();
        verify(goalStateOrchestrator).orchestrateGoalStates(clusterId);
    }

    @Test
    void testExecuteWithContextException() {
        // Given
        when(taskContext.getClusterName()).thenThrow(new RuntimeException("Context error"));

        // When
        String result = task.execute(taskContext);

        // Then
        assertThat(result).isEqualTo(TASK_STATUS_FAILED);
        verify(taskContext).getClusterName();
        verify(taskContext, never()).getGoalStateOrchestrator();
        verify(goalStateOrchestrator, never()).orchestrateGoalStates(anyString());
    }

    @Test
    void testExecuteWithNullClusterName() throws Exception {
        // Given
        when(taskContext.getClusterName()).thenReturn(null);
        when(taskContext.getGoalStateOrchestrator()).thenReturn(goalStateOrchestrator);

        // When
        String result = task.execute(taskContext);

        // Then
        assertThat(result).isEqualTo(TASK_STATUS_COMPLETED);
        verify(taskContext).getClusterName();
        verify(taskContext).getGoalStateOrchestrator();
        verify(goalStateOrchestrator).orchestrateGoalStates(null);
    }

    @Test
    void testExecuteWithEmptyClusterName() throws Exception {
        // Given
        when(taskContext.getClusterName()).thenReturn("");
        when(taskContext.getGoalStateOrchestrator()).thenReturn(goalStateOrchestrator);

        // When
        String result = task.execute(taskContext);

        // Then
        assertThat(result).isEqualTo(TASK_STATUS_COMPLETED);
        verify(taskContext).getClusterName();
        verify(taskContext).getGoalStateOrchestrator();
        verify(goalStateOrchestrator).orchestrateGoalStates("");
    }

    @Test
    void testExecuteWithInterruptedException() throws Exception {
        // Given
        String clusterId = "test-cluster";
        when(taskContext.getClusterName()).thenReturn(clusterId);
        when(taskContext.getGoalStateOrchestrator()).thenReturn(goalStateOrchestrator);
        doThrow(new RuntimeException("Interrupted")).when(goalStateOrchestrator).orchestrateGoalStates(clusterId);

        // When
        String result = task.execute(taskContext);

        // Then
        assertThat(result).isEqualTo(TASK_STATUS_FAILED);
        verify(taskContext).getClusterName();
        verify(taskContext).getGoalStateOrchestrator();
        verify(goalStateOrchestrator).orchestrateGoalStates(clusterId);
    }

    @Test
    void testExecuteWithCheckedException() throws Exception {
        // Given
        String clusterId = "test-cluster";
        when(taskContext.getClusterName()).thenReturn(clusterId);
        when(taskContext.getGoalStateOrchestrator()).thenReturn(goalStateOrchestrator);
        doThrow(new RuntimeException("Checked exception")).when(goalStateOrchestrator).orchestrateGoalStates(clusterId);

        // When
        String result = task.execute(taskContext);

        // Then
        assertThat(result).isEqualTo(TASK_STATUS_FAILED);
        verify(taskContext).getClusterName();
        verify(taskContext).getGoalStateOrchestrator();
        verify(goalStateOrchestrator).orchestrateGoalStates(clusterId);
    }

    @Test
    void testExecuteWithError() throws Exception {
        // Given
        String clusterId = "test-cluster";
        when(taskContext.getClusterName()).thenReturn(clusterId);
        when(taskContext.getGoalStateOrchestrator()).thenReturn(goalStateOrchestrator);
        doThrow(new Error("Fatal error")).when(goalStateOrchestrator).orchestrateGoalStates(clusterId);

        // When & Then
        // Error should propagate up (not caught by Exception handler)
        assertThatThrownBy(() -> task.execute(taskContext))
                .isInstanceOf(Error.class)
                .hasMessage("Fatal error");
        
        verify(taskContext).getClusterName();
        verify(taskContext).getGoalStateOrchestrator();
        verify(goalStateOrchestrator).orchestrateGoalStates(clusterId);
    }

    @Test
    void testExecuteWithNullTaskContext() {
        // Given
        TaskContext nullContext = null;

        // When
        String result = task.execute(nullContext);

        // Then
        assertThat(result).isEqualTo(TASK_STATUS_FAILED);
        verify(goalStateOrchestrator, never()).orchestrateGoalStates(anyString());
    }

    @Test
    void testExecuteWithNullGoalStateOrchestrator() {
        // Given
        String clusterId = "test-cluster";
        when(taskContext.getClusterName()).thenReturn(clusterId);
        when(taskContext.getGoalStateOrchestrator()).thenReturn(null);

        // When
        String result = task.execute(taskContext);

        // Then
        assertThat(result).isEqualTo(TASK_STATUS_FAILED);
        verify(taskContext).getClusterName();
        verify(taskContext).getGoalStateOrchestrator();
    }

    @Test
    void testGetName() {
        // When
        String name = task.getName();

        // Then
        assertThat(name).isEqualTo("test-task");
    }

    @Test
    void testGetPriority() {
        // When
        int priority = task.getPriority();

        // Then
        assertThat(priority).isEqualTo(1);
    }

    @Test
    void testGetInput() {
        // When
        String input = task.getInput();

        // Then
        assertThat(input).isEqualTo("test-input");
    }

    @Test
    void testGetSchedule() {
        // When
        String schedule = task.getSchedule();

        // Then
        assertThat(schedule).isEqualTo("repeat");
    }

    @Test
    void testExecuteMultipleTimes() throws Exception {
        // Given
        String clusterId = "test-cluster";
        when(taskContext.getClusterName()).thenReturn(clusterId);
        when(taskContext.getGoalStateOrchestrator()).thenReturn(goalStateOrchestrator);

        // When
        String result1 = task.execute(taskContext);
        String result2 = task.execute(taskContext);
        String result3 = task.execute(taskContext);

        // Then
        assertThat(result1).isEqualTo(TASK_STATUS_COMPLETED);
        assertThat(result2).isEqualTo(TASK_STATUS_COMPLETED);
        assertThat(result3).isEqualTo(TASK_STATUS_COMPLETED);
        
        verify(taskContext, times(3)).getClusterName();
        verify(taskContext, times(3)).getGoalStateOrchestrator();
        verify(goalStateOrchestrator, times(3)).orchestrateGoalStates(clusterId);
    }

    @Test
    void testExecuteWithDifferentClusterIds() throws Exception {
        // Given
        String clusterId1 = "cluster1";
        String clusterId2 = "cluster2";
        
        when(taskContext.getClusterName()).thenReturn(clusterId1).thenReturn(clusterId2);
        when(taskContext.getGoalStateOrchestrator()).thenReturn(goalStateOrchestrator);

        // When
        String result1 = task.execute(taskContext);
        String result2 = task.execute(taskContext);

        // Then
        assertThat(result1).isEqualTo(TASK_STATUS_COMPLETED);
        assertThat(result2).isEqualTo(TASK_STATUS_COMPLETED);
        
        verify(taskContext, times(2)).getClusterName();
        verify(taskContext, times(2)).getGoalStateOrchestrator();
        verify(goalStateOrchestrator).orchestrateGoalStates(clusterId1);
        verify(goalStateOrchestrator).orchestrateGoalStates(clusterId2);
    }
}
