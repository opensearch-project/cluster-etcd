package io.clustercontroller.proxy;

import io.clustercontroller.models.SearchUnit;
import io.clustercontroller.models.SearchUnitActualState;
import io.clustercontroller.store.MetadataStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

class CoordinatorSelectorTest {

    @Mock
    private MetadataStore metadataStore;

    private CoordinatorSelector coordinatorSelector;

    private final String testClusterId = "test-cluster";

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        coordinatorSelector = new CoordinatorSelector(metadataStore);
    }

    @Test
    void testSelectCoordinator_Success() throws Exception {
        // Given: Two healthy coordinators
        SearchUnit coord1 = createSearchUnit("coord-1", "COORDINATOR", "10.0.0.1", testClusterId);
        SearchUnit coord2 = createSearchUnit("coord-2", "COORDINATOR", "10.0.0.2", testClusterId);
        SearchUnit dataNode = createSearchUnit("data-1", "PRIMARY", "10.0.0.3", testClusterId);

        List<SearchUnit> allUnits = Arrays.asList(coord1, coord2, dataNode);

        SearchUnitActualState coord1State = createHealthyState("coord-1", System.currentTimeMillis());
        SearchUnitActualState coord2State = createHealthyState("coord-2", System.currentTimeMillis());

        when(metadataStore.getAllSearchUnits(testClusterId)).thenReturn(allUnits);
        when(metadataStore.getSearchUnitActualState(testClusterId, "coord-1")).thenReturn(coord1State);
        when(metadataStore.getSearchUnitActualState(testClusterId, "coord-2")).thenReturn(coord2State);

        // When
        SearchUnit selected = coordinatorSelector.selectCoordinator(testClusterId);

        // Then
        assertThat(selected).isNotNull();
        assertThat(selected.getRole()).isEqualTo("COORDINATOR");
        assertThat(selected.getName()).isIn("coord-1", "coord-2");
    }

    @Test
    void testSelectCoordinator_RoundRobin() throws Exception {
        // Given: Two healthy coordinators
        SearchUnit coord1 = createSearchUnit("coord-1", "COORDINATOR", "10.0.0.1", testClusterId);
        SearchUnit coord2 = createSearchUnit("coord-2", "COORDINATOR", "10.0.0.2", testClusterId);

        List<SearchUnit> allUnits = Arrays.asList(coord1, coord2);

        SearchUnitActualState coord1State = createHealthyState("coord-1", System.currentTimeMillis());
        SearchUnitActualState coord2State = createHealthyState("coord-2", System.currentTimeMillis());

        when(metadataStore.getAllSearchUnits(testClusterId)).thenReturn(allUnits);
        when(metadataStore.getSearchUnitActualState(testClusterId, "coord-1")).thenReturn(coord1State);
        when(metadataStore.getSearchUnitActualState(testClusterId, "coord-2")).thenReturn(coord2State);

        // When: Select multiple times
        SearchUnit first = coordinatorSelector.selectCoordinator(testClusterId);
        SearchUnit second = coordinatorSelector.selectCoordinator(testClusterId);
        SearchUnit third = coordinatorSelector.selectCoordinator(testClusterId);

        // Then: Should round-robin between coordinators
        assertThat(first.getName()).isEqualTo("coord-1");
        assertThat(second.getName()).isEqualTo("coord-2");
        assertThat(third.getName()).isEqualTo("coord-1"); // Back to first
    }

    @Test
    void testSelectCoordinator_FiltersUnhealthyCoordinators() throws Exception {
        // Given: One healthy, one unhealthy coordinator
        SearchUnit coord1 = createSearchUnit("coord-1", "COORDINATOR", "10.0.0.1", testClusterId);
        SearchUnit coord2 = createSearchUnit("coord-2", "COORDINATOR", "10.0.0.2", testClusterId);

        List<SearchUnit> allUnits = Arrays.asList(coord1, coord2);

        // coord-1 is healthy (recent heartbeat)
        SearchUnitActualState coord1State = createHealthyState("coord-1", System.currentTimeMillis());
        
        // coord-2 is unhealthy (stale heartbeat - 60 seconds old)
        SearchUnitActualState coord2State = createHealthyState("coord-2", System.currentTimeMillis() - 60000);

        when(metadataStore.getAllSearchUnits(testClusterId)).thenReturn(allUnits);
        when(metadataStore.getSearchUnitActualState(testClusterId, "coord-1")).thenReturn(coord1State);
        when(metadataStore.getSearchUnitActualState(testClusterId, "coord-2")).thenReturn(coord2State);

        // When
        SearchUnit selected = coordinatorSelector.selectCoordinator(testClusterId);

        // Then: Should only select healthy coordinator
        assertThat(selected).isNotNull();
        assertThat(selected.getName()).isEqualTo("coord-1");
    }

    @Test
    void testSelectCoordinator_NoCoordinatorsFound() throws Exception {
        // Given: Only data nodes, no coordinators
        SearchUnit dataNode = createSearchUnit("data-1", "PRIMARY", "10.0.0.3", testClusterId);

        when(metadataStore.getAllSearchUnits(testClusterId)).thenReturn(Collections.singletonList(dataNode));

        // When & Then
        assertThatThrownBy(() -> coordinatorSelector.selectCoordinator(testClusterId))
            .isInstanceOf(Exception.class)
            .hasMessageContaining("No coordinator nodes found");
    }

    @Test
    void testSelectCoordinator_NoHealthyCoordinators() throws Exception {
        // Given: Coordinators exist but all are unhealthy
        SearchUnit coord1 = createSearchUnit("coord-1", "COORDINATOR", "10.0.0.1", testClusterId);

        List<SearchUnit> allUnits = Collections.singletonList(coord1);

        // Unhealthy state (stale heartbeat)
        SearchUnitActualState coord1State = createHealthyState("coord-1", System.currentTimeMillis() - 60000);

        when(metadataStore.getAllSearchUnits(testClusterId)).thenReturn(allUnits);
        when(metadataStore.getSearchUnitActualState(testClusterId, "coord-1")).thenReturn(coord1State);

        // When & Then
        assertThatThrownBy(() -> coordinatorSelector.selectCoordinator(testClusterId))
            .isInstanceOf(Exception.class)
            .hasMessageContaining("No healthy coordinator nodes found");
    }

    @Test
    void testBuildCoordinatorUrl() {
        // Given
        SearchUnit coordinator = createSearchUnit("coord-1", "COORDINATOR", "10.0.0.1", testClusterId);
        coordinator.setPortHttp(9200);

        // When
        String url = coordinatorSelector.buildCoordinatorUrl(coordinator);

        // Then
        assertThat(url).isEqualTo("http://10.0.0.1:9200");
    }

    // Helper methods

    private SearchUnit createSearchUnit(String name, String role, String host, String clusterName) {
        SearchUnit unit = new SearchUnit();
        unit.setName(name);
        unit.setRole(role);
        unit.setHost(host);
        unit.setClusterName(clusterName);
        unit.setPortHttp(9200);
        unit.setPortTransport(9300);
        return unit;
    }

    private SearchUnitActualState createHealthyState(String nodeName, long timestamp) {
        SearchUnitActualState state = new SearchUnitActualState();
        state.setNodeName(nodeName);
        state.setTimestamp(timestamp);
        state.setMemoryUsedPercent(50); // Below threshold
        state.setDiskAvailableMB(10000); // Above threshold
        return state;
    }
}

