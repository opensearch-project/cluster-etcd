package io.clustercontroller.api.models.responses;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class IndexResponseTest {

    private ObjectMapper objectMapper;
    
    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
    }
    
    @Test
    void testCreateSuccess_StaticFactory() {
        // When
        IndexResponse response = IndexResponse.createSuccess("test-index");
        
        // Then
        assertThat(response.isAcknowledged()).isTrue();
        assertThat(response.isShardsAcknowledged()).isTrue();
        assertThat(response.getIndex()).isEqualTo("test-index");
        assertThat(response.getSettings()).isNull();
        assertThat(response.getMappings()).isNull();
    }
    
    @Test
    void testDeleteSuccess_StaticFactory() {
        // When
        IndexResponse response = IndexResponse.deleteSuccess("test-index");
        
        // Then
        assertThat(response.isAcknowledged()).isTrue();
        assertThat(response.isShardsAcknowledged()).isFalse(); // Not set for delete, defaults to false
        assertThat(response.getIndex()).isEqualTo("test-index");
    }
    
    @Test
    void testSerialization_SnakeCaseConversion() throws Exception {
        // Given
        IndexResponse response = IndexResponse.builder()
            .acknowledged(true)
            .shardsAcknowledged(true)
            .index("my-index")
            .settings(Map.of("replicas", 1))
            .build();
        
        // When
        String json = objectMapper.writeValueAsString(response);
        
        // Then
        assertThat(json).contains("\"acknowledged\":true");
        assertThat(json).contains("\"shards_acknowledged\":true"); // Snake case conversion
        assertThat(json).contains("\"index\":\"my-index\"");
        assertThat(json).contains("\"settings\"");
        assertThat(json).doesNotContain("\"mappings\""); // Null excluded by @JsonInclude(NON_EMPTY)
    }
    
    @Test
    void testDeserialization_FromJson() throws Exception {
        // Given
        String json = """
            {
                "acknowledged": true,
                "shards_acknowledged": false,
                "index": "test-index",
                "settings": {"number_of_shards": 2}
            }
            """;
        
        // When
        IndexResponse response = objectMapper.readValue(json, IndexResponse.class);
        
        // Then
        assertThat(response.isAcknowledged()).isTrue();
        assertThat(response.isShardsAcknowledged()).isFalse();
        assertThat(response.getIndex()).isEqualTo("test-index");
        assertThat(response.getSettings()).containsEntry("number_of_shards", 2);
        assertThat(response.getMappings()).isNull();
    }
    
    @Test
    void testBuilder_AllFields() {
        // Given
        Map<String, Object> settings = Map.of("shards", 3);
        Map<String, Object> mappings = Map.of("properties", Map.of());
        
        // When
        IndexResponse response = IndexResponse.builder()
            .acknowledged(true)
            .shardsAcknowledged(false)
            .index("custom-index")
            .settings(settings)
            .mappings(mappings)
            .build();
        
        // Then
        assertThat(response.isAcknowledged()).isTrue();
        assertThat(response.isShardsAcknowledged()).isFalse();
        assertThat(response.getIndex()).isEqualTo("custom-index");
        assertThat(response.getSettings()).isEqualTo(settings);
        assertThat(response.getMappings()).isEqualTo(mappings);
    }
}
