package io.clustercontroller.api.models.requests;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class IndexRequestTest {

    private ObjectMapper objectMapper;
    
    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
    }
    
    @Test
    void testSerialization_WithAllFields() throws Exception {
        // Given
        IndexRequest request = IndexRequest.builder()
            .settings(Map.of("number_of_replicas", 1, "refresh_interval", "30s"))
            .mappings(Map.of("properties", Map.of("title", Map.of("type", "text"))))
            .aliases(Map.of("my_alias", Map.of()))
            .build();
        
        // When
        String json = objectMapper.writeValueAsString(request);
        
        // Then
        assertThat(json).contains("\"settings\"");
        assertThat(json).contains("\"mappings\"");
        assertThat(json).contains("\"aliases\"");
        assertThat(json).contains("\"number_of_replicas\"");
        assertThat(json).contains("\"refresh_interval\"");
    }
    
    @Test
    void testSerialization_WithEmptyFields() throws Exception {
        // Given
        IndexRequest request = IndexRequest.builder()
            .settings(Map.of("replicas", 1))
            .build();
        
        // When
        String json = objectMapper.writeValueAsString(request);
        
        // Then
        assertThat(json).contains("\"settings\"");
        assertThat(json).doesNotContain("\"mappings\""); // Empty maps excluded by @JsonInclude(NON_EMPTY)
        assertThat(json).doesNotContain("\"aliases\"");
    }
    
    @Test
    void testDeserialization_FromJson() throws Exception {
        // Given
        String json = """
            {
                "settings": {"number_of_replicas": 2},
                "mappings": {"properties": {"field1": {"type": "keyword"}}},
                "unknown_field": "should_be_ignored"
            }
            """;
        
        // When
        IndexRequest request = objectMapper.readValue(json, IndexRequest.class);
        
        // Then
        assertThat(request.getSettings()).containsEntry("number_of_replicas", 2);
        assertThat(request.getMappings()).containsKey("properties");
        assertThat(request.getAliases()).isNull(); // Not provided in JSON
        // unknown_field is ignored due to @JsonIgnoreProperties(ignoreUnknown = true)
    }
    
    @Test
    void testBuilder_DefaultValues() {
        // When
        IndexRequest request = IndexRequest.builder().build();
        
        // Then
        assertThat(request.getSettings()).isNull();
        assertThat(request.getMappings()).isNull();
        assertThat(request.getAliases()).isNull();
    }
    
    @Test
    void testBuilder_WithValues() {
        // Given
        Map<String, Object> settings = Map.of("shards", 3);
        Map<String, Object> mappings = Map.of("properties", Map.of());
        
        // When
        IndexRequest request = IndexRequest.builder()
            .settings(settings)
            .mappings(mappings)
            .build();
        
        // Then
        assertThat(request.getSettings()).isEqualTo(settings);
        assertThat(request.getMappings()).isEqualTo(mappings);
        assertThat(request.getAliases()).isNull();
    }
}
