package io.clustercontroller.templates;

import io.clustercontroller.store.MetadataStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TemplateManagerTest {

    @Mock
    private MetadataStore metadataStore;

    @InjectMocks
    private TemplateManager templateManager;
    
    private final String testClusterId = "test-cluster";

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void testTemplateExists_NotImplemented() {
        String templateName = "test-template";
        assertThatThrownBy(() -> templateManager.templateExists(testClusterId, templateName))
            .isInstanceOf(UnsupportedOperationException.class)
            .hasMessageContaining("Template existence check not yet implemented");
    }

    @Test
    void testPutTemplate_NotImplemented() {
        String templateName = "test-template";
        String templateConfig = "{\"index_patterns\":[\"logs-*\"]}";

        assertThatThrownBy(() -> templateManager.putTemplate(testClusterId, templateName, templateConfig))
            .isInstanceOf(UnsupportedOperationException.class)
            .hasMessageContaining("Template creation not yet implemented");
    }

    @Test
    void testDeleteTemplate_NotImplemented() {
        String templateName = "test-template";

        assertThatThrownBy(() -> templateManager.deleteTemplate(testClusterId, templateName))
            .isInstanceOf(UnsupportedOperationException.class)
            .hasMessageContaining("Template deletion not yet implemented");
    }

    @Test
    void testGetTemplate_NotImplemented() {
        String templateName = "test-template";
        assertThatThrownBy(() -> templateManager.getTemplate(testClusterId, templateName))
            .isInstanceOf(UnsupportedOperationException.class)
            .hasMessageContaining("Get template not yet implemented");
    }

    @Test
    void testGetAllTemplates_NotImplemented() {
        assertThatThrownBy(() -> templateManager.getAllTemplates(testClusterId))
            .isInstanceOf(UnsupportedOperationException.class)
            .hasMessageContaining("Get all templates not yet implemented");
    }
}