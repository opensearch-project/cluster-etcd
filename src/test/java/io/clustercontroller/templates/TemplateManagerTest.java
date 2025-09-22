package io.clustercontroller.templates;

import io.clustercontroller.store.MetadataStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TemplateManagerTest {

    @Mock
    private MetadataStore metadataStore;

    @InjectMocks
    private TemplateManager templateManager;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void testTemplateExists_NotImplemented() {
        String templateName = "test-template";
        assertThatThrownBy(() -> templateManager.templateExists(templateName))
            .isInstanceOf(UnsupportedOperationException.class)
            .hasMessageContaining("Template existence check not yet implemented");
    }

    @Test
    void testPutTemplate_NotImplemented() {
        String templateName = "test-template";
        String templateConfig = "{\"index_patterns\":[\"logs-*\"]}";

        assertThatThrownBy(() -> templateManager.putTemplate(templateName, templateConfig))
            .isInstanceOf(UnsupportedOperationException.class)
            .hasMessageContaining("Template creation not yet implemented");
    }

    @Test
    void testDeleteTemplate_NotImplemented() {
        String templateName = "test-template";

        assertThatThrownBy(() -> templateManager.deleteTemplate(templateName))
            .isInstanceOf(UnsupportedOperationException.class)
            .hasMessageContaining("Template deletion not yet implemented");
    }

    @Test
    void testGetTemplate_NotImplemented() {
        String templateName = "test-template";
        assertThatThrownBy(() -> templateManager.getTemplate(templateName))
            .isInstanceOf(UnsupportedOperationException.class)
            .hasMessageContaining("Get template not yet implemented");
    }
}
