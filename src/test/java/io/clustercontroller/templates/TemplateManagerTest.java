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
    void testPutTemplate_MissingPattern() {
        String templateName = "test-template";
        String templateConfig = "{\"instance_name\":\"prod-cluster\",\"region\":\"us-west-2\",\"index_template_name\":\"test-template\"}";

        assertThatThrownBy(() -> templateManager.putTemplate(testClusterId, templateName, templateConfig))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Template must have an index pattern");
    }
}