package io.clustercontroller.templates;

import io.clustercontroller.models.Template;
import io.clustercontroller.store.MetadataStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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

    @Test
    void testPutTemplate() throws Exception {
        String templateName = "logs-template";
        String templateConfig = "{\"instance_name\":\"prod-cluster\",\"region\":\"us-west-2\",\"index_template_name\":\"logs-template\",\"index_template_pattern\":\"logs-*\"}";
        
        when(metadataStore.getTemplate(testClusterId, templateName)).thenThrow(new IllegalArgumentException("Template not found"));
        when(metadataStore.createTemplate(eq(testClusterId), eq(templateName), anyString())).thenReturn(templateName);

        templateManager.putTemplate(testClusterId, templateName, templateConfig);

        verify(metadataStore).createTemplate(eq(testClusterId), eq(templateName), anyString());
    }

    @Test
    void testGetTemplate() throws Exception {
        String templateName = "logs-template";
        Template template = new Template();
        template.setIndexTemplateName(templateName);
        template.setIndexTemplatePattern("logs-*");
        template.setInstanceName("prod-cluster");
        template.setRegion("us-west-2");
        
        when(metadataStore.getTemplate(testClusterId, templateName)).thenReturn(template);

        String result = templateManager.getTemplate(testClusterId, templateName);

        assertThat(result).contains("instance_name");
        assertThat(result).contains("logs-*");
        verify(metadataStore).getTemplate(testClusterId, templateName);
    }

    @Test
    void testGetAllTemplates() throws Exception {
        Template template1 = new Template();
        template1.setIndexTemplateName("logs-template");
        template1.setIndexTemplatePattern("logs-*");
        template1.setInstanceName("prod-cluster");
        template1.setRegion("us-west-2");
        
        Template template2 = new Template();
        template2.setIndexTemplateName("metrics-template");
        template2.setIndexTemplatePattern("metrics-*");
        template2.setInstanceName("prod-cluster");
        template2.setRegion("us-east-1");
        
        when(metadataStore.getAllTemplates(testClusterId)).thenReturn(Arrays.asList(template1, template2));

        String result = templateManager.getAllTemplates(testClusterId);

        assertThat(result).contains("index_templates");
        assertThat(result).contains("logs-template");
        assertThat(result).contains("metrics-template");
        verify(metadataStore).getAllTemplates(testClusterId);
    }

    @Test
    void testDeleteTemplate() throws Exception {
        String templateName = "logs-template";
        Template template = new Template();
        template.setIndexTemplateName(templateName);
        template.setIndexTemplatePattern("logs-*");
        template.setInstanceName("prod-cluster");
        template.setRegion("us-west-2");
        
        when(metadataStore.getTemplate(testClusterId, templateName)).thenReturn(template);

        templateManager.deleteTemplate(testClusterId, templateName);

        verify(metadataStore).deleteTemplate(testClusterId, templateName);
    }
}