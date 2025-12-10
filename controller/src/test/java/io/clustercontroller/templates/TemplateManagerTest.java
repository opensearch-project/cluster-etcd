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
        String templateConfig = "{\"instance_name\":\"prod-cluster\",\"region\":\"us-west-2\"}";

        assertThatThrownBy(() -> templateManager.putTemplate(testClusterId, templateName, templateConfig))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Template must have at least one index pattern");
    }

    @Test
    void testPutTemplate() throws Exception {
        String templateName = "logs-template";
        String templateConfig = "{\"index_patterns\":[\"logs-*\"],\"priority\":100,\"template\":{\"settings\":{\"number_of_shards\":3}},\"instance_name\":\"prod-cluster\",\"region\":\"us-west-2\"}";
        
        when(metadataStore.getTemplate(testClusterId, templateName)).thenThrow(new IllegalArgumentException("Template not found"));
        when(metadataStore.createTemplate(eq(testClusterId), eq(templateName), anyString())).thenReturn(templateName);

        templateManager.putTemplate(testClusterId, templateName, templateConfig);

        verify(metadataStore).createTemplate(eq(testClusterId), eq(templateName), anyString());
    }

    @Test
    void testGetTemplate() throws Exception {
        String templateName = "logs-template";
        Template template = new Template();
        template.setIndexPatterns(Arrays.asList("logs-*"));
        template.setPriority(100);
        template.setInstanceName("prod-cluster");
        template.setRegion("us-west-2");
        
        Template.TemplateDefinition templateDef = new Template.TemplateDefinition();
        templateDef.setSettings(java.util.Map.of("number_of_shards", 3));
        template.setTemplate(templateDef);
        
        when(metadataStore.getTemplate(testClusterId, templateName)).thenReturn(template);

        String result = templateManager.getTemplate(testClusterId, templateName);

        assertThat(result).contains("instance_name");
        assertThat(result).contains("logs-*");
        assertThat(result).contains("index_patterns");
        verify(metadataStore).getTemplate(testClusterId, templateName);
    }

    @Test
    void testGetAllTemplates() throws Exception {
        Template template1 = new Template();
        template1.setIndexPatterns(Arrays.asList("logs-*"));
        template1.setPriority(100);
        template1.setInstanceName("prod-cluster");
        template1.setRegion("us-west-2");
        
        Template template2 = new Template();
        template2.setIndexPatterns(Arrays.asList("metrics-*"));
        template2.setPriority(50);
        template2.setInstanceName("prod-cluster");
        template2.setRegion("us-east-1");
        
        when(metadataStore.getAllTemplates(testClusterId)).thenReturn(Arrays.asList(template1, template2));

        String result = templateManager.getAllTemplates(testClusterId);

        assertThat(result).contains("index_templates");
        assertThat(result).contains("logs-*");
        assertThat(result).contains("metrics-*");
        verify(metadataStore).getAllTemplates(testClusterId);
    }

    @Test
    void testDeleteTemplate() throws Exception {
        String templateName = "logs-template";
        Template template = new Template();
        template.setIndexPatterns(Arrays.asList("logs-*"));
        template.setPriority(100);
        template.setInstanceName("prod-cluster");
        template.setRegion("us-west-2");
        
        when(metadataStore.getTemplate(testClusterId, templateName)).thenReturn(template);

        templateManager.deleteTemplate(testClusterId, templateName);

        verify(metadataStore).deleteTemplate(testClusterId, templateName);
    }
}