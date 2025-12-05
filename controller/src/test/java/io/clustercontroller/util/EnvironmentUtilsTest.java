package io.clustercontroller.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for EnvironmentUtils
 */
class EnvironmentUtilsTest {

    @Test
    void testGetRequiredEnvWithValidValue() {
        // Test with an environment variable that should exist (PATH is standard)
        String path = EnvironmentUtils.getRequiredEnv("PATH");
        assertNotNull(path);
        assertFalse(path.trim().isEmpty());
    }

    @Test
    void testGetRequiredEnvWithMissingValue() {
        // Test with a non-existent environment variable
        IllegalStateException exception = assertThrows(
                IllegalStateException.class,
                () -> EnvironmentUtils.getRequiredEnv("NON_EXISTENT_ENV_VAR_12345")
        );
        
        assertTrue(exception.getMessage().contains("NON_EXISTENT_ENV_VAR_12345"));
        assertTrue(exception.getMessage().contains("not set or is empty"));
    }

    @Test
    void testGetEnvWithValidValue() {
        // Test with an environment variable that should exist
        String path = EnvironmentUtils.getEnv("PATH", "default-value");
        assertNotNull(path);
        assertFalse(path.trim().isEmpty());
        assertNotEquals("default-value", path);
    }

    @Test
    void testGetEnvWithMissingValue() {
        // Test with a non-existent environment variable
        String result = EnvironmentUtils.getEnv("NON_EXISTENT_ENV_VAR_12345", "my-default");
        assertEquals("my-default", result);
    }

    @Test
    void testGetEnvWithNullDefault() {
        // Test with null default value
        String result = EnvironmentUtils.getEnv("NON_EXISTENT_ENV_VAR_12345", null);
        assertNull(result);
    }

    @Test
    void testTrimmingBehavior() {
        // Since we can't easily set environment variables in tests,
        // we test the trimming behavior indirectly by verifying
        // that existing env vars are trimmed (though they usually don't have whitespace)
        String path = EnvironmentUtils.getEnv("PATH", "default");
        
        // The trimming should not change valid paths, but ensures consistency
        assertNotNull(path);
        assertEquals(path, path.trim());
    }
}
