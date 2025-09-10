package io.clustercontroller.util;

/**
 * Utility class for environment variable operations
 */
public final class EnvironmentUtils {
    
    private EnvironmentUtils() {
        // Utility class - prevent instantiation
    }
    
    /**
     * Get required environment variable - throws exception if not set
     * 
     * @param name the environment variable name
     * @return the trimmed environment variable value
     * @throws IllegalStateException if the environment variable is not set or is empty
     */
    public static String getRequiredEnv(String name) {
        String value = System.getenv(name);
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalStateException("Required environment variable '" + name + "' is not set or is empty");
        }
        return value.trim();
    }
    
    /**
     * Get environment variable with default value
     * 
     * @param name the environment variable name
     * @param defaultValue the default value to return if not set
     * @return the environment variable value or default if not set
     */
    public static String getEnv(String name, String defaultValue) {
        String value = System.getenv(name);
        return value != null ? value.trim() : defaultValue;
    }
}
