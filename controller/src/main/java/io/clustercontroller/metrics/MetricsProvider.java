package io.clustercontroller.metrics;

import com.google.common.util.concurrent.AtomicDouble;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/*
 * MetricsProvider is a utility class for creating and managing various types of metrics
 * such as counters, gauges, and timers.
 */
@Component
@Slf4j
public class MetricsProvider {
    private static final double[] TIMER_PERCENTILES = {0.5, 0.9, 0.99};
    private static final String HOST_NAME_TAG = "hostname";

    private final MeterRegistry registry;
    private final String hostname;
    private final Map<String, AtomicDouble> gaugeCache = new ConcurrentHashMap<>();

    @Autowired
    public MetricsProvider(
        MeterRegistry registry,
        @Value("${controller.id}") String controllerId) {
        this.registry = registry;
        this.hostname = controllerId;
        log.info("MetricsProvider initialized for the controller: {}", hostname);
    }

    /**
     * Creates or retrieves a Counter metric with the given name and tags.
     *
     * @param name the name of the counter
     * @param tags a map of tag keys to tag values
     * @return the Counter instance
     */
    public Counter counter(String name, Map<String, String> tags) {
        tags.put(HOST_NAME_TAG, hostname);
        return Counter.builder(name).tags(mapToTagArray(tags)).register(registry);
    }

    /**
     * Gets or creates a Gauge metric that can be updated.
     * Returns the same AtomicDouble for identical name+tags combinations.
     * This allows updating the gauge value by calling .set() on the returned AtomicDouble.
     *
     * @param name the name of the gauge
     * @param value the value of the gauge
     * @param tags a map of tag keys to tag values
     * @return the AtomicDouble instance representing the gauge value
     */
    public AtomicDouble gauge(String name, double value, Map<String, String> tags) {
        tags.put(HOST_NAME_TAG, hostname);
        String cacheKey = buildCacheKey(name, tags);
        AtomicDouble gauge = gaugeCache.computeIfAbsent(cacheKey, k -> {
            AtomicDouble gaugeValue = new AtomicDouble(value);
            Gauge.builder(name, gaugeValue::get)
                .tags(mapToTagArray(tags))
                .register(registry);
            return gaugeValue;
        });
        gauge.set(value);
        return gauge;
    }

    /**
     * Creates or retrieves a Timer metric with the given name and tags.
     *
     * @param name the name of the timer
     * @param tags a map of tag keys to tag values
     * @return the Timer instance
     */
    public Timer timer(String name, Map<String, String> tags) {
        tags.put(HOST_NAME_TAG, hostname);
        return Timer.builder(name)
            .tags(mapToTagArray(tags))
            .publishPercentileHistogram()
            .publishPercentiles(TIMER_PERCENTILES)
            .register(registry);
    }

    /**
     * Builds a cache key from metric name and tags for gauge reuse.
     *
     * @param name the metric name
     * @param tags the metric tags
     * @return a unique cache key
     */
    private String buildCacheKey(String name, Map<String, String> tags) {
        StringBuilder key = new StringBuilder(name);
        tags.entrySet().stream()
            .sorted(Map.Entry.comparingByKey())
            .forEach(e -> key.append(":").append(e.getKey()).append("=").append(e.getValue()));
        return key.toString();
    }

    /**
     * Converts a map of tags to an array of alternating keys and values, including hostname.
     *
     * @param tags the map of tags
     * @return array of alternating keys and values
     */
    private String[] mapToTagArray(Map<String, String> tags) {
        String[] tagArray = new String[tags.size() * 2];
        int index = 0;
        for (Map.Entry<String, String> entry : tags.entrySet()) {
            tagArray[index++] = entry.getKey();
            tagArray[index++] = entry.getValue();
        }
        return tagArray;
    }
}

