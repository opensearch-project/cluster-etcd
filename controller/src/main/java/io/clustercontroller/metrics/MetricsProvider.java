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

    @Autowired
    public MetricsProvider(
        MeterRegistry registry,
        @Value("${controller.id}") String controllerId) {
        this.registry = registry;
        this.hostname = controllerId;
        log.info("MetricsProvider initialized for the controller: {}", hostname);
    }

    /**
     * Create or retrieve a Counter metric with the given name and tags.
     *
     * @param name the name of the counter
     * @param tags a map of tag keys to tag values
     * @return the Counter instance
     */
    public Counter counter(String name, Map<String, String> tags) {
        return Counter.builder(name).tags(mapToTagArray(tags)).register(registry);
    }

    /**
     * Create a Gauge metric with the given name, initial value, and tags.
     *
     * @param name the name of the gauge
     * @param tags a map of tag keys to tag values
     * @return the AtomicDouble instance representing the gauge value
     */
    public AtomicDouble gauge(String name, Map<String, String> tags) {
        AtomicDouble myGaugeValue = new AtomicDouble(0);
        Gauge.builder(name, myGaugeValue::get).tags(mapToTagArray(tags)).register(registry);
        return myGaugeValue;
    }

    /**
     * Create or retrieve a Timer metric with the given name and tags.
     *
     * @param name the name of the timer
     * @param tags a map of tag keys to tag values
     * @return the Timer instance
     */
    public Timer timer(String name, Map<String, String> tags) {
        return Timer.builder(name)
            .tags(mapToTagArray(tags))
            .publishPercentileHistogram()
            .publishPercentiles(TIMER_PERCENTILES)
            .register(registry);
    }

    /**
     * Convert a map of tags to an array of alternating keys and values, including hostname.
     *
     * @param tags the map of tags
     * @return array of alternating keys and values
     */
    private String[] mapToTagArray(Map<String, String> tags) {
        String[] tagArray = new String[(tags.size() + 1) * 2];    
        int index = 0;
        for (Map.Entry<String, String> entry : tags.entrySet()) {
            tagArray[index++] = entry.getKey();
            tagArray[index++] = entry.getValue();
        }
        tagArray[index++] = HOST_NAME_TAG;
        tagArray[index] = hostname;
        return tagArray;
    }
}

