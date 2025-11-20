package io.clustercontroller.metrics;


import com.google.common.util.concurrent.AtomicDouble;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/*
 * MetricsProvider is a utility class for creating and managing various types of metrics
 * such as counters, gauges, and timers.
 */
@Component
@Slf4j
public class MetricsProvider {
    private static final double[] TIMER_PERCENTILES = {0.5, 0.9, 0.99};
    
    private final MeterRegistry registry;

    @Autowired
    public MetricsProvider(MeterRegistry registry) {
        this.registry = registry;
        log.info("MetricsProvider initialized");
    }

    /**
     * Create or retrieve a Counter metric with the given name.
     *
     * @param name the name of the counter
     * @return the Counter instance
     */
    public Counter counter(String name) {
        return Counter.builder(name).register(registry);
    }

    /**
     * Create a Gauge metric with the given name and initial value.
     *
     * @param name the name of the gauge
     * @return the AtomicDouble instance representing the gauge value
     */
    public AtomicDouble gauge(String name) {
        AtomicDouble myGaugeValue = new AtomicDouble(0);
        Gauge.builder(name, myGaugeValue::get).register(registry);
        return myGaugeValue;
    }

    /**
     * Create or retrieve a Timer metric with the given name.
     *
     * @param name the name of the timer
     * @return the Timer instance
     */
    public Timer timer(String name) {
        return Timer.builder(name)
            .publishPercentileHistogram()
            .publishPercentiles(TIMER_PERCENTILES)
            .register(registry);
    }
}

