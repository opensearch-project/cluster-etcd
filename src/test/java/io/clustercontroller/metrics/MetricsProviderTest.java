package io.clustercontroller.metrics;

import com.google.common.util.concurrent.AtomicDouble;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.*;

@ExtendWith(MockitoExtension.class)
class MetricsProviderTest {

    @Mock
    private MeterRegistry mockRegistry;

    private static final String TEST_CONTROLLER_ID = "test-controller-01";

    @Test
    void testConstructorInitializesWithRegistry() {
        MetricsProvider provider = new MetricsProvider(mockRegistry, TEST_CONTROLLER_ID);
        assertThat(provider).isNotNull();
    }

    @Test
    void testCounterCreatesCounterWithGivenName() {
        String counterName = "test.counter";
        MeterRegistry realRegistry = new SimpleMeterRegistry();
        MetricsProvider provider = new MetricsProvider(realRegistry, TEST_CONTROLLER_ID);
        Map<String, String> tags = new HashMap<>();
        tags.put("environment", "test");
        Counter counter = provider.counter(counterName, tags);
        assertThat(counter).isNotNull();
        assertThat(counter.getId().getName()).isEqualTo(counterName);
        assertThat(counter.getId().getTag("hostname")).isEqualTo(TEST_CONTROLLER_ID);
        assertThat(counter.getId().getTag("environment")).isEqualTo("test");

        counter.increment();
        counter.increment(5.0);
        assertThat(counter.count()).isEqualTo(6.0);
    }

    @Test
    void testGaugeCreatesGaugeWithGivenName() {
        String gaugeName = "test.gauge";
        MeterRegistry realRegistry = new SimpleMeterRegistry();
        MetricsProvider provider = new MetricsProvider(realRegistry, TEST_CONTROLLER_ID);
        Map<String, String> tags = new HashMap<>();
        tags.put("type", "memory");
        AtomicDouble gaugeValue = provider.gauge(gaugeName, 10.0, tags);

        assertThat(gaugeValue).isNotNull();
        assertThat(gaugeValue.get()).isEqualTo(10.0);

        Gauge gauge = realRegistry.find(gaugeName).gauge();
        assertThat(gauge).isNotNull();
        assertThat(gauge.value()).isEqualTo(10.0);
        assertThat(gauge.getId().getTag("hostname")).isEqualTo(TEST_CONTROLLER_ID);
        assertThat(gauge.getId().getTag("type")).isEqualTo("memory");

        gaugeValue.set(42.5);
        assertThat(gaugeValue.get()).isEqualTo(42.5);
        assertThat(gauge.value()).isEqualTo(42.5);
    }

    @Test
    void testGaugeCachingReturnsSameAtomicDouble() {
        String gaugeName = "test.gauge";
        MeterRegistry realRegistry = new SimpleMeterRegistry();
        MetricsProvider provider = new MetricsProvider(realRegistry, TEST_CONTROLLER_ID);
        Map<String, String> tags = new HashMap<>();
        tags.put("type", "cpu");
        AtomicDouble gaugeValue1 = provider.gauge(gaugeName, 10.0, tags);
        assertThat(gaugeValue1.get()).isEqualTo(10.0);
        AtomicDouble gaugeValue2 = provider.gauge(gaugeName, 20.0, tags);
        assertThat(gaugeValue2.get()).isEqualTo(20.0);
        assertThat(gaugeValue1).isSameAs(gaugeValue2);
        assertThat(gaugeValue1.get()).isEqualTo(20.0);
        assertThat(realRegistry.find(gaugeName).gauges()).hasSize(1);
    }

    @Test
    void testGaugeWithDifferentTagsCreatesSeparateGauges() {
        String gaugeName = "test.gauge.multi";
        MeterRegistry realRegistry = new SimpleMeterRegistry();
        MetricsProvider provider = new MetricsProvider(realRegistry, TEST_CONTROLLER_ID);
        Map<String, String> tags1 = new HashMap<>();
        tags1.put("shard", "0");
        Map<String, String> tags2 = new HashMap<>();
        tags2.put("shard", "1");

        AtomicDouble gauge1 = provider.gauge(gaugeName, 10.0, tags1);
        AtomicDouble gauge2 = provider.gauge(gaugeName, 20.0, tags2);
        assertThat(gauge1).isNotSameAs(gauge2);
        assertThat(gauge1.get()).isEqualTo(10.0);
        assertThat(gauge2.get()).isEqualTo(20.0);
        assertThat(realRegistry.find(gaugeName).gauges()).hasSize(2);
    }

    @Test
    void testTimerCreatesTimerWithGivenName() {
        String timerName = "test.timer";
        MeterRegistry realRegistry = new SimpleMeterRegistry();
        MetricsProvider provider = new MetricsProvider(realRegistry, TEST_CONTROLLER_ID);
        Map<String, String> tags = new HashMap<>();
        tags.put("operation", "query");
        Timer timer = provider.timer(timerName, tags);

        assertThat(timer).isNotNull();
        assertThat(timer.getId().getName()).isEqualTo(timerName);
        assertThat(timer.getId().getTag("hostname")).isEqualTo(TEST_CONTROLLER_ID);
        assertThat(timer.getId().getTag("operation")).isEqualTo("query");

        timer.record(100, java.util.concurrent.TimeUnit.MILLISECONDS);
        assertThat(timer.count()).isEqualTo(1);
        assertThat(timer.totalTime(java.util.concurrent.TimeUnit.MILLISECONDS)).isEqualTo(100);
    }
}

