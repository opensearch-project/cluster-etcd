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

import static org.assertj.core.api.Assertions.*;

@ExtendWith(MockitoExtension.class)
class MetricsProviderTest {

    @Mock
    private MeterRegistry mockRegistry;

    @Test
    void testConstructorInitializesWithRegistry() {
        MetricsProvider provider = new MetricsProvider(mockRegistry);
        assertThat(provider).isNotNull();
    }

    @Test
    void testCounterCreatesCounterWithGivenName() {
        String counterName = "test.counter";
        MeterRegistry realRegistry = new SimpleMeterRegistry();
        MetricsProvider provider = new MetricsProvider(realRegistry);
        Counter counter = provider.counter(counterName);
        assertThat(counter).isNotNull();
        assertThat(counter.getId().getName()).isEqualTo(counterName);

        counter.increment();
        counter.increment(5.0);
        assertThat(counter.count()).isEqualTo(6.0);
    }

    @Test
    void testGaugeCreatesGaugeWithGivenName() {
        String gaugeName = "test.gauge";
        MeterRegistry realRegistry = new SimpleMeterRegistry();
        MetricsProvider provider = new MetricsProvider(realRegistry);
        AtomicDouble gaugeValue = provider.gauge(gaugeName);

        assertThat(gaugeValue).isNotNull();
        assertThat(gaugeValue.get()).isEqualTo(0.0);

        Gauge gauge = realRegistry.find(gaugeName).gauge();
        assertThat(gauge).isNotNull();
        assertThat(gauge.value()).isEqualTo(0.0);

        gaugeValue.set(42.5);
        assertThat(gaugeValue.get()).isEqualTo(42.5);
    }

    @Test
    void testTimerCreatesTimerWithGivenName() {
        String timerName = "test.timer";
        MeterRegistry realRegistry = new SimpleMeterRegistry();
        MetricsProvider provider = new MetricsProvider(realRegistry);
        Timer timer = provider.timer(timerName);

        assertThat(timer).isNotNull();
        assertThat(timer.getId().getName()).isEqualTo(timerName);

        timer.record(100, java.util.concurrent.TimeUnit.MILLISECONDS);
        assertThat(timer.count()).isEqualTo(1);
        assertThat(timer.totalTime(java.util.concurrent.TimeUnit.MILLISECONDS)).isEqualTo(100);
    }
}

