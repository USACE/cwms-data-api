package cwms.cda;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.codahale.metrics.MetricRegistry;
import io.prometheus.metrics.instrumentation.dropwizard.DropwizardExports;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import io.prometheus.metrics.model.snapshots.CounterSnapshot;
import io.prometheus.metrics.model.snapshots.MetricSnapshots;
import org.junit.jupiter.api.Test;

class CdaMetricsContextListenerTest {

    @Test
    void webXmlServletClassIsOnClasspath() {
        assertDoesNotThrow(() -> Class.forName(
                "io.prometheus.metrics.exporter.servlet.javax.PrometheusMetricsServlet"));
    }

    @Test
    void codahaleCounterIsExportedToPrometheusRegistry() {
        MetricRegistry codaHale = new MetricRegistry();
        PrometheusRegistry prom = new PrometheusRegistry();
        DropwizardExports.builder().dropwizardRegistry(codaHale).register(prom);

        codaHale.counter("cda_test_counter").inc(7);

        MetricSnapshots snapshots = prom.scrape();
        CounterSnapshot snapshot = (CounterSnapshot) snapshots.iterator().next();
        assertEquals("cda_test_counter", snapshot.getMetadata().getPrometheusName());
        assertEquals(7.0, snapshot.getDataPoints().get(0).getValue());
    }
}
