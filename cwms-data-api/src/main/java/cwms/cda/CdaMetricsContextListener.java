package cwms.cda;

import javax.servlet.annotation.WebListener;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.servlets.MetricsServlet;

import io.prometheus.metrics.instrumentation.dropwizard.DropwizardExports;
import io.prometheus.metrics.model.registry.PrometheusRegistry;

@WebListener
public class CdaMetricsContextListener extends MetricsServlet.ContextListener {

    public static final MetricRegistry METRIC_REGISTRY = new MetricRegistry();

    static {
        DropwizardExports.builder()
                .dropwizardRegistry(METRIC_REGISTRY)
                .register(PrometheusRegistry.defaultRegistry);
    }

    @Override
    protected MetricRegistry getMetricRegistry() {
        return METRIC_REGISTRY;
    }
}
