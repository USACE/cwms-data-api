package cwms.radar;

import javax.servlet.annotation.WebListener;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.servlets.MetricsServlet;

@WebListener
public class RADARMetricsContextListener extends MetricsServlet.ContextListener {

    public static final MetricRegistry METRIC_REGISTRY = new MetricRegistry();

    @Override
    protected MetricRegistry getMetricRegistry() {
        return METRIC_REGISTRY;
    }

}
