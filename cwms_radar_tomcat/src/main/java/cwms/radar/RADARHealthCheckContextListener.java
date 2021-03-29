package cwms.radar;

import javax.servlet.annotation.WebListener;

import com.codahale.metrics.health.HealthCheckRegistry;
import com.codahale.metrics.servlets.HealthCheckServlet;

@WebListener
public class RADARHealthCheckContextListener extends HealthCheckServlet.ContextListener {

    public static final HealthCheckRegistry HEALTH_CHECK_REGISTRY = new HealthCheckRegistry();

    @Override
    protected HealthCheckRegistry getHealthCheckRegistry() {
        return HEALTH_CHECK_REGISTRY;
    }

}
