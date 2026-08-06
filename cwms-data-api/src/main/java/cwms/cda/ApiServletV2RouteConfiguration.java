package cwms.cda;

import static java.lang.String.format;

import com.codahale.metrics.MetricRegistry;
import cwms.cda.api.Controllers;
import cwms.cda.api.v2.ForecastSpecControllerV2;
import io.javalin.core.security.RouteRole;
import java.util.concurrent.TimeUnit;

public final class ApiServletV2RouteConfiguration {

    private ApiServletV2RouteConfiguration() {
        throw new AssertionError("Utility class - do not instantiate");
    }

    public static void configureRoutes(MetricRegistry metrics, RouteRole[] requiredRoles) {
        ApiServlet.cdaCrudCache(formatV2(ApiServlet.FORECAST_SPEC_PATH, Controllers.NAME),
                new ForecastSpecControllerV2(metrics), requiredRoles, 5, TimeUnit.MINUTES);
    }

    private static String formatV2(String path, Object... args) {
        return format("/v2/" + path, args);
    }
}
