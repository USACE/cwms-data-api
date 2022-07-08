package cwms.radar.api;

import static com.codahale.metrics.MetricRegistry.name;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.radar.api.errors.RadarError;
import cwms.radar.data.CwmsDataManager;
import cwms.radar.formatters.Formats;
import io.javalin.apibuilder.CrudHandler;
import io.javalin.http.Context;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.servlet.http.HttpServletResponse;

public class TimeZoneController implements CrudHandler {
    private static final Logger logger = Logger.getLogger(TimeZoneController.class.getName());
    private final MetricRegistry metrics;
    private final Meter getAllRequests;
    private final Timer getAllRequestsTime;
    private final Meter getOneRequest;
    private final Timer getOneRequestTime;
    private final Histogram requestResultSize;

    public TimeZoneController(MetricRegistry metrics) {
        this.metrics = metrics;
        String className = this.getClass().getName();
        getAllRequests = this.metrics.meter(name(className, "getAll", "count"));
        getAllRequestsTime = this.metrics.timer(name(className, "getAll", "time"));
        getOneRequest = this.metrics.meter(name(className, "getOne", "count"));
        getOneRequestTime = this.metrics.timer(name(className, "getOne", "time"));
        requestResultSize = this.metrics.histogram((name(className, "results", "size")));
    }

    @OpenApi(ignore = true)
    @Override
    public void create(Context ctx) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @OpenApi(ignore = true)
    @Override
    public void delete(Context ctx, String id) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @OpenApi(
            queryParams = {
                    @OpenApiParam(name = "format", required = false, description = "Specifies the"
                            + " encoding format of the response. Valid value for the format field"
                            + " for this URI are:\r\n1. tab\r\n2. csv\r\n 3. xml\r\n4. json "
                            + "(default)")
            },
            responses = {
                    @OpenApiResponse(status = "200"),
                    @OpenApiResponse(status = "501", description = "The format requested is not "
                            + "implemented")
            },
            tags = {"TimeZones"}
    )
    @Override
    public void getAll(Context ctx) {
        getAllRequests.mark();
        try (Timer.Context timeContext = getAllRequestsTime.time();
              CwmsDataManager cdm = new CwmsDataManager(ctx)) {
            String format = ctx.queryParamAsClass("format", String.class).getOrDefault("json");


            switch (format) {
                case "json": {
                    ctx.contentType(Formats.JSON);
                    break;
                }
                case "tab": {
                    ctx.contentType(Formats.TAB);
                    break;
                }
                case "csv": {
                    ctx.contentType(Formats.CSV);
                    break;
                }
                case "xml": {
                    ctx.contentType(Formats.XML);
                    break;
                }
                case "wml2": {
                    ctx.contentType(Formats.WML2);
                    break;
                }
                default: {
                    ctx.status(HttpServletResponse.SC_NOT_IMPLEMENTED)
                            .json(RadarError.notImplemented());
                    return;
                }
            }

            String results = cdm.getTimeZones(format);
            requestResultSize.update(results.length());
            ctx.status(HttpServletResponse.SC_OK);
            ctx.result(results);
            requestResultSize.update(results.length());
        } catch (SQLException ex) {
            logger.log(Level.SEVERE, null, ex);
            ctx.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            ctx.result("Failed to process request");
        }
    }

    @OpenApi(ignore = true)
    @Override
    public void getOne(Context ctx, String id) {
        getOneRequest.mark();
        try (Timer.Context timeContext = getOneRequestTime.time()) {
            throw new UnsupportedOperationException("Not supported yet.");
        }
    }

    @OpenApi(ignore = true)
    @Override
    public void update(Context ctx, String id) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

}
