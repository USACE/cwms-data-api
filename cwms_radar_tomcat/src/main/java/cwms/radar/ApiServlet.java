package cwms.radar;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Resource;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.sql.DataSource;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.servlets.MetricsServlet;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import cwms.radar.api.CatalogController;
import cwms.radar.api.ClobController;
import cwms.radar.api.LevelsController;
import cwms.radar.api.LocationCategoryController;
import cwms.radar.api.LocationController;
import cwms.radar.api.LocationGroupController;
import cwms.radar.api.OfficeController;
import cwms.radar.api.ParametersController;
import cwms.radar.api.PoolController;
import cwms.radar.api.RatingController;
import cwms.radar.api.TimeSeriesCategoryController;
import cwms.radar.api.TimeSeriesController;
import cwms.radar.api.TimeSeriesGroupController;
import cwms.radar.api.TimeZoneController;
import cwms.radar.api.UnitsController;
import cwms.radar.api.enums.UnitSystem;
import cwms.radar.api.errors.RadarError;
import cwms.radar.formatters.Formats;
import cwms.radar.formatters.FormattingException;
import io.javalin.Javalin;
import io.javalin.core.validation.JavalinValidation;
import io.javalin.http.BadRequestResponse;
import io.javalin.http.JavalinServlet;
import io.javalin.plugin.json.JavalinJackson;
import io.javalin.plugin.openapi.OpenApiOptions;
import io.javalin.plugin.openapi.OpenApiPlugin;
import io.swagger.v3.oas.models.info.Info;
import org.apache.http.entity.ContentType;
import org.owasp.html.HtmlPolicyBuilder;
import org.owasp.html.PolicyFactory;

import cwms.radar.api.*;
import cwms.radar.api.enums.UnitSystem;
import cwms.radar.api.errors.RadarError;

import static io.javalin.apibuilder.ApiBuilder.crud;
import static io.javalin.apibuilder.ApiBuilder.get;


/**
 * Setup all the information required so we can serve the request.
 *
 */
@WebServlet(urlPatterns = { "/catalog/*",
                            "/swagger-docs",
                            "/timeseries/*",
                            "/offices/*",
                            "/locations/*",
                            "/parameters/*",
                            "/timezones/*",
                            "/units/*",
                            "/ratings/*",
                            "/levels/*",
                            "/blobs/*",
                            "/clobs/*",
                            "/pools/*",
                            "/index*"
})
public class ApiServlet extends HttpServlet {
    public static final Logger logger = Logger.getLogger(ApiServlet.class.getName());
    private MetricRegistry metrics;
    private Meter total_requests;
    /**
     *
     */
    private static final long serialVersionUID = 1L;

    static JavalinServlet javalin = null;

    @Resource(name = "jdbc/CWMS3")
    DataSource cwms;

    @Override
    public void init() throws ServletException{
        //System.setProperty("org.eclipse.jetty.util.log.class", "org.eclipse.jetty.util.log.StdErrLog");
        //System.setProperty("org.eclipse.jetty.LEVEL", "OFF");
        String context = this.getServletContext().getContextPath();

        PolicyFactory sanitizer = new HtmlPolicyBuilder().disallowElements("<script>").toFactory();
        ObjectMapper om = JavalinJackson.getObjectMapper();
        JavalinValidation.register(UnitSystem.class, UnitSystem::systemFor);
        om.setPropertyNamingStrategy(PropertyNamingStrategies.KEBAB_CASE);
        om.registerModule(new JavaTimeModule());            // Needed in Java 8 to properly format java.time classes

        javalin = Javalin.createStandalone(config -> {
            config.defaultContentType = "application/json";
            config.contextPath = context;
            config.registerPlugin(new OpenApiPlugin(getOpenApiOptions()));
            config.enableDevLogging();
            config.requestLogger( (ctx,ms) -> logger.info(ctx.toString()));
            config.addStaticFiles("/static");
        })
                .attribute(PolicyFactory.class,sanitizer)
                .before( ctx -> {
                    /* authorization on connection setup will go here
                    Connection conn = ctx.attribute("db");
                    */
                    ctx.attribute("sanitizer",sanitizer);
                    ctx.header("X-Content-Type-Options","nosniff");
                    ctx.header("X-Frame-Options","SAMEORIGIN");
                    ctx.header("X-XSS-Protection", "1; mode=block");
                }).exception(FormattingException.class, (fe, ctx ) -> {
                    final RadarError re = new RadarError("Formatting error");

                    if( fe.getCause() instanceof IOException ){
                        ctx.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                    } else {
                        ctx.status(HttpServletResponse.SC_NOT_IMPLEMENTED);
                    }
                    logger.log(Level.SEVERE,fe, () -> re + "for request: " + ctx.fullUrl());
                    ctx.json(re);
                })
                .exception(UnsupportedOperationException.class, (e, ctx) -> {
                    ctx.status(HttpServletResponse.SC_NOT_IMPLEMENTED).json(RadarError.notImplemented());
                })
                .exception(BadRequestResponse.class, (e, ctx) -> {
                    RadarError re = new RadarError("Bad Request", e.getDetails());
                    logger.log(Level.INFO, re.toString(), e );
                    ctx.status(e.getStatus()).json(re);
                })
                .exception(IllegalArgumentException.class, (e, ctx ) -> {
                    RadarError re = new RadarError("Bad Request");
                    logger.log(Level.INFO, re.toString(), e );
                    ctx.status(HttpServletResponse.SC_BAD_REQUEST).json(re);
                })
                .exception(Exception.class, (e,ctx) -> {
                    RadarError errResponse = new RadarError("System Error");
                    logger.log(Level.WARNING,String.format("error on request[%s]: %s", errResponse.getIncidentIdentifier(), ctx.req.getRequestURI()),e);
                    ctx.status(500);
                    ctx.contentType(ContentType.APPLICATION_JSON.toString());
                    ctx.json(errResponse);
                })
                .routes( () -> {
                    get("/", ctx -> ctx.result("Welcome to the CWMS REST API").contentType(Formats.PLAIN));
                    crud("/locations/:location_code", new LocationController(metrics));
                    crud("/location/category/:category-id", new LocationCategoryController(metrics));
                    crud("/location/group/:group-id", new LocationGroupController(metrics));
                    crud("/offices/:office", new OfficeController(metrics));
                    crud("/units/:unit_name", new UnitsController(metrics));
                    crud("/parameters/:param_name", new ParametersController(metrics));
                    crud("/timezones/:zone", new TimeZoneController(metrics));
                    crud("/levels/:location", new LevelsController(metrics));
                    TimeSeriesController tsController = new TimeSeriesController(metrics);
                    crud("/timeseries/:timeseries", tsController);
                    get("/timeseries/recent/:group-id", tsController::getRecent);
                    crud("/timeseries/category/:category-id", new TimeSeriesCategoryController(metrics));
                    crud("/timeseries/group/:group-id", new TimeSeriesGroupController(metrics));
                    crud("/ratings/:rating", new RatingController(metrics));
                    crud("/catalog/:dataSet", new CatalogController(metrics));
                    crud("/blobs/:blob-id", new BlobController(metrics));
                    crud("/clobs/:clob-id", new ClobController(metrics));
                    crud("/pools/:pool-id", new PoolController(metrics));
                }).servlet();
    }

    private OpenApiOptions getOpenApiOptions() {
        Info applicationInfo = new Info().version("2.0").description("CWMS REST API for Data Retrieval");
        OpenApiOptions options = new OpenApiOptions(applicationInfo)
                    .path("/swagger-docs")

                    .activateAnnotationScanningFor("cwms.radar.api");
        return options;
    }

    @Override
    protected void service(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        total_requests.mark();
        try (Connection db = cwms.getConnection()) {
            String office = req.getContextPath().substring(1).split("-")[0];//
            if( office.equalsIgnoreCase("cwms")){
                office = "HQ";
            }
            req.setAttribute("office_id", office.toUpperCase());
            req.setAttribute("database", db);
            javalin.service(req, resp);
        } catch (SQLException ex) {
            Logger.getLogger(ApiServlet.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

    @Override
    public void init(ServletConfig config) throws ServletException {
        metrics = (MetricRegistry)config.getServletContext().getAttribute(MetricsServlet.METRICS_REGISTRY);
        total_requests = metrics.meter("radar.total_requests");
        super.init(config);
    }

}
