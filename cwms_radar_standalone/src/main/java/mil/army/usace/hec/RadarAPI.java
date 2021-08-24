package mil.army.usace.hec;

import java.io.IOException;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.servlet.http.HttpServletResponse;

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
import cwms.radar.api.BasinController;
import cwms.radar.api.enums.UnitSystem;
import cwms.radar.api.errors.RadarError;
import cwms.radar.formatters.FormattingException;
import io.javalin.Javalin;
import io.javalin.core.validation.JavalinValidation;
import io.javalin.http.BadRequestResponse;
import io.javalin.plugin.json.JavalinJackson;
import io.javalin.plugin.openapi.OpenApiOptions;
import io.javalin.plugin.openapi.OpenApiPlugin;
import io.javalin.plugin.openapi.ui.SwaggerOptions;
import io.swagger.v3.oas.models.info.Info;
import org.apache.http.entity.ContentType;
import org.apache.tomcat.jdbc.pool.DataSource;
import org.eclipse.jetty.servlet.ServletHolder;
import org.owasp.html.HtmlPolicyBuilder;
import org.owasp.html.PolicyFactory;

import static io.javalin.apibuilder.ApiBuilder.crud;


public class RadarAPI {
    private static final Logger logger = Logger.getLogger(RadarAPI.class.getName());
    private static final MetricRegistry metrics = new MetricRegistry();
    private static final Meter total_requests = metrics.meter("radar.total_requests");
    private Javalin app = null;
    private int port = -1;

    public static void main(String[] args){
        DataSource ds = new DataSource();
        int port = Integer.parseInt(System.getProperty("RADAR_LISTEN_PORT","7000"));
        try{
            ds.setDriverClassName(getconfig("RADAR_JDBC_DRIVER","oracle.jdbc.driver.OracleDriver"));
            ds.setUrl(getconfig("RADAR_JDBC_URL","jdbc:oracle:thin:@localhost/CWMSDEV"));
            ds.setUsername(getconfig("RADAR_JDBC_USERNAME"));
            ds.setPassword(getconfig("RADAR_JDBC_PASSWORD"));
            ds.setInitialSize(Integer.parseInt(getconfig("RADAR_POOL_INIT_SIZE","5")));
            ds.setMaxActive(Integer.parseInt(getconfig("RADAR_POOL_MAX_ACTIVE","10")));
            ds.setMaxIdle(Integer.parseInt(getconfig("RADAR_POOL_MAX_IDLE","5")));
            ds.setMinIdle(Integer.parseInt(getconfig("RADAR_POOL_MIN_IDLE","2")));
        } catch( Exception err ){
            logger.log(Level.SEVERE,"Required Parameter not set in environment",err);
            System.exit(1);
        }
        RadarAPI api = new RadarAPI(ds,port);
        api.start();
    }

    public RadarAPI(javax.sql.DataSource ds, int port){
        this.port = port;
        PolicyFactory sanitizer = new HtmlPolicyBuilder().disallowElements("<script>").toFactory();
        JavalinValidation.register(UnitSystem.class, v -> UnitSystem.systemFor(v) );

        ObjectMapper om = JavalinJackson.getObjectMapper();
        om.setPropertyNamingStrategy(PropertyNamingStrategies.KEBAB_CASE);
        om.registerModule(new JavaTimeModule());

        JavalinJackson.configure(om);
        app = Javalin.create( config -> {
            config.defaultContentType = "application/json";
            config.contextPath = "/";
            config.registerPlugin(new OpenApiPlugin(getOpenApiOptions()));
            if( System.getProperty("RADAR_DEBUG_LOGGING","false").equalsIgnoreCase("true")){
                config.enableDevLogging();
            }
            config.requestLogger( (ctx,ms) -> logger.info(ctx.toString()));
            config.configureServletContextHandler( sch -> {
                sch.addServlet(new ServletHolder(new MetricsServlet(metrics)),"/metrics/*");
            });
            config.addStaticFiles("/static");
        }).attribute(PolicyFactory.class,sanitizer)

          .before( ctx -> {
            ctx.header("X-Content-Type-Options","nosniff");
            ctx.header("X-Frame-Options","SAMEORIGIN");
            ctx.header("X-XSS-Protection", "1; mode=block");
            ctx.attribute("database",ds.getConnection());
            /* authorization on connection setup will go here
            Connection conn = ctx.attribute("db");
            */
            logger.info(ctx.header("accept"));
            total_requests.mark();
        }).after( ctx -> {
            try{
                ((java.sql.Connection)ctx.attribute("database")).close();
            } catch( SQLException e ){
                logger.log(Level.WARNING, "Failed to close database connection", e);
            }
        })
        .exception(FormattingException.class, (fe, ctx ) -> {
            final RadarError re = new RadarError("Formatting error");

            if( fe.getCause() instanceof IOException ){
                ctx.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            } else {
                ctx.status(HttpServletResponse.SC_NOT_IMPLEMENTED);
            }
            logger.log(Level.SEVERE,fe, () -> {
                return new StringBuilder(re.toString())
                .append("for request: " + ctx.fullUrl()).toString();
            });
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
            //get("/", ctx -> { ctx.result("welcome to the CWMS REST API").contentType(Formats.PLAIN);});
            crud("/locations/:location_code", new LocationController(metrics));
            crud("/location/category/:category-id", new LocationCategoryController(metrics));
            crud("/location/group/:group-id", new LocationGroupController(metrics));
            crud("/offices/:office", new OfficeController(metrics));
            crud("/units/:unit_name", new UnitsController(metrics));
            crud("/parameters/:param_name", new ParametersController(metrics));
            crud("/timezones/:zone", new TimeZoneController(metrics));
            crud("/levels/:location", new LevelsController(metrics));
            crud("/timeseries/:timeseries", new TimeSeriesController(metrics));
            crud("/timeseries/category/:category-id", new TimeSeriesCategoryController(metrics));
            crud("/timeseries/group/:group-id", new TimeSeriesGroupController(metrics));
            crud("/ratings/:rating", new RatingController(metrics));
            crud("/catalog/:dataSet", new CatalogController(metrics));
            crud("/basins/:basin-id", new BasinController(metrics));
            crud("/clobs/:clob-id", new ClobController(metrics));
            crud("/pools/:pool-id", new PoolController(metrics));
        });

    }

    private static OpenApiOptions getOpenApiOptions() {
        Info applicationInfo = new Info().version("2.0").description("CWMS REST API for Data Retrieval");
        OpenApiOptions options = new OpenApiOptions(applicationInfo)
                    .path("/swagger-docs")
                    .swagger( new SwaggerOptions("/swagger-ui.html"))
                    .activateAnnotationScanningFor("cwms.radar.api")
                    .defaultDocumentation( doc -> {
                        doc.json("500", RadarError.class);
                        doc.json("400", RadarError.class);
                        doc.json("404", RadarError.class);
                        doc.json("501", RadarError.class);
                    });

        return options;
    }

    public void start(){
        this.app.start(this.port);
    }

    public void stop(){
        this.app.stop();
    }

    private static String getconfig(String envName){
        return System.getenv(envName);
    }
    private static String getconfig(String envName,String defaultVal){
        String val = System.getenv(envName);
        return val != null ? val : defaultVal;
    }
}
