package mil.army.usace.hec;

import java.util.logging.Level;
import java.util.logging.Logger;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;

//import java.sql.DriverManager;

import io.javalin.Javalin;
import static io.javalin.apibuilder.ApiBuilder.*;
import io.javalin.core.plugin.Plugin;
import io.javalin.plugin.json.JavalinJackson;
import io.javalin.plugin.json.JavalinJson;
import io.javalin.plugin.openapi.OpenApiOptions;
import io.javalin.plugin.openapi.OpenApiPlugin;
import io.javalin.plugin.openapi.ui.SwaggerOptions;
import io.swagger.v3.oas.models.info.Info;


import cwms.radar.api.*;

import org.apache.tomcat.jdbc.pool.DataSource;
import org.owasp.html.*;


public class RadarAPI {
    private static final Logger logger = Logger.getLogger(RadarAPI.class.getName());
    private static final MetricRegistry metrics = new MetricRegistry();
    private static final Meter total_requests = metrics.meter("radar.total_requests");;
    public static void main(String args[]){        
        DataSource ds = new DataSource();
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
        
        PolicyFactory sanitizer = new HtmlPolicyBuilder().disallowElements("<script>").toFactory();
        int port = Integer.parseInt(System.getProperty("RADAR_LISTEN_PORT","7000"));
        ObjectMapper om = JavalinJackson.getObjectMapper();
        om.setPropertyNamingStrategy(PropertyNamingStrategy.KEBAB_CASE);
        JavalinJackson.configure(om);
        Javalin.create( config -> {
            config.defaultContentType = "application/json";   
            config.contextPath = "/";                        
            config.registerPlugin((Plugin) new OpenApiPlugin(getOpenApiOptions()));  
            if( System.getProperty("RADAR_DEBUG_LOGGING","false").equalsIgnoreCase("true")){
                config.enableDevLogging();          
            }            
            config.requestLogger( (ctx,ms) -> { logger.info(ctx.toString());} );
        }).before( ctx -> { 
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
            ((java.sql.Connection)ctx.attribute("database")).close();
        })
        .exception(UnsupportedOperationException.class, (e,ctx) -> {
            ctx.status(501);
            ctx.json(sanitizer.sanitize(e.getMessage()));
        })
        .exception(Exception.class, (e,ctx) -> {
            ctx.status(500);
            ctx.json("There was an error processing your request");
            logger.log(Level.WARNING,"error on request: " + ctx.req.getRequestURI(),e);                   
        })
        .routes( () -> {      
            get("/", ctx -> { ctx.result("welcome to the CWMS REST APi").contentType("text/plain");});                          
            crud("/locations/:location_code", new LocationController(metrics));
            crud("/offices/:office", new OfficeController(metrics));
            crud("/units/:unit_name", new UnitsController(metrics));
            crud("/parameters/:param_name", new ParametersController(metrics));
            crud("/timezones/:zone", new TimeZoneController(metrics));
            crud("/levels/:location", new LevelsController(metrics));
            crud("/timeseries/:timeseries", new TimeSeriesController(metrics));
            crud("/ratings/:rating", new RatingController(metrics)); 
            crud("/catalog/:", new CatalogController(metrics));                   
        }).start(port);
        
    }

    private static OpenApiOptions getOpenApiOptions() {
        Info applicationInfo = new Info().version("2.0").description("CWMS REST API for Data Retrieval");
        OpenApiOptions options = new OpenApiOptions(applicationInfo)
                    .path("/swagger-docs")           
                    .swagger( new SwaggerOptions("/swagger-ui"))
                    .activateAnnotationScanningFor("cwms.radar.api");                        
        return options;
    }
 
    private static String getconfig(String envName){
        return System.getenv(envName);
    }
    private static String getconfig(String envName,String defaultVal){
        String val = System.getenv(envName);
        return val != null ? val : defaultVal;
    }
}
