package cwms.radar;

import io.javalin.Javalin;
import static io.javalin.apibuilder.ApiBuilder.*;
import io.javalin.http.JavalinServlet;
import io.javalin.plugin.openapi.OpenApiOptions;
import io.javalin.plugin.openapi.OpenApiPlugin;
import io.swagger.v3.oas.models.info.Info;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;
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

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.servlets.MetricsServlet;

import cwms.radar.api.*;

/**
 * Setup all the information required so we can serve the request.
 * 
 */
@WebServlet(urlPatterns = { "/locations/*", 
                            "/offices/*",
                            "/timeseries/*",
                            "/swagger-docs",
                            "/ratings/*",
                            "/parameters/*",
                            "/timezones/*",
                            "/levels/*"
})
public class ApiServlet extends HttpServlet {
    private Logger log = Logger.getLogger(ApiServlet.class.getName());    
    private MetricRegistry metrics;
    private Meter total_requests;
    /**
     *
     */
    private static final long serialVersionUID = 1L;

    JavalinServlet javalin = null;

    @Resource(name = "jdbc/CWMS3")
    DataSource cwms;

    @Override
    public void init() throws ServletException{
        //System.setProperty("org.eclipse.jetty.util.log.class", "org.eclipse.jetty.util.log.StdErrLog");
        //System.setProperty("org.eclipse.jetty.LEVEL", "OFF");
        String context = this.getServletContext().getContextPath();
        this.javalin = Javalin.createStandalone(config -> {
            config.defaultContentType = "application/json";   
            config.contextPath = context;                        
            config.registerPlugin(new OpenApiPlugin(getOpenApiOptions()));  
            config.enableDevLogging();          
            config.requestLogger( (ctx,ms) -> { log.info(ctx.toString());} );
        })
                .before( ctx -> { 
                    /* authorization on connection setup will go here
                    Connection conn = ctx.attribute("db");                    
                    */
                }
                )
                .exception(Exception.class, (e,ctx) -> {
                    e.printStackTrace(System.err);                   
                })
                .routes( () -> {      
                    //get("/", ctx -> { ctx.result("welcome to the CWMS REST APi").contentType("text/plain");});              
                    crud("/locations/:location_code", new LocationController(metrics));
                    crud("/offices/:office", new OfficeController(metrics));
                    crud("/units/:unit_name", new UnitsController(metrics));
                    crud("/parameters/:param_name", new ParametersController(metrics));
                    crud("/timezones/:zone", new TimeZoneController(metrics));
                    crud("/levels/:location", new LevelsController(metrics));
                    crud("/timeseries/:timeseries", new TimeSeriesController(metrics));
                    crud("/ratings/:rating", new RatingController(metrics));                    
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
        try (Connection db = cwms.getConnection() ) {      
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
