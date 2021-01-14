package cwms.radar;

import io.javalin.Javalin;
import static io.javalin.apibuilder.ApiBuilder.*;
import io.javalin.http.JavalinServlet;
import io.javalin.plugin.openapi.OpenApiOptions;
import io.javalin.plugin.openapi.OpenApiPlugin;
import io.javalin.plugin.openapi.ui.SwaggerOptions;
import io.swagger.v3.oas.models.info.Info;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Resource;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.sql.DataSource;
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
    /**
     *
     */
    private static final long serialVersionUID = 1L;

    final JavalinServlet javalin;

    @Resource(name = "jdbc/CWMS")
    DataSource cwms;

    public ApiServlet() {
        //System.setProperty("org.eclipse.jetty.util.log.class", "org.eclipse.jetty.util.log.StdErrLog");
        //System.setProperty("org.eclipse.jetty.LEVEL", "OFF");
        this.javalin = Javalin.createStandalone(config -> {
            config.defaultContentType = "application/json";   
            config.contextPath = "/cwms-data";                        
            config.registerPlugin(new OpenApiPlugin(getOpenApiOptions()));  
            config.enableDevLogging();          
            config.requestLogger( (ctx,ms) -> { log.info(ctx.toString());} );
        })
                .before( ctx -> { 
                    Connection conn = ctx.attribute("db");
                    //TODO: any pre-connection setup goes here
                }
                )
                .exception(Exception.class, (e,ctx) -> {
                    e.printStackTrace(System.err);                   
                })
                .routes( () -> {      
                    //get("/", ctx -> { ctx.result("welcome to the CWMS REST APi").contentType("text/plain");});              
                    crud("/locations/:location_code", new LocationController());
                    crud("/offices/:office_name", new OfficeController());
                    crud("/units/:unit_name", new UnitsController());
                    crud("/parameters/:param_name", new ParametersController());
                    crud("/timezones/:zone", new TimeZoneController());
                    crud("/levels/:location", new LevelsController());
                    crud("/timeseries/:timeseries", new TimeSeriesController());
                    crud("/ratings/:rating", new RatingController());
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
        try (Connection db = cwms.getConnection() ) {            
            req.setAttribute("database", db);
            javalin.service(req, resp);     
        } catch (SQLException ex) {
            Logger.getLogger(ApiServlet.class.getName()).log(Level.SEVERE, null, ex);
        } 
        
    }
  
}
