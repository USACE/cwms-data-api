package cwms.radar;

import io.javalin.Javalin;
import static io.javalin.apibuilder.ApiBuilder.*;
import io.javalin.http.JavalinServlet;
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
@WebServlet( urlPatterns = {"/*"})
public class ApiServlet extends HttpServlet {
    /**
     *
     */
    private static final long serialVersionUID = 1L;

    final JavalinServlet javalin;
    
    @Resource(name="jdbc/CWMS")
    DataSource cwms;

    
    public ApiServlet(){
        this.javalin = Javalin.createStandalone(config -> {
            config.defaultContentType = "application/json";   
            config.contextPath = "/";            
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
                    get("/", ctx -> { ctx.result("welcome to the CWMS REST APi");});              
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
