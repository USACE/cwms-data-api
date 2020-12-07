/*
 * The MIT License
 *
 * Copyright 2020 mike.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
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
 *
 * @author mike
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
