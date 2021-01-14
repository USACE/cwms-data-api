package cwms.radar.api;

import io.javalin.apibuilder.CrudHandler;
import io.javalin.http.Context;
import io.javalin.plugin.openapi.annotations.OpenApi;

import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.servlet.http.HttpServletResponse;

import cwms.radar.data.CwmsDataManager;


/**
 *
 * 
 */
public class LocationController implements CrudHandler {
    
    

    @Override
    public void getAll(Context ctx) {
        try (
                CwmsDataManager cdm = new CwmsDataManager(ctx);
            ) {
                String format = ctx.queryParam("format","json");           
                String names = ctx.queryParam("names");
                String units = ctx.queryParam("units");
                String datum = ctx.queryParam("datum");
                String office = ctx.queryParam("office");


                switch(format){
                    case "json": {ctx.contentType("application/json"); break;}
                    case "tab": {ctx.contentType("text/tab-sperated-values");break;}
                    case "csv": {ctx.contentType("text/csv"); break;}
                    case "xml": {ctx.contentType("application/xml");break;}
                    case "wml2": {ctx.contentType("application/xml");break;}
                }

                String results = cdm.getLocations(names,format,units,datum,office);                
                ctx.status(HttpServletResponse.SC_OK);
                ctx.result(results);                
        } catch (SQLException ex) {
            Logger.getLogger(LocationController.class.getName()).log(Level.SEVERE, null, ex);
            ctx.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            ctx.result("Failed to process request");
        }
    }

    @OpenApi(ignore = true)
    @Override
    public void getOne(Context ctx, String location_code) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @OpenApi(ignore = true)
    @Override
    public void create(Context ctx) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @OpenApi(ignore = true)
    @Override
    public void update(Context ctx, String location_code) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @OpenApi(ignore = true)
    @Override
    public void delete(Context ctx, String location_code) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

}
