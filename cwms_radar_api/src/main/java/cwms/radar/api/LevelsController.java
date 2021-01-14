package cwms.radar.api;

import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.http.HttpServletResponse;

import cwms.radar.data.CwmsDataManager;
import io.javalin.apibuilder.CrudHandler;
import io.javalin.http.Context;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;

public class LevelsController implements CrudHandler {
    private static final Logger logger = Logger.getLogger(UnitsController.class.getName());

    @Override
    public void create(Context ctx) {
        ctx.status(HttpServletResponse.SC_NOT_FOUND);        
    }

    @Override
    public void delete(Context ctx, String id) {
        ctx.status(HttpServletResponse.SC_NOT_FOUND);        

    }
    @OpenApi(
        pathParams = @OpenApiParam(name="id"),        
        responses = @OpenApiResponse(status="200" )
    )
    @Override
    public void getAll(Context ctx) {
        try (
            CwmsDataManager cdm = new CwmsDataManager(ctx);
        ) {
            String format = ctx.queryParam("format","json");                       
            String names = ctx.queryParam("name");
            String office = ctx.queryParam("office");
            String unit = ctx.queryParam("unit");
            String datum = ctx.queryParam("datum");
            String begin = ctx.queryParam("begin");
            String end = ctx.queryParam("end");
            String timezone = ctx.queryParam("timezone");
            

            switch(format){
                case "json": {ctx.contentType("application/json"); break;}
                case "tab": {ctx.contentType("text/tab-sperated-values");break;}
                case "csv": {ctx.contentType("text/csv"); break;}
                case "xml": {ctx.contentType("application/xml");break;}
                case "wml2": {ctx.contentType("application/xml");break;}
                case "png":
                case "jpg":{
                    ctx.contentType("text/plain");
                    ctx.status(HttpServletResponse.SC_NOT_IMPLEMENTED);
                    ctx.result("At this time we are not implemeting graphics in this version.");
                    break;
                }
                default: {
                    ctx.contentType("text/plain");
                    ctx.status(HttpServletResponse.SC_NOT_FOUND);
                    ctx.result(String.format("Format {} is not implemented for this endpoint",format));
                    return;
                }
            }

            String results = cdm.getLocationLevels(format,names,office,unit,datum,begin,end,timezone);                
            ctx.status(HttpServletResponse.SC_OK);
            ctx.result(results);                
        } catch (SQLException ex) {
            logger.log(Level.SEVERE, null, ex);
            ctx.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            ctx.result("Failed to process request");
        }        
    }

    @Override
    public void getOne(Context ctx, String id) {
        ctx.status(HttpServletResponse.SC_NOT_IMPLEMENTED);        

    }

    @Override
    public void update(Context ctx, String id) {
        ctx.status(HttpServletResponse.SC_NOT_FOUND);        

    }
    
}
