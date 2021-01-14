package cwms.radar.api;

import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.http.HttpServletResponse;

import cwms.radar.data.CwmsDataManager;
import io.javalin.apibuilder.CrudHandler;
import io.javalin.http.Context;

public class RatingController implements CrudHandler {
    private static final Logger logger = Logger.getLogger(RatingController.class.getName());

    @Override
    public void create(Context ctx) {
        ctx.status(HttpServletResponse.SC_NOT_FOUND);
    }

    @Override
    public void delete(Context ctx, String rating) {
        ctx.status(HttpServletResponse.SC_NOT_FOUND);
    }

    @Override
    public void getAll(Context ctx) {
        try (
                CwmsDataManager cdm = new CwmsDataManager(ctx);
            ) {
                String format = ctx.queryParam("format","json");           
                String names = ctx.queryParam("names");
                String unit = ctx.queryParam("unit");
                String datum = ctx.queryParam("datum");
                String office = ctx.queryParam("office");
                String start = ctx.queryParam("at");
                String end = ctx.queryParam("end");
                String timezone = ctx.queryParam("timezone","UTC");
                String size = ctx.queryParam("size");                

                switch(format){
                    case "json": {ctx.contentType("application/json"); break;}
                    case "tab": {ctx.contentType("text/tab-sperated-values");break;}
                    case "csv": {ctx.contentType("text/csv"); break;}
                    case "xml": {ctx.contentType("application/xml");break;}
                    case "wml2": {ctx.contentType("application/xml");break;}
                    case "jpg": // same handler
                    case "png": {
                        ctx.status(HttpServletResponse.SC_NOT_FOUND);
                        ctx.contentType("text/plain");
                        ctx.result("Server Side image creation is not supported in this version. Note: we may be retiring it in favor or encouraging the use of client based rendering.");
                        return;                        
                     }
                     default: {
                        ctx.status(HttpServletResponse.SC_NOT_FOUND);
                        ctx.contentType("text/plain");
                        ctx.result(String.format("Format ,{} , is not available on this endpoint",format));
                        return;
                     }
                }

                String results = cdm.getRatings(names,format,unit,datum,office,start,end,timezone,size);                
                ctx.status(HttpServletResponse.SC_OK);
                ctx.result(results);                
        } catch (SQLException ex) {
            logger.log(Level.SEVERE, null, ex);
            ctx.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            ctx.result("Failed to process request");
        }

    }

    @Override
    public void getOne(Context ctx, String rating) {
        ctx.status(HttpServletResponse.SC_NOT_FOUND);
    }

    @Override
    public void update(Context ctx, String rating) {
        ctx.status(HttpServletResponse.SC_NOT_FOUND);
    }
    
}
