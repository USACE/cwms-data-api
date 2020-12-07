package cwms.radar.api;

import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.http.HttpServletResponse;

import cwms.radar.data.CwmsDataManager;
import io.javalin.apibuilder.CrudHandler;
import io.javalin.http.Context;

public class UnitsController implements CrudHandler {
    private static final Logger logger = Logger.getLogger(UnitsController.class.getName());

    @Override
    public void create(Context arg0) {
        // TODO Auto-generated method stub

    }

    @Override
    public void delete(Context arg0, String arg1) {
        // TODO Auto-generated method stub

    }

    @Override
    public void getAll(Context ctx) {
        try (
            CwmsDataManager cdm = new CwmsDataManager(ctx);
        ) {
            String format = ctx.queryParam("format","json");                       


            switch(format){
                case "json": {ctx.contentType("application/json"); break;}
                case "tab": {ctx.contentType("text/tab-sperated-values");break;}
                case "csv": {ctx.contentType("text/csv"); break;}
                case "xml": {ctx.contentType("application/xml");break;}
                case "wml2": {ctx.contentType("application/xml");break;}
            }

            String results = cdm.getUnits(format);                
            ctx.status(HttpServletResponse.SC_OK);
            ctx.result(results);                
        } catch (SQLException ex) {
            logger.log(Level.SEVERE, null, ex);
            ctx.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            ctx.result("Failed to process request");
        }

    }

    @Override
    public void getOne(Context arg0, String arg1) {
        // TODO Auto-generated method stub

    }

    @Override
    public void update(Context arg0, String arg1) {
        // TODO Auto-generated method stub

    }
    
}
