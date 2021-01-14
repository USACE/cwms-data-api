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

public class ParametersController implements CrudHandler {
    private static final Logger logger = Logger.getLogger(UnitsController.class.getName());

    @OpenApi(ignore = true)
    @Override
    public void create(Context ctx) {
        ctx.status(HttpServletResponse.SC_NOT_FOUND);        
    }

    @OpenApi(ignore = true)
    @Override
    public void delete(Context ctx, String id) {
        ctx.status(HttpServletResponse.SC_NOT_FOUND);        

    }

    @OpenApi(
        queryParams = {
            @OpenApiParam(name="format",required = false, description = "Specifies the encoding format of the response. Valid value for the format field for this URI are:\r\n1. tab\r\n2. csv\r\n 3. xml\r\n4. json (default)")        
        },
        responses = {
            @OpenApiResponse( status = "200"),
            @OpenApiResponse(status="501",description = "The format requested is not implemented")
        }
    )
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

            String results = cdm.getParameters(format);                
            ctx.status(HttpServletResponse.SC_OK);
            ctx.result(results);                
        } catch (SQLException ex) {
            logger.log(Level.SEVERE, null, ex);
            ctx.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            ctx.result("Failed to process request");
        }        
    }

    @OpenApi(ignore = true)
    @Override
    public void getOne(Context ctx, String id) {
        ctx.status(HttpServletResponse.SC_NOT_IMPLEMENTED);        

    }

    @OpenApi(ignore = true)
    @Override
    public void update(Context ctx, String id) {
        ctx.status(HttpServletResponse.SC_NOT_FOUND);        

    }
    
}
