package cwms.radar.api;

import io.javalin.apibuilder.CrudHandler;
import io.javalin.http.Context;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.http.HttpServletResponse;


import cwms.radar.data.CwmsDataManager;
import cwms.radar.data.dao.Office;

/**
 *
 */
public class OfficeController implements CrudHandler {
    
    @OpenApi(
        queryParams = @OpenApiParam(name="format",required = false, description = "Specifies the encoding format of the response. Valid value for the format field for this URI are:\r\n1. tab\r\n2. csv\r\n 3. xml\r\n4. json (default)"),        
        responses = { @OpenApiResponse(status="200" ),
                      @OpenApiResponse(status="501",description = "The format requested is not implemented")
                    }
    )
    @Override
    public void getAll(Context ctx) {
        try (
                CwmsDataManager cdm = new CwmsDataManager(ctx);
            ) {
                                
                HashMap<String,Object> results = new HashMap<>();
                results.put("offices",cdm.getOffices());
                ctx.status(HttpServletResponse.SC_OK);
                ctx.json(results);            
        } catch (SQLException ex) {
            Logger.getLogger(LocationController.class.getName()).log(Level.SEVERE, null, ex);
            ctx.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            ctx.result("Failed to process request");
        }
    }

    @OpenApi(
        pathParams = @OpenApiParam(name="office", description = "The 3 letter office ID you want more information for", type = String.class),
        queryParams = @OpenApiParam(name="format",required = false, description = "Specifies the encoding format of the response. Valid value for the format field for this URI are:\r\n1. tab\r\n2. csv\r\n 3. xml\r\n4. json (default)"),        
        responses = { @OpenApiResponse(status="200" ),
                      @OpenApiResponse(status="501",description = "The format requested is not implemented")
                    }
    )
    @Override
    public void getOne(Context ctx, String office_id) {
        try(
            CwmsDataManager cdm = new CwmsDataManager(ctx);
        ) {
            Office office = cdm.getOfficeById(office_id);
            ctx.status(HttpServletResponse.SC_OK);
            ctx.json(office);
        } catch( SQLException ex ){
            Logger.getLogger(LocationController.class.getName()).log(Level.SEVERE, null, ex);
            ctx.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            ctx.result("Failed to process request");
        }        
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
