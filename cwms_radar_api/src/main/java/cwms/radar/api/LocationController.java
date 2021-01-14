package cwms.radar.api;

import io.javalin.apibuilder.CrudHandler;
import io.javalin.http.Context;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;

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
    
    
    @OpenApi(
        queryParams = {
            @OpenApiParam( name="names",required = false, description = "Specifies the name(s) of the location(s) whose data is to be included in the response"),
            @OpenApiParam(name="office", required=false, description="Specifies the owning office of the location level(s) whose data is to be included in the response. If this field is not specified, matching location level information from all offices shall be returned."),
            @OpenApiParam(name="unit", required=false, description="Specifies the unit or unit system of the response. Valid values for the unit field are:\r\n 1. EN.   Specifies English unit system.  Location level values will be in the default English units for their parameters.\r\n2. SI.   Specifies the SI unit system.  Location level values will be in the default SI units for their parameters.\r\n3. Other. Any unit returned in the response to the units URI request that is appropriate for the requested parameters."),
            @OpenApiParam(name="datum", required=false, description="Specifies the elevation datum of the response. This field affects only elevation location levels. Valid values for this field are:\r\n1. NAVD88.  The elevation values will in the specified or default units above the NAVD-88 datum.\r\n2. NGVD29.  The elevation values will be in the specified or default units above the NGVD-29 datum."),
            @OpenApiParam(name="format", required=false, description="Specifies the encoding format of the response. Valid values for the format field for this URI are:\r\n1.    tab\r\n2.    csv\r\n3.    xml\r\n4.  wml2 (only if name field is specified)\r\n5.    json (default)")            
        },
        responses = {
            @OpenApiResponse( status="200"),
            @OpenApiResponse( status="404", description = "Based on the combination of inputs provided the location(s) were not found."),
            @OpenApiResponse( status="501", description = "request format is not implemented")
        }
        , description = "Returns CWMS Location Data"        
    )
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
