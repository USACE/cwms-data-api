package cwms.radar.api;

import io.javalin.apibuilder.CrudHandler;
import io.javalin.core.util.Header;
import io.javalin.http.Context;
import io.javalin.plugin.openapi.annotations.*;
import kotlin.NotImplementedError;

import java.sql.SQLException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.http.HttpServletResponse;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import static com.codahale.metrics.MetricRegistry.*;
import com.codahale.metrics.Timer;

import cwms.radar.data.CwmsDataManager;
import cwms.radar.data.dao.Office;
import cwms.radar.formatters.FormatFactory;
import cwms.radar.formatters.Formats;
import cwms.radar.formatters.OfficeFormatV1;
import cwms.radar.formatters.OutputFormatter;
import cwms.radar.formatters.csv.CsvV1Office;
import cwms.radar.formatters.tab.TabV1Office;

/**
 *
 */
public class OfficeController implements CrudHandler {
    private static final Logger logger = Logger.getLogger(OfficeController.class.getName());
    private final MetricRegistry metrics;// = new MetricRegistry();
    private final Meter getAllRequests;// = metrics.meter(OfficeController.class.getName()+"."+"getAll.count");
    private final Timer getAllRequestsTime;// =metrics.timer(OfficeController.class.getName()+"."+"getAll.time");
    private final Meter getOneRequest;
    private final Timer getOneRequestTime;
    private final Histogram requestResultSize;
    

    public OfficeController(MetricRegistry metrics){
        this.metrics=metrics;
        String className = OfficeController.class.getName();
        getAllRequests = this.metrics.meter(name(className,"getAll","count"));
        getAllRequestsTime = this.metrics.timer(name(className,"getAll","time"));
        getOneRequest = this.metrics.meter(name(className,"getOne","count"));
        getOneRequestTime = this.metrics.timer(name(className,"getOne","time"));
        requestResultSize = this.metrics.histogram((name(className,"results","size")));
    }
    

    @OpenApi(                
        queryParams = @OpenApiParam(name="format",
                                    required = false, 
                                    deprecated = true,
                                    description = "(Deprecated in favor of Accept header Specifies the encoding format of the response. Valid value for the format field for this URI are:\r\n1. tab\r\n2. csv\r\n 3. xml\r\n4. json (default)"
                                    ),        
        responses = { @OpenApiResponse(status="200",
                                       description = "A list of offices.",
                                       content = {
                                           @OpenApiContent(from = OfficeFormatV1.class, type = ""),
                                           @OpenApiContent(from = Office.class, isArray = true,type=Formats.JSONV2),
                                           @OpenApiContent(from = OfficeFormatV1.class, type = Formats.JSON ),
                                           @OpenApiContent(from = TabV1Office.class, type = Formats.TAB ),
                                           @OpenApiContent(from = CsvV1Office.class, type = Formats.CSV )
                                       }
                      ),
                      @OpenApiResponse(status="501",description = "The format requested is not implemented"),
                      @OpenApiResponse(status="400", description = "Invalid Parameter combination")
                    },
        /*
        headers = {
            @OpenApiParam(name="Accept",
                          description="Specifies the encoding format of the response. Valid values are shown in the Response descriptions below"                          
                          )

        },*/
        tags = {"Offices"}
    )
    @Override    
    public void getAll(Context ctx) {        
        getAllRequests.mark();
        
        try (
                final Timer.Context time_context  = getAllRequestsTime.time();
                CwmsDataManager cdm = new CwmsDataManager(ctx);
            ) {                                            
                List<Office> offices = cdm.getOffices();
                String format_parm = ctx.queryParam("format","");
                String format_header = ctx.header(Header.ACCEPT);
                String contentType = FormatFactory.parseHeaderAndQueryParm(format_header, format_parm);                
                OutputFormatter formatter = FormatFactory.formatFor(contentType);
                String result = formatter.format(offices);
                
                ctx.result(result).contentType(formatter.getContentType());
                requestResultSize.update(result.length());
                
        } catch (SQLException ex) {
            logger.log(Level.SEVERE, null, ex);
            ctx.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            ctx.result("Failed to process request");
        }
    }

    @OpenApi(
        pathParams = @OpenApiParam(name="office", description = "The 3 letter office ID you want more information for", type = String.class),
        queryParams = @OpenApiParam(name="format",required = false, description = "Specifies the encoding format of the response. Valid value for the format field for this URI are:\r\n1. tab\r\n2. csv\r\n 3. xml\r\n4. json (default)"),        
        responses = { @OpenApiResponse(status="200" ),
                      @OpenApiResponse(status="501",description = "The format requested is not implemented")
                    },
        tags = {"Offices"}
    )
    @Override
    public void getOne(Context ctx, String office_id) {
        getOneRequest.mark();
        try(
            final Timer.Context time_context = getOneRequestTime.time();
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
