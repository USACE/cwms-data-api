package cwms.radar.api;

import java.io.IOException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.servlet.http.HttpServletResponse;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.radar.data.dao.OfficeDao;
import cwms.radar.data.dto.Office;
import cwms.radar.formatters.ContentType;
import cwms.radar.formatters.Formats;
import cwms.radar.formatters.FormattingException;
import cwms.radar.formatters.OfficeFormatV1;
import cwms.radar.formatters.csv.CsvV1Office;
import cwms.radar.formatters.tab.TabV1Office;
import io.javalin.apibuilder.CrudHandler;
import io.javalin.core.util.Header;
import io.javalin.http.Context;
import io.javalin.http.NotFoundResponse;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import org.jooq.DSLContext;

import static com.codahale.metrics.MetricRegistry.name;
import static cwms.radar.data.dao.JooqDao.getDslContext;

/**
 *
 */
public class OfficeController implements CrudHandler {
    private static final Logger logger = Logger.getLogger(OfficeController.class.getName());
    private final MetricRegistry metrics;
    private final Meter getAllRequests;
    private final Timer getAllRequestsTime;
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
                                    description = "(Deprecated in favor of Accept header) Specifies the encoding format of the response. Valid value for the format field for this URI are:\r\n1. tab\r\n2. csv\r\n 3. xml\r\n4. json (default)"
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
                final Timer.Context timeContext  = getAllRequestsTime.time();
                DSLContext dsl = getDslContext(ctx))
        {
                OfficeDao dao = new OfficeDao(dsl);
                List<Office> offices = dao.getOffices();
                String formatParm = ctx.queryParam("format","");
                String formatHeader = ctx.header(Header.ACCEPT);
                ContentType contentType = Formats.parseHeaderAndQueryParm(formatHeader, formatParm);
                
                String result = Formats.format(contentType,offices);
                
                ctx.result(result).contentType(contentType.toString());
                requestResultSize.update(result.length());

        } catch ( FormattingException fe ){
            logger.log(Level.SEVERE,"failed to format data",fe);
            if( fe.getCause() instanceof IOException ){
                ctx.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                ctx.result("server error");
            } else {
                ctx.status(HttpServletResponse.SC_BAD_REQUEST);
                ctx.result("Invalid Format Options");
            }
        }
    }

    @OpenApi(
        pathParams = @OpenApiParam(name="office", description = "The 3 letter office ID you want more information for", type = String.class),
        queryParams = @OpenApiParam(name="format",
                                    required = false, 
                                    deprecated = true,
                                    description = "(Deprecated in favor of Accept header) Specifies the encoding format of the response. Valid value for the format field for this URI are:\r\n1. tab\r\n2. csv\r\n 3. xml\r\n4. json (default)"
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
        tags = {"Offices"}
    )
    @Override
    public void getOne(Context ctx, String officeId) {
        getOneRequest.mark();
        try(
            final Timer.Context timeContext = getOneRequestTime.time();
            DSLContext dsl = getDslContext(ctx))
        {
            OfficeDao dao = new OfficeDao(dsl);
            Office office = dao.getOfficeById(officeId);
            if( office == null ){
                throw new NotFoundResponse("Office id: " + officeId + " does not exist");
            }
            String formatParm = ctx.queryParam("format","");
            String formatHeader = ctx.header(Header.ACCEPT);
            ContentType contentType = Formats.parseHeaderAndQueryParm(formatHeader, formatParm);
            String result = Formats.format(contentType,office);
            ctx.result(result).contentType(contentType.toString());

            requestResultSize.update(result.length());
        } catch( FormattingException fe ){
            logger.log(Level.SEVERE,"failed to format data",fe);
            if( fe.getCause() instanceof IOException ){
                ctx.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                ctx.result("server error");
            } else {
                ctx.status(HttpServletResponse.SC_BAD_REQUEST);
                ctx.result("Invalid Format Options");
            }
        }
    }

    @OpenApi(ignore = true)
    @Override
    public void create(Context ctx) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @OpenApi(ignore = true)
    @Override
    public void update(Context ctx, String officeId) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @OpenApi(ignore = true)
    @Override
    public void delete(Context ctx, String officeId) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

}
