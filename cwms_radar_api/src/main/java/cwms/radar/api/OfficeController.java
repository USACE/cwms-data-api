package cwms.radar.api;

import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.logging.Logger;
import javax.servlet.http.HttpServletResponse;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

import cwms.radar.api.errors.RadarError;
import cwms.radar.data.dao.OfficeDao;
import cwms.radar.data.dto.Office;
import cwms.radar.formatters.ContentType;
import cwms.radar.formatters.Formats;
import cwms.radar.formatters.OfficeFormatV1;
import cwms.radar.formatters.csv.CsvV1Office;
import cwms.radar.formatters.tab.TabV1Office;
import io.javalin.apibuilder.CrudHandler;
import io.javalin.core.util.Header;
import io.javalin.http.Context;
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
                                           @OpenApiContent(from = CsvV1Office.class, type = Formats.CSV ),
                                           @OpenApiContent(from = CsvV1Office.class, type = Formats.XML)
                                       }
                      ),
                    },
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
                String formatParm = ctx.queryParamAsClass("format",String.class).getOrDefault("");
                String formatHeader = ctx.header(Header.ACCEPT);
                ContentType contentType = Formats.parseHeaderAndQueryParm(formatHeader, formatParm);

                String result = Formats.format(contentType,offices);

                ctx.result(result).contentType(contentType.toString());
                requestResultSize.update(result.length());

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
                                           @OpenApiContent(from = CsvV1Office.class, type = Formats.CSV ),
                                           @OpenApiContent(from = CsvV1Office.class, type = Formats.XML)
                                       }
                      )
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
            Optional<Office> office = dao.getOfficeById(officeId);
            if( office.isPresent() ){
                String formatParm = ctx.queryParamAsClass("format",String.class).getOrDefault("");
                String formatHeader = ctx.header(Header.ACCEPT);
                ContentType contentType = Formats.parseHeaderAndQueryParm(formatHeader, formatParm);
                String result = Formats.format(contentType,office.get());
                ctx.result(result).contentType(contentType.toString());

                requestResultSize.update(result.length());
            }
            else {
                RadarError re = new RadarError("Not Found", new HashMap<String,String>(){
                    {
                        put("office", "An office with that name does not exist");
                    }

                });
                ctx.status(HttpServletResponse.SC_NOT_FOUND).json(re);
            }

        }
    }

    @OpenApi(ignore = true)
    @Override
    public void create(Context ctx) {
        ctx.status(HttpServletResponse.SC_NOT_IMPLEMENTED).json(RadarError.notImplemented());
    }

    @OpenApi(ignore = true)
    @Override
    public void update(Context ctx, String officeId) {
        ctx.status(HttpServletResponse.SC_NOT_IMPLEMENTED).json(RadarError.notImplemented());
    }

    @OpenApi(ignore = true)
    @Override
    public void delete(Context ctx, String officeId) {
        ctx.status(HttpServletResponse.SC_NOT_IMPLEMENTED).json(RadarError.notImplemented());
    }

}
