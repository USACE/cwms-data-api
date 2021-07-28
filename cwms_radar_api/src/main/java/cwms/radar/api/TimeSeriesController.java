package cwms.radar.api;

import static com.codahale.metrics.MetricRegistry.name;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.format.DateTimeFormatter;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.http.HttpServletResponse;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

import cwms.radar.api.errors.RadarError;
import cwms.radar.data.CwmsDataManager;
import cwms.radar.data.dto.TimeSeries;
import cwms.radar.formatters.ContentType;
import cwms.radar.formatters.Formats;
import cwms.radar.formatters.FormattingException;
import io.javalin.apibuilder.CrudHandler;
import io.javalin.core.util.Header;
import io.javalin.http.Context;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;

public class TimeSeriesController implements CrudHandler {
    private static final Logger logger = Logger.getLogger(UnitsController.class.getName());


    private final MetricRegistry metrics;// = new MetricRegistry();
    private final Meter getAllRequests;// = metrics.meter(OfficeController.class.getName()+"."+"getAll.count");
    private final Timer getAllRequestsTime;// =metrics.timer(OfficeController.class.getName()+"."+"getAll.time");
    private final Meter getOneRequest;
    private final Timer getOneRequestTime;
    private final Histogram requestResultSize;
    private final int defaultPageSize = 500;

    public TimeSeriesController(MetricRegistry metrics){
        this.metrics=metrics;
        String className = this.getClass().getName();
        getAllRequests = this.metrics.meter(name(className,"getAll","count"));
        getAllRequestsTime = this.metrics.timer(name(className,"getAll","time"));
        getOneRequest = this.metrics.meter(name(className,"getOne","count"));
        getOneRequestTime = this.metrics.timer(name(className,"getOne","time"));
        requestResultSize = this.metrics.histogram((name(className,"results","size")));
    }

    @OpenApi(tags = {"TimeSeries"}, ignore = true)
    @Override
    public void create(Context ctx) {
        ctx.status(HttpServletResponse.SC_NOT_FOUND);
    }

    @OpenApi(tags = {"TimeSeries"}, ignore = true)
    @Override
    public void delete(Context ctx, String id) {
        ctx.status(HttpServletResponse.SC_NOT_FOUND);

    }

    @OpenApi(
        queryParams = {
            @OpenApiParam(name="name", required=true, description="Specifies the name(s) of the time series whose data is to be included in the response. A case insensitive comparison is used to match names."),
            @OpenApiParam(name="office", required=false, description="Specifies the owning office of the location level(s) whose data is to be included in the response. If this field is not specified, matching location level information from all offices shall be returned."),
            @OpenApiParam(name="unit", required=false, description="Specifies the unit or unit system of the response. Valid values for the unit field are:\r\n 1. EN.   (default) Specifies English unit system.  Location level values will be in the default English units for their parameters.\r\n2. SI.   Specifies the SI unit system.  Location level values will be in the default SI units for their parameters.\r\n3. Other. Any unit returned in the response to the units URI request that is appropriate for the requested parameters."),
            @OpenApiParam(name="datum", required=false, description="Specifies the elevation datum of the response. This field affects only elevation location levels. Valid values for this field are:\r\n1. NAVD88.  The elevation values will in the specified or default units above the NAVD-88 datum.\r\n2. NGVD29.  The elevation values will be in the specified or default units above the NGVD-29 datum."),
            @OpenApiParam(name="begin", required=false, description="Specifies the start of the time window for data to be included in the response. If this field is not specified, any required time window begins 24 hours prior to the specified or default end time. The format for this field is ISO 8601 extended, with optional offset and timezone, i.e., 'YYYY-MM-dd'T'hh:mm:ss[Z'['VV']']', e.g., '2021-06-10T13:00:00-0700[PST8PDT]'."),
            @OpenApiParam(name="end", required=false, description="Specifies the end of the time window for data to be included in the response. If this field is not specified, any required time window ends at the current time. The format for this field is ISO 8601 extended, with optional timezone, i.e., 'YYYY-MM-dd'T'hh:mm:ss[Z'['VV']']', e.g., '2021-06-10T13:00:00-0700[PST8PDT]'."),
            @OpenApiParam(name="timezone", required=false, description="Specifies the time zone of the values of the begin and end fields (unless otherwise specified), as well as the time zone of any times in the response. If this field is not specified, the default time zone of UTC shall be used.\r\nIgnored if begin was specified with offset and timezone."),
            @OpenApiParam(name="format", required=false, description="Specifies the encoding format of the response. Valid values for the format field for this URI are:\r\n1.    tab\r\n2.    csv\r\n3.    xml\r\n4.  wml2 (only if name field is specified)\r\n5.    json (default)"),
            @OpenApiParam(name="page",
                          required = false,
                          description = "This end point can return a lot of data, this identifies where in the request you are. This is an opaque value, and can be obtained from the 'next-page' value in the response."
            ),
            @OpenApiParam(name="pageSize",
                          required=false,
                          type=Integer.class,
                          description = "How many entries per page returned. Default " + defaultPageSize + "."
            )
        },
        responses = { @OpenApiResponse(status="200",
                                       description = "A list of elements of the data set you've selected.",
                                       content = {
                                           @OpenApiContent(from = TimeSeries.class, type=Formats.JSONV2),
                                           @OpenApiContent(from = TimeSeries.class, type=Formats.XML)
                                       }
                      ),
                      @OpenApiResponse(status="400", description = "Invalid parameter combination"),
                      @OpenApiResponse(status="404", description = "The provided combination of parameters did not find a timeseries."),
                      @OpenApiResponse(status="501",description = "Requested format is not implemented")
                    },
        tags = {"TimeSeries"}
    )
    @Override
    public void getAll(Context ctx) {
        getAllRequests.mark();
        try (
            final Timer.Context time_context = getAllRequestsTime.time();
            CwmsDataManager cdm = new CwmsDataManager(ctx);
        ) {
            String format = ctx.queryParam("format","");
            String names = ctx.queryParam("name");
            String office = ctx.queryParam("office");
            String unit = ctx.queryParam("unit", "EN");
            String datum = ctx.queryParam("datum");
            String begin = ctx.queryParam("begin");
            String end = ctx.queryParam("end");
            String timezone = ctx.queryParam("timezone");
            // The following parameters are only used for jsonv2 and xmlv2
            String cursor = ctx.queryParam("cursor",String.class,ctx.queryParam("page",String.class,"").getValue()).getValue();
            int pageSize = ctx.queryParam("pageSize",Integer.class,ctx.queryParam("pagesize",String.class,Integer.toString(defaultPageSize)).getValue()).getValue();

            String acceptHeader = ctx.header(Header.ACCEPT);
            ContentType contentType = Formats.parseHeaderAndQueryParm(acceptHeader, format);

            String results;
            String version = contentType.getParameters().get("version");
            if(version != null && version.equals("2")) {
                TimeSeries ts = cdm.getTimeseries(cursor, pageSize, names, office, unit, datum, begin, end, timezone);

                results = Formats.format(contentType, ts);
                ctx.status(HttpServletResponse.SC_OK);

                // Send back the link to the next page in the response header
                StringBuffer linkValue = new StringBuffer(600);
                linkValue.append(String.format("<%s>; rel=self; type=\"%s\"", buildRequestUrl(ctx, ts, ts.getPage()), contentType.toString()));

                if(ts.getNextPage() != null) {
                    linkValue.append(",");
                    linkValue.append(String.format("<%s>; rel=next; type=\"%s\"", buildRequestUrl(ctx, ts, ts.getNextPage()), contentType.toString()));
                }

                ctx.header("Link", linkValue.toString());
                ctx.result(results).contentType(contentType.toString());
            }
            else {
                results = cdm.getTimeseries(format == null || format.isEmpty() ? "json" : format,names,office,unit,datum,begin,end,timezone);
                ctx.status(HttpServletResponse.SC_OK);
                ctx.result(results);
            }
            requestResultSize.update(results.length());
        } catch (IllegalArgumentException ex) {
            RadarError re = new RadarError("Invalid arguments supplied");
            logger.log(Level.SEVERE, re.toString(), ex);
            ctx.status(HttpServletResponse.SC_BAD_REQUEST);
            ctx.json(re);
        } catch (SQLException ex) {
            RadarError re = new RadarError("Failed to ProcessRequest");
            logger.log(Level.SEVERE, re.toString(), ex);
            ctx.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            ctx.json(re);
        }
    }

    @OpenApi(tags = {"TimeSeries"}, ignore = true)
    @Override
    public void getOne(Context ctx, String id) {
        getOneRequest.mark();
        try( final Timer.Context time_context = getOneRequestTime.time(); ){

            throw new UnsupportedOperationException("Not supported yet.");
        }

    }

    @OpenApi(tags = {"TimeSeries"}, ignore = true)
    @Override
    public void update(Context ctx, String id) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    /**
     * Builds a URL that references a specific "page" of the result.
     * @param ctx
     * @param ts
     * @return
     */
    private String buildRequestUrl(Context ctx, TimeSeries ts, String cursor)
    {
        StringBuffer result = ctx.req.getRequestURL();
        try {
            result.append(String.format("?name=%s", URLEncoder.encode(ts.getName(), StandardCharsets.UTF_8.toString())));
            result.append(String.format("&office=%s", URLEncoder.encode(ts.getOfficeId(), StandardCharsets.UTF_8.toString())));
            result.append(String.format("&unit=%s", URLEncoder.encode(ts.getUnits(), StandardCharsets.UTF_8.toString())));
            result.append(String.format("&begin=%s", URLEncoder.encode(ts.getBegin().format(DateTimeFormatter.ISO_ZONED_DATE_TIME), StandardCharsets.UTF_8.toString())));
            result.append(String.format("&end=%s", URLEncoder.encode(ts.getEnd().format(DateTimeFormatter.ISO_ZONED_DATE_TIME), StandardCharsets.UTF_8.toString())));

            String format = ctx.queryParam("format");
            if(format != null && !format.isEmpty())
                result.append(String.format("&format=%s", format));

            if(cursor != null && !cursor.isEmpty())
                result.append(String.format("&page=%s", URLEncoder.encode(cursor, StandardCharsets.UTF_8.toString())));
        } catch (UnsupportedEncodingException ex) {
            // We shouldn't get here
            logger.log(Level.WARNING, null, ex);
        }
        return result.toString();
    }
}
