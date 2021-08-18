package cwms.radar.api;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import cwms.radar.api.errors.RadarError;
import cwms.radar.data.dao.BasinDao;
import cwms.radar.data.dao.LocationsDao;
import cwms.radar.formatters.ContentType;
import cwms.radar.formatters.Formats;
import io.javalin.apibuilder.CrudHandler;
import io.javalin.core.util.Header;
import io.javalin.http.Context;
import io.javalin.plugin.json.JavalinJackson;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import org.geojson.FeatureCollection;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;
import usace.cwms.db.dao.ifc.basin.CwmsDbBasin;
import usace.cwms.db.jooq.codegen.packages.CWMS_BASIN_PACKAGE;
import usace.cwms.db.jooq.dao.CwmsDbBasinJooq;

import static com.codahale.metrics.MetricRegistry.name;
import static cwms.radar.data.dao.JooqDao.getDslContext;
import javax.servlet.http.HttpServletResponse;
import java.sql.Connection;
import java.util.logging.Level;
import java.util.logging.Logger;
public class BasinController implements CrudHandler
{
    private static final Logger LOGGER = Logger.getLogger(LocationController.class.getName());
    private final MetricRegistry _metrics;
    private final Meter _getAllRequests;
    private final Timer _getAllRequestsTime;
    private final Meter _getOneRequest;
    private final Timer _getOneRequestTime;
    private final Histogram _requestResultSize;

    public BasinController(MetricRegistry metrics)
    {
        _metrics = metrics;
        String className = this.getClass().getName();
        _getAllRequests = _metrics.meter(name(className,"getAll","count"));
        _getAllRequestsTime = _metrics.timer(name(className, "getAll", "time"));
        _getOneRequest = _metrics.meter(name(className,"getOne","count"));
        _getOneRequestTime = _metrics.timer(name(className, "getOne", "time"));
        _requestResultSize = _metrics.histogram((name(className,"results","size")));
    }
    @OpenApi(
            queryParams = {
                    @OpenApiParam(name="name", required=false, description="Specifies the name(s) of the time series whose data is to be included in the response. A case insensitive comparison is used to match names."),
                    @OpenApiParam(name="office", required=false, description="Specifies the owning office of the location level(s) whose data is to be included in the response. If this field is not specified, matching location level information from all offices shall be returned."),
                    @OpenApiParam(name="unit", required=false, description="Specifies the unit or unit system of the response. Valid values for the unit field are:\r\n 1. EN.   Specifies English unit system.  Location level values will be in the default English units for their parameters.\r\n2. SI.   Specifies the SI unit system.  Location level values will be in the default SI units for their parameters.\r\n3. Other. Any unit returned in the response to the units URI request that is appropriate for the requested parameters."),
                    @OpenApiParam(name="datum", required=false, description="Specifies the elevation datum of the response. This field affects only elevation location levels. Valid values for this field are:\r\n1. NAVD88.  The elevation values will in the specified or default units above the NAVD-88 datum.\r\n2. NGVD29.  The elevation values will be in the specified or default units above the NGVD-29 datum."),
                    @OpenApiParam(name="timezone", required=false, description="Specifies the time zone of the values of the begin and end fields (unless otherwise specified), as well as the time zone of any times in the response. If this field is not specified, the default time zone of UTC shall be used."),
                    @OpenApiParam(name="format", required=false, description="Specifies the encoding format of the response. Valid values for the format field for this URI are:\r\n1.    tab\r\n2.    csv\r\n3.    xml\r\n4.    json (default)")
            },
            responses = {
                    @OpenApiResponse(status="200",
                            content = {
                                    @OpenApiContent(type = Formats.JSON ),
                                    @OpenApiContent(type = Formats.TAB ),
                                    @OpenApiContent(type = Formats.CSV ),
                                    @OpenApiContent(type = Formats.XML ),
                                    @OpenApiContent(type = Formats.WML2),
                                    @OpenApiContent(type = Formats.GEOJSON )
                            }),
                    @OpenApiResponse(status="404", description = "The provided combination of parameters did not find a basin."),
                    @OpenApiResponse(status="501", description = "Requested format is not implemented")
            },
            description = "Returns CWMS Basin Data",
            tags = {"Basins"}
    )
    @Override
    public void getAll(@NotNull Context ctx)
    {
        _getAllRequests.mark();
        try(final Timer.Context timeContext = _getAllRequestsTime.time();
            DSLContext dsl = getDslContext(ctx))
        {
            String names = ctx.queryParam("names");
            String units = ctx.queryParam("unit");
            String datum = ctx.queryParam("datum");
            String office = ctx.queryParam("office");
            String formatParm = ctx.queryParam("format", "");
            String formatHeader = ctx.header(Header.ACCEPT);
            ContentType contentType = Formats.parseHeaderAndQueryParm(formatHeader, formatParm);
            ctx.contentType(contentType.toString());
            BasinDao basinDao = new BasinDao(dsl);
        }
    }

    @OpenApi(ignore = true)
    @Override
    public void create(@NotNull Context ctx)
    {
        ctx.status(HttpServletResponse.SC_NOT_IMPLEMENTED).json(RadarError.notImplemented());
    }
    @OpenApi(ignore = true)
    @Override
    public void delete(@NotNull Context ctx, @NotNull String s)
    {
        ctx.status(HttpServletResponse.SC_NOT_IMPLEMENTED).json(RadarError.notImplemented());
    }
    @Override
    public void getOne(@NotNull Context ctx, @NotNull String s)
    {
        _getOneRequest.mark();
        try (final Timer.Context timeContext = _getOneRequestTime.time())
        {
            ctx.status(HttpServletResponse.SC_NOT_IMPLEMENTED).json(RadarError.notImplemented());
        }
    }
    @OpenApi(ignore = true)
    @Override
    public void update(@NotNull Context ctx, @NotNull String s)
    {
        ctx.status(HttpServletResponse.SC_NOT_IMPLEMENTED).json(RadarError.notImplemented());
    }
}