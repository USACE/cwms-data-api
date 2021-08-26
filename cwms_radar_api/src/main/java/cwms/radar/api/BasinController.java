package cwms.radar.api;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.radar.api.errors.RadarError;
import cwms.radar.data.dao.BasinDao;
import cwms.radar.data.dto.basinconnectivity.Basin;
import cwms.radar.formatters.ContentType;
import cwms.radar.formatters.Formats;
import cwms.radar.formatters.FormattingException;
import cwms.radar.formatters.json.PgJsonFormatter;
import io.javalin.apibuilder.CrudHandler;
import io.javalin.core.util.Header;
import io.javalin.http.Context;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;

import static com.codahale.metrics.MetricRegistry.name;
import static cwms.radar.data.dao.JooqDao.getDslContext;
import javax.servlet.http.HttpServletResponse;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
public class BasinController implements CrudHandler
{
    private static final Logger LOGGER = Logger.getLogger(BasinController.class.getName());
    private static final List<String> SUPPORTED_CONTENT_TYPES = Arrays.asList
        (
            "json"
        );
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
                    @OpenApiParam(name="office", required=false, description="Specifies the owning office of the basin whose data is to be included in the response. If this field is not specified, matching basin information from all offices shall be returned."),
                    @OpenApiParam(name="unit", required=false, description="Specifies the unit or unit system of the response. Valid values for the unit field are:\r\n 1. EN.   Specifies English unit system. Basin values will be in the default English units for their parameters. (This is default if no value is entered)\r\n2. SI.   Specifies the SI unit system. Basin values will be in the default SI units for their parameters."),
                    @OpenApiParam(name="format", required=false, description="Specifies the encoding format of the response. Valid values for the format field for this URI are:\r\n 1. json (returns a PG-JSON graph. This is default if no value is entered)")
            },
            responses = {
                    @OpenApiResponse(status="200",
                            content = {
                                    @OpenApiContent(type = Formats.JSON)
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
            String units = ctx.queryParam("unit");
            String office = ctx.queryParam("office");
            String formatParam = ctx.queryParam("format", "");
            if(units == null)
            {
                units = "EN";
            }
            if(formatParam == null)
            {
                formatParam = "json";
            }
            if(!SUPPORTED_CONTENT_TYPES.contains(formatParam))
            {
                throw new FormattingException("content-type " + formatParam + " is not implemented");
            }
            String formatHeader = ctx.header(Header.ACCEPT);
            ContentType contentType = Formats.parseHeaderAndQueryParm(formatHeader, formatParam);
            ctx.contentType(contentType.toString());
            BasinDao basinDao = new BasinDao(dsl);
            List<Basin> basins = basinDao.getAllBasins(units, office);
            if(contentType.getType().equals(Formats.JSON))
            {
                PgJsonFormatter pgJsonFormatter = new PgJsonFormatter();
                String result = pgJsonFormatter.format(basins);
                ctx.result(result);
            }
        }
        catch (SQLException ex)
        {
            LOGGER.log(Level.SEVERE, "Error retrieving all basins", ex);
        }
    }

    @OpenApi(
            queryParams = {
                    @OpenApiParam(name="office", required=false, description="Specifies the owning office of the basin whose data is to be included in the response. If this field is not specified, matching basin information from all offices shall be returned."),
                    @OpenApiParam(name="unit", required=false, description="Specifies the unit or unit system of the response. Valid values for the unit field are:\r\n 1. EN.   Specifies English unit system. Basin values will be in the default English units for their parameters. (This is default if no value is entered)\r\n2. SI.   Specifies the SI unit system. Basin values will be in the default SI units for their parameters."),
                    @OpenApiParam(name="format", required=false, description="Specifies the encoding format of the response. Valid values for the format field for this URI are: \r\n 1. json (returns a PG-JSON graph. This is default if no value is entered)")
            },
            responses = {
                    @OpenApiResponse(status="200",
                            content = {
                                    @OpenApiContent(type = Formats.JSON)
                            }),
                    @OpenApiResponse(status="404", description = "The provided combination of parameters did not find a basin."),
                    @OpenApiResponse(status="501", description = "Requested format is not implemented")
            },
            description = "Returns CWMS Basin Data",
            tags = {"Basins"}
    )
    @Override
    public void getOne(@NotNull Context ctx, @NotNull String basinId)
    {
        _getOneRequest.mark();
        try(final Timer.Context timeContext = _getAllRequestsTime.time();
            DSLContext dsl = getDslContext(ctx))
        {
            String units = ctx.queryParam("unit");
            String office = ctx.queryParam("office");
            String formatParam = ctx.queryParam("format", "");
            if(units == null)
            {
                units = "EN";
            }
            if(formatParam == null)
            {
                formatParam = "json";
            }
            if(!SUPPORTED_CONTENT_TYPES.contains(formatParam))
            {
                throw new FormattingException("content-type " + formatParam + " is not implemented");
            }
            String formatHeader = ctx.header(Header.ACCEPT);
            ContentType contentType = Formats.parseHeaderAndQueryParm(formatHeader, formatParam);
            ctx.contentType(contentType.toString());
            BasinDao basinDao = new BasinDao(dsl);
            Basin basin = basinDao.getBasin(basinId, units, office);
            if(contentType.getType().equals(Formats.JSON))
            {
                PgJsonFormatter pgJsonFormatter = new PgJsonFormatter();
                String result = pgJsonFormatter.format(basin);
                ctx.result(result);
            }
        }
        catch (SQLException ex)
        {
            String errorMsg = "Error retrieving " + basinId;
            LOGGER.log(Level.SEVERE, errorMsg, ex);
        }
    }
    @OpenApi(ignore = true)
    @Override
    public void update(@NotNull Context ctx, @NotNull String s)
    {
        ctx.status(HttpServletResponse.SC_NOT_IMPLEMENTED).json(RadarError.notImplemented());
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
}