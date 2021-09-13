package cwms.radar.api;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.radar.api.enums.UnitSystem;
import cwms.radar.api.errors.RadarError;
import cwms.radar.data.dao.BasinDao;
import cwms.radar.data.dto.basinconnectivity.Basin;
import cwms.radar.formatters.ContentType;
import cwms.radar.formatters.Formats;
import cwms.radar.formatters.json.NamedPgJsonFormatter;
import io.javalin.apibuilder.CrudHandler;
import io.javalin.core.util.Header;
import io.javalin.http.Context;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import org.eclipse.jetty.http.HttpStatus;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;

import static com.codahale.metrics.MetricRegistry.name;
import static cwms.radar.data.dao.JooqDao.getDslContext;
import javax.servlet.http.HttpServletResponse;
import java.sql.SQLException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
public class BasinController implements CrudHandler
{
    private static final Logger LOGGER = Logger.getLogger(BasinController.class.getName());
    private final MetricRegistry metrics;
    private final Meter getAllRequests;
    private final Timer getAllRequestsTime;
    private final Meter getOneRequest;
    private final Timer getOneRequestTime;
    private final Histogram requestResultSize;

    public BasinController(MetricRegistry metrics)
    {
        this.metrics = metrics;
        String className = this.getClass().getName();
        getAllRequests = metrics.meter(name(className,"getAll","count"));
        getAllRequestsTime = metrics.timer(name(className, "getAll", "time"));
        getOneRequest = metrics.meter(name(className,"getOne","count"));
        getOneRequestTime = metrics.timer(name(className, "getOne", "time"));
        requestResultSize = metrics.histogram((name(className,"results","size")));
    }
    @OpenApi(
            queryParams = {
                    @OpenApiParam(name="office", required=false, description="Specifies the owning office of the basin whose data is to be included in the response. If this field is not specified, matching basin information from all offices shall be returned."),
                    @OpenApiParam(name="unit", required=false, description="Specifies the unit or unit system of the response. Valid values for the unit field are:\r\n 1. EN.   Specifies English unit system. Basin values will be in the default English units for their parameters. (This is default if no value is entered)\r\n2. SI.   Specifies the SI unit system. Basin values will be in the default SI units for their parameters."),
            },
            responses = {
                    @OpenApiResponse(status="200",
                            content = {
                                    @OpenApiContent(from = Basin.class, type = Formats.NAMED_PGJSON)
                            }),
                    @OpenApiResponse(status="404", description = "The provided combination of parameters did not find a basin."),
                    @OpenApiResponse(status="501", description = "Requested format is not implemented")
            },
            description = "Returns CWMS Basin Data",
            tags = {"Basins-Beta"}
    )
    @Override
    public void getAll(@NotNull Context ctx)
    {
        getAllRequests.mark();
        try(final Timer.Context timeContext = getAllRequestsTime.time();
            DSLContext dsl = getDslContext(ctx))
        {
            String units = ctx.queryParam("unit", UnitSystem.EN.value());
            String office = ctx.queryParam("office");
            String formatHeader = ctx.header(Header.ACCEPT) != null ? ctx.header(Header.ACCEPT) : Formats.NAMED_PGJSON;
            ContentType contentType = Formats.parseHeader(formatHeader);
            ctx.contentType(contentType.toString());
            BasinDao basinDao = new BasinDao(dsl);
            List<Basin> basins = basinDao.getAllBasins(units, office);
            if(contentType.getType().equals(Formats.NAMED_PGJSON))
            {
                NamedPgJsonFormatter basinPgJsonFormatter = new NamedPgJsonFormatter();
                String result = basinPgJsonFormatter.format(basins);
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
            },
            responses = {
                    @OpenApiResponse(status="200",
                            content = {
                                    @OpenApiContent(from = Basin.class, type = Formats.NAMED_PGJSON)
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
        getOneRequest.mark();
        try(final Timer.Context timeContext = getAllRequestsTime.time();
            DSLContext dsl = getDslContext(ctx))
        {
            String units = ctx.queryParam("unit", UnitSystem.EN.value());
            String office = ctx.queryParam("office");
            String formatHeader = ctx.header(Header.ACCEPT) != null ? ctx.header(Header.ACCEPT) : Formats.NAMED_PGJSON;
            ContentType contentType = Formats.parseHeader(formatHeader);
            ctx.contentType(contentType.toString());
            BasinDao basinDao = new BasinDao(dsl);
            Basin basin = basinDao.getBasin(basinId, units, office);
            if(contentType.getType().equals(Formats.NAMED_PGJSON))
            {
                NamedPgJsonFormatter basinPgJsonFormatter = new NamedPgJsonFormatter();
                String result = basinPgJsonFormatter.format(basin);
                ctx.result(result);
            }
            else
            {
                ctx.status(HttpStatus.NOT_FOUND_404).json(new RadarError("Unsupported format for basins"));
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