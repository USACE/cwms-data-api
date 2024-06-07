package cwms.cda.api;

import static cwms.cda.api.Controllers.GET_ALL;
import static cwms.cda.api.Controllers.GET_ONE;
import static cwms.cda.api.Controllers.OFFICE;
import static cwms.cda.api.Controllers.STATUS_200;
import static cwms.cda.api.Controllers.STATUS_404;
import static cwms.cda.api.Controllers.STATUS_501;
import static cwms.cda.api.Controllers.UNIT;
import static cwms.cda.data.dao.JooqDao.getDslContext;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.cda.api.enums.UnitSystem;
import cwms.cda.api.errors.CdaError;
import cwms.cda.data.dao.BasinDao;
import cwms.cda.data.dto.basinconnectivity.Basin;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.FormattingException;
import cwms.cda.formatters.json.NamedPgJsonFormatter;
import io.javalin.apibuilder.CrudHandler;
import io.javalin.core.util.Header;
import io.javalin.http.Context;
import io.javalin.http.HttpCode;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import java.sql.SQLException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.servlet.http.HttpServletResponse;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;

public class BasinController implements CrudHandler {
    private static final Logger LOGGER = Logger.getLogger(BasinController.class.getName());
    public static final String TAG = "Basins";


    private final MetricRegistry metrics;


    public BasinController(MetricRegistry metrics) {
        this.metrics = metrics;
    }

    private Timer.Context markAndTime(String subject) {
        return Controllers.markAndTime(metrics, getClass().getName(), subject);
    }

    @OpenApi(
            queryParams = {
                @OpenApiParam(name = OFFICE, description = "Specifies the"
                        + " owning office of the basin whose data is to be included in the "
                        + "response. If this field is not specified, matching basin "
                        + "information from all offices shall be returned."),
                @OpenApiParam(name = UNIT, description = "Specifies the "
                        + "unit or unit system of the response. Valid values for the unit "
                        + "field are: "
                        + "\n* `EN`  Specifies English unit system. Basin "
                        + "values will be in the default English units for their parameters. "
                        + "(This is default if no value is entered)"
                        + "\n* `SI`  Specifies the"
                        + " SI unit system. Basin values will be in the default SI units for "
                        + "their parameters."),
            },
            responses = {
                @OpenApiResponse(status = STATUS_200,
                        content = {
                            @OpenApiContent(from = Basin.class, type = Formats.NAMED_PGJSON)
                        }),
                @OpenApiResponse(status = STATUS_404, description = "The provided combination of "
                        + "parameters did not find a basin."),
                @OpenApiResponse(status = STATUS_501, description = "Requested format is not "
                        + "implemented")
            },
            description = "Returns CWMS Basin Data",
            tags = {TAG}
    )
    @Override
    public void getAll(@NotNull Context ctx) {

        try (final Timer.Context timeContext = markAndTime(GET_ALL)) {
            DSLContext dsl = getDslContext(ctx);
            String units =
                    ctx.queryParamAsClass(UNIT, String.class).getOrDefault(UnitSystem.EN.value());
            String office = ctx.queryParam(OFFICE);
            String formatHeader = ctx.header(Header.ACCEPT) != null ? ctx.header(Header.ACCEPT) :
                    Formats.NAMED_PGJSON;
            ContentType contentType = Formats.parseHeader(formatHeader);
            ctx.contentType(contentType.toString());
            BasinDao basinDao = new BasinDao(dsl);
            List<Basin> basins = basinDao.getAllBasins(units, office);
            if (contentType.getType().equals(Formats.NAMED_PGJSON)) {
                NamedPgJsonFormatter basinPgJsonFormatter = new NamedPgJsonFormatter();
                String result = basinPgJsonFormatter.format(basins);
                ctx.result(result);
            }
        } catch (SQLException ex) {
            LOGGER.log(Level.SEVERE, "Error retrieving all basins", ex);
        }
    }

    @OpenApi(
            queryParams = {
                @OpenApiParam(name = OFFICE, description = "Specifies the"
                        + " owning office of the basin whose data is to be included in the "
                        + "response. If this field is not specified, matching basin "
                        + "information from all offices shall be returned."),
                @OpenApiParam(name = UNIT, description = "Specifies the "
                        + "unit or unit system of the response. Valid values for the unit "
                        + "field are:"
                        + "\n* `EN`  Specifies English unit system. Basin values will be in "
                        + "the default English units for their parameters. "
                        + "(This is default if no value is entered)"
                        + "\n* `SI`  Specifies the SI unit system. Basin values will be in "
                        + "the default SI units for "
                        + "their parameters."),
            },
            responses = {
                @OpenApiResponse(status = STATUS_200,
                        content = {
                            @OpenApiContent(from = Basin.class, type = Formats.NAMED_PGJSON)
                        }),
                @OpenApiResponse(status = STATUS_404, description = "The provided combination of "
                        + "parameters did not find a basin."),
                @OpenApiResponse(status = STATUS_501, description = "Requested format is not "
                        + "implemented")
            },
            description = "Returns CWMS Basin Data",
            tags = {TAG}
    )
    @Override
    public void getOne(@NotNull Context ctx, @NotNull String basinId) {

        try (final Timer.Context timeContext = markAndTime(GET_ONE)) {
            DSLContext dsl = getDslContext(ctx);
            String units =
                    ctx.queryParamAsClass(UNIT, String.class).getOrDefault(UnitSystem.EN.value());
            String office = ctx.queryParam(OFFICE);
            String formatHeader = ctx.header(Header.ACCEPT) != null ? ctx.header(Header.ACCEPT) :
                    Formats.NAMED_PGJSON;
            ContentType contentType = Formats.parseHeader(formatHeader);
            ctx.contentType(contentType.toString());
            BasinDao basinDao = new BasinDao(dsl);
            Basin basin = basinDao.getBasin(basinId, units, office);
            if (contentType.getType().equals(Formats.NAMED_PGJSON)) {
                NamedPgJsonFormatter basinPgJsonFormatter = new NamedPgJsonFormatter();
                String result = basinPgJsonFormatter.format(basin);
                ctx.result(result);
            } else {
                ctx.status(HttpServletResponse.SC_NOT_FOUND).json(new CdaError("Unsupported "
                        + "format for basins"));
            }
        } catch (SQLException ex) {
            String errorMsg = "Error retrieving " + basinId;
            LOGGER.log(Level.SEVERE, errorMsg, ex);
        }
    }

    @OpenApi(ignore = true)
    @Override
    public void update(@NotNull Context ctx, @NotNull String s) {
        ctx.status(HttpCode.NOT_IMPLEMENTED).json(CdaError.notImplemented());
    }

    @OpenApi(ignore = true)
    @Override
    public void create(@NotNull Context ctx) {
        ctx.status(HttpCode.NOT_IMPLEMENTED).json(CdaError.notImplemented());
    }

    @OpenApi(ignore = true)
    @Override
    public void delete(@NotNull Context ctx, @NotNull String s) {
        ctx.status(HttpCode.NOT_IMPLEMENTED).json(CdaError.notImplemented());
    }
}