package cwms.cda.api;

import static com.codahale.metrics.MetricRegistry.name;
import static cwms.cda.api.Controllers.ACCEPT;
import static cwms.cda.api.Controllers.FORMAT;
import static cwms.cda.api.Controllers.GET_ALL;
import static cwms.cda.api.Controllers.GET_ONE;
import static cwms.cda.api.Controllers.RESULTS;
import static cwms.cda.api.Controllers.SIZE;
import static cwms.cda.api.Controllers.STATUS_200;
import static cwms.cda.api.Controllers.STATUS_501;
import static cwms.cda.api.Controllers.VERSION;
import static cwms.cda.api.Controllers.addDeprecatedContentTypeWarning;
import static cwms.cda.data.dao.JooqDao.getDslContext;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.flogger.FluentLogger;
import cwms.cda.api.errors.CdaError;
import cwms.cda.data.dao.UnitsDao;
import cwms.cda.data.dto.Unit;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import io.javalin.apibuilder.CrudHandler;
import io.javalin.core.util.Header;
import io.javalin.http.Context;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import java.io.IOException;
import java.util.List;
import javax.servlet.http.HttpServletResponse;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;

public class UnitsController implements CrudHandler {
    private static final FluentLogger LOGGER = FluentLogger.forEnclosingClass();

    private final MetricRegistry metrics;

    private final Histogram requestResultSize;

    public UnitsController(MetricRegistry metrics) {
        this.metrics = metrics;
        String className = this.getClass().getName();

        requestResultSize = this.metrics.histogram((name(className, RESULTS, SIZE)));
    }

    private Timer.Context markAndTime(String subject) {
        return Controllers.markAndTime(metrics, getClass().getName(), subject);
    }

    @OpenApi(ignore = true)
    @Override
    public void create(@NotNull Context ctx) {
        ctx.status(HttpServletResponse.SC_NOT_IMPLEMENTED).json(CdaError.notImplemented());
    }

    @OpenApi(ignore = true)
    @Override
    public void delete(@NotNull Context ctx, @NotNull String unit) {
        ctx.status(HttpServletResponse.SC_NOT_IMPLEMENTED).json(CdaError.notImplemented());
    }

    @OpenApi(
        queryParams = {
            @OpenApiParam(name = FORMAT, description = "Specifies the"
                + " encoding format of the response. Valid value for the format field"
                + " for this URI are:"
                + "\n* `tab`"
                + "\n* `csv`"
                + "\n* `xml`"
                + "\n* `json` (default)"
                + "\n\nSee <a href=\"legacy-format/\">this page</a> for more"
                + " information about accept header usage.")
        },
        responses = {
            @OpenApiResponse(status = STATUS_200),
            @OpenApiResponse(status = STATUS_501, description = "The format requested is not "
                + "implemented")
        },
        tags = {"Units"}
    )
    @Override
    public void getAll(@NotNull Context ctx) {

        try (final Timer.Context timeContext = markAndTime(GET_ALL)) {
            DSLContext dsl = getDslContext(ctx);
            UnitsDao dao = new UnitsDao(dsl);
            String format = ctx.queryParamAsClass(FORMAT, String.class).getOrDefault("");
            String header = ctx.header(ACCEPT);

            ContentType contentType = Formats.parseQueryOrHeaderParam(header, format, Unit.class);
            String version = contentType.getParameters()
                                        .getOrDefault(VERSION, "");

            boolean isLegacyVersion = version.equals("1");

            String results;
            if (format.isEmpty() && !isLegacyVersion) {
                List<Unit> units = dao.getUnits();
                results = Formats.format(contentType, units, Unit.class);
                ctx.contentType(contentType.toString());
            } else {
                if (isLegacyVersion) {
                    format = Formats.getLegacyTypeFromContentType(contentType);
                }
                results = dao.getUnits(format);
                if (isLegacyVersion) {
                    ctx.contentType(contentType.toString());
                } else {
                    ctx.contentType(contentType.getType());
                }
            }

            ctx.status(HttpServletResponse.SC_OK);
            addDeprecatedContentTypeWarning(ctx, contentType);
            requestResultSize.update(results.length());

            byte[] bytes = results.getBytes();
            ctx.header(Header.CONTENT_LENGTH, String.valueOf(bytes.length));
            ctx.res.getOutputStream().write(bytes);
        } catch (IOException ex) {
            CdaError re = new CdaError("Failed to procees request to retrieve units");
            LOGGER.atSevere().withCause(ex).log("Failed to process request to retrieve units");
            ctx.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR).json(re);
        }
    }

    @OpenApi(ignore = true)
    @Override
    public void getOne(@NotNull Context ctx, @NotNull String unit) {

        try (Timer.Context timeContext = markAndTime(GET_ONE)) {
            ctx.status(HttpServletResponse.SC_NOT_IMPLEMENTED).json(CdaError.notImplemented());
        }
    }

    @OpenApi(ignore = true)
    @Override
    public void update(@NotNull Context ctx, @NotNull String unit) {
        ctx.status(HttpServletResponse.SC_NOT_IMPLEMENTED).json(CdaError.notImplemented());
    }

}
