package cwms.cda.api;

import static com.codahale.metrics.MetricRegistry.name;
import static cwms.cda.api.Controllers.ACCEPT;
import static cwms.cda.api.Controllers.FORMAT;
import static cwms.cda.api.Controllers.GET_ALL;
import static cwms.cda.api.Controllers.GET_ONE;
import static cwms.cda.api.Controllers.OFFICE;
import static cwms.cda.api.Controllers.RESULTS;
import static cwms.cda.api.Controllers.SIZE;
import static cwms.cda.api.Controllers.STATUS_200;
import static cwms.cda.api.Controllers.VERSION;
import static cwms.cda.api.Controllers.addDeprecatedContentTypeWarning;
import static cwms.cda.data.dao.JooqDao.getDslContext;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.cda.api.errors.CdaError;
import cwms.cda.data.dao.ParameterDao;
import cwms.cda.data.dto.Parameter;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import io.javalin.apibuilder.CrudHandler;
import io.javalin.http.Context;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.servlet.http.HttpServletResponse;
import org.jooq.DSLContext;

public class ParametersController implements CrudHandler {
    private static final Logger logger = Logger.getLogger(ParametersController.class.getName());
    private final MetricRegistry metrics;

    private final Histogram requestResultSize;

    public ParametersController(MetricRegistry metrics) {
        this.metrics = metrics;
        String className = this.getClass().getName();

        requestResultSize = this.metrics.histogram((name(className, RESULTS, SIZE)));
    }

    private Timer.Context markAndTime(String subject) {
        return Controllers.markAndTime(metrics, getClass().getName(), subject);
    }

    @OpenApi(ignore = true)
    @Override
    public void create(Context ctx) {
        ctx.status(HttpServletResponse.SC_NOT_FOUND);
    }

    @OpenApi(ignore = true)
    @Override
    public void delete(Context ctx, String id) {
        ctx.status(HttpServletResponse.SC_NOT_FOUND);

    }

    @OpenApi(
            queryParams = {
                    @OpenApiParam(name = FORMAT, deprecated = true, required = false, description = "Specifies the"
                            + " encoding format of the response. Valid value for the format field"
                            + " for this URI are:"
                            + "\n* `tab`"
                            + "\n* `csv`"
                            + "\n* `xml`"
                            + "\n* `json` (default)"),
                    @OpenApiParam(name = OFFICE, description = "Specifies the"
                            + " owning office of the parameters whose data is to be included in the "
                            + "response. If this field is not specified, the session user's default office will be"
                            + " used."),
            },
            responses = {
                @OpenApiResponse(status = STATUS_200, content = {
                    @OpenApiContent(isArray = true, from = Parameter.class, type = Formats.JSONV2),
                    @OpenApiContent(isArray = true, from = Parameter.class, type = Formats.JSON)
                }),
            },
            tags = {"Parameters"}
    )
    @Override
    public void getAll(Context ctx) {
        try (final Timer.Context timeContext = markAndTime(GET_ALL)) {
            DSLContext dsl = getDslContext(ctx);
            ParameterDao dao = new ParameterDao(dsl);
            String format = ctx.queryParamAsClass(FORMAT, String.class).getOrDefault("");
            String office = ctx.queryParamAsClass(OFFICE, String.class).getOrDefault(null);
            String header = ctx.header(ACCEPT);
            ContentType contentType = Formats.parseHeaderAndQueryParm(header, format, Parameter.class);
            String version = contentType.getParameters()
                                        .getOrDefault(VERSION, "");

            boolean isLegacyVersion = version.equals("1");

            String results;
            if (format.isEmpty() && !isLegacyVersion)
            {
                List<Parameter> params = dao.getParametersV2(office);
                results = Formats.format(contentType, params, Parameter.class);
                ctx.contentType(contentType.toString());
            }
            else
            {
                if (isLegacyVersion)
                {
                    format = Formats.getLegacyTypeFromContentType(contentType);
                }

                results = dao.getParameters(format);
                if (isLegacyVersion)
                {
                    ctx.contentType(contentType.toString());
                }
                else
                {
                    ctx.contentType(contentType.getType());
                }
            }

            ctx.status(HttpServletResponse.SC_OK);
            ctx.result(results);
            addDeprecatedContentTypeWarning(ctx, contentType);
            requestResultSize.update(results.length());
        } catch (Exception ex) {
            CdaError re = new CdaError("Failed to process request");
            logger.log(Level.SEVERE, re.toString(), ex);
            ctx.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR).json(re);
        }
    }

    @OpenApi(ignore = true)
    @Override
    public void getOne(Context ctx, String id) {
        try (final Timer.Context timeContext = markAndTime(GET_ONE)) {
            ctx.status(HttpServletResponse.SC_NOT_IMPLEMENTED).json(CdaError.notImplemented());
        }
    }

    @OpenApi(ignore = true)
    @Override
    public void update(Context ctx, String id) {
        ctx.status(HttpServletResponse.SC_NOT_IMPLEMENTED).json(CdaError.notImplemented());

    }

}
