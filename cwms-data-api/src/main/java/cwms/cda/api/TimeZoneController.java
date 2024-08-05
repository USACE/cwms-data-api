package cwms.cda.api;

import static com.codahale.metrics.MetricRegistry.name;
import static cwms.cda.api.Controllers.ACCEPT;
import static cwms.cda.api.Controllers.FORMAT;
import static cwms.cda.api.Controllers.GET_ALL;
import static cwms.cda.api.Controllers.GET_ONE;
import static cwms.cda.api.Controllers.NOT_SUPPORTED_YET;
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
import cwms.cda.data.dao.TimeZoneDao;
import cwms.cda.data.dto.TimeZoneId;
import cwms.cda.data.dto.TimeZoneIds;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import io.javalin.apibuilder.CrudHandler;
import io.javalin.http.Context;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;

import java.util.logging.Level;
import java.util.logging.Logger;
import javax.servlet.http.HttpServletResponse;
import org.jooq.DSLContext;

public class TimeZoneController implements CrudHandler {
    private static final Logger logger = Logger.getLogger(TimeZoneController.class.getName());

    private final MetricRegistry metrics;

    private final Histogram requestResultSize;

    public TimeZoneController(MetricRegistry metrics) {
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
        throw new UnsupportedOperationException(NOT_SUPPORTED_YET);
    }

    @OpenApi(ignore = true)
    @Override
    public void delete(Context ctx, String id) {
        throw new UnsupportedOperationException(NOT_SUPPORTED_YET);
    }

    @OpenApi(
            queryParams = {
                    @OpenApiParam(name = FORMAT, required = false, description = "Specifies the"
                            + " encoding format of the response. Valid value for the format field"
                            + " for this URI are:"
                            + "\n* `tab`  "
                            + "\n* `csv`  "
                            + "\n* `xml`  "
                            + "\n* `json`  (default)")
            },
            responses = {
                    @OpenApiResponse(status = STATUS_200, content = {
                        @OpenApiContent(from = TimeZoneIds.class, type = Formats.JSONV2),
                        @OpenApiContent(from = TimeZoneIds.class, type = Formats.JSON)
                    }),
                    @OpenApiResponse(status = STATUS_501, description = "The format requested is not "
                            + "implemented")
            },
            tags = {"TimeZones"}
    )
    @Override
    public void getAll(Context ctx) {
        try (Timer.Context timeContext = markAndTime(GET_ALL)) {
            DSLContext dsl = getDslContext(ctx);
            TimeZoneDao dao = new TimeZoneDao(dsl);
            String format = ctx.queryParamAsClass(FORMAT, String.class).getOrDefault("");
            String header = ctx.header(ACCEPT);

            ContentType contentType = Formats.parseHeaderAndQueryParm(header, format, TimeZoneId.class);
            String version = contentType.getParameters()
                                        .getOrDefault(VERSION, "");

            boolean isLegacyVersion = version.equals("1");

            String results;
            if (format.isEmpty() && !isLegacyVersion)
            {
                TimeZoneIds zones = dao.getTimeZones();
                results = Formats.format(contentType, zones);
                ctx.contentType(contentType.toString());
            }
            else
            {
                if (isLegacyVersion)
                {
                    format = Formats.getLegacyTypeFromContentType(contentType);
                }
                results = dao.getTimeZones(format);
                if (isLegacyVersion)
                {
                    ctx.contentType(contentType.toString());
                }
                else
                {
                    ctx.contentType(contentType.getType());
                }
            }

            addDeprecatedContentTypeWarning(ctx, contentType);

            requestResultSize.update(results.length());
            ctx.status(HttpServletResponse.SC_OK);
            ctx.result(results);
        } catch (Exception ex) {
            logger.log(Level.SEVERE, null, ex);
            ctx.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            ctx.result("Failed to process request");
        }
    }

    @OpenApi(ignore = true)
    @Override
    public void getOne(Context ctx, String id) {
        try (Timer.Context timeContext = markAndTime(GET_ONE)) {
            throw new UnsupportedOperationException(NOT_SUPPORTED_YET);
        }
    }

    @OpenApi(ignore = true)
    @Override
    public void update(Context ctx, String id) {
        throw new UnsupportedOperationException(NOT_SUPPORTED_YET);
    }

}
