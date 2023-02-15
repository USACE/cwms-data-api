package cwms.radar.api;

import static com.codahale.metrics.MetricRegistry.name;
import static cwms.radar.data.dao.JooqDao.getDslContext;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.radar.api.errors.RadarError;
import cwms.radar.data.dao.TimeSeriesIdentifierDao;
import cwms.radar.data.dto.TimeSeriesIdentifier;
import cwms.radar.formatters.ContentType;
import cwms.radar.formatters.Formats;
import io.javalin.apibuilder.CrudHandler;
import io.javalin.core.util.Header;
import io.javalin.http.Context;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import java.util.List;
import java.util.Optional;
import java.util.logging.Logger;
import javax.servlet.http.HttpServletResponse;
import org.jooq.DSLContext;

public class TimeSeriesIdentifierController implements CrudHandler {
    public static final Logger logger =
            Logger.getLogger(TimeSeriesIdentifierController.class.getName());
    public static final String TAG = "TimeSeries Identifier-Beta";

    private final MetricRegistry metrics;


    private final Histogram requestResultSize;

    public TimeSeriesIdentifierController(MetricRegistry metrics) {
        this.metrics = metrics;
        String className = this.getClass().getName();

        requestResultSize = this.metrics.histogram((name(className, "results", "size")));
    }

    private Timer.Context markAndTime(String subject) {
        return Controllers.markAndTime(metrics, getClass().getName(), subject);
    }

    @OpenApi(queryParams = {
            @OpenApiParam(name = "office", description = "Specifies the owning office of the "
                    + "timeseries identifier(s) whose data is to be included in the response. If "
                    + "this field is not specified, matching timeseries identifier information from"
                    + " all offices shall be returned."),},
            responses = {@OpenApiResponse(status = "200",
                    content = {@OpenApiContent(isArray = true, from = TimeSeriesIdentifier.class,
                            type = Formats.JSON)
                    }),
                    @OpenApiResponse(status = "404", description = "Based on the combination of "
                            + "inputs provided the categories were not found."),
                    @OpenApiResponse(status = "501", description = "request format is not "
                            + "implemented")}, description = "Returns CWMS timeseries identifier "
            + "Data", tags = {TAG})
    @Override
    public void getAll(Context ctx) {

        try (final Timer.Context ignored = markAndTime("getAll");
             DSLContext dsl = getDslContext(ctx)) {
            TimeSeriesIdentifierDao dao = new TimeSeriesIdentifierDao(dsl);
            String office = ctx.queryParam("office");

            List<cwms.radar.data.dto.TimeSeriesIdentifier> ids =
                    dao.getTimeSeriesIdentifiers(office);

            String formatHeader = ctx.header(Header.ACCEPT);
            ContentType contentType = Formats.parseHeaderAndQueryParm(formatHeader, null);

            String result = null;  /// Formats.format(contentType, ids, TimeSeriesIdentifier.class);

            ctx.result(result).contentType(contentType.toString());
            requestResultSize.update(result.length());

            ctx.status(HttpServletResponse.SC_OK);
        }

    }

    @OpenApi(
            pathParams = {
                    @OpenApiParam(name = "timeseries-id", required = true, description = "Specifies"
                            + " the identifier of the timeseries to be included in the response."),
            },
            queryParams = {
                    @OpenApiParam(name = "office", required = true, description = "Specifies the "
                            + "owning office of the timeseries identifier to be "
                            + "included in the response."),
            },
            responses = {
                    @OpenApiResponse(status = "200",
                            content = {
                                    @OpenApiContent(from = TimeSeriesIdentifier.class, type =
                                            Formats.JSON)
                            }
                    ),
                    @OpenApiResponse(status = "404", description = "Based on the combination of "
                            + "inputs provided the timeseries identifier was not found."),
                    @OpenApiResponse(status = "501", description = "request format is not "
                            + "implemented")},
            description = "Retrieves requested timeseries identifier", tags = {TAG})
    @Override
    public void getOne(Context ctx, String timeseriesId) {

        try (final Timer.Context ignored = markAndTime("getOne");
             DSLContext dsl = getDslContext(ctx)) {
            TimeSeriesIdentifierDao dao = new TimeSeriesIdentifierDao(dsl);
            String office = ctx.queryParam("office");

            String formatHeader = ctx.header(Header.ACCEPT);
            ContentType contentType = Formats.parseHeaderAndQueryParm(formatHeader, null);

            Optional<TimeSeriesIdentifier> grp = dao.getTimeSeriesIdentifier(office, timeseriesId);
            if (grp.isPresent()) {
                String result = null;  /// Formats.format(contentType, grp.get());

                ctx.result(result).contentType(contentType.toString());
                requestResultSize.update(result.length());

                ctx.status(HttpServletResponse.SC_OK);
            } else {
                RadarError re = new RadarError("Unable to find identifier based on parameters "
                        + "given");
                logger.info(() -> re + System.lineSeparator() + "for request " + ctx.fullUrl());
                ctx.status(HttpServletResponse.SC_NOT_FOUND).json(re);
            }

        }

    }

    @OpenApi(ignore = true)
    @Override
    public void create(Context ctx) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @OpenApi(ignore = true)
    @Override
    public void update(Context ctx, String locationCode) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @OpenApi(ignore = true)
    @Override
    public void delete(Context ctx, String locationCode) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
