package cwms.radar.api;

import static com.codahale.metrics.MetricRegistry.name;

import static cwms.radar.api.Controllers.CATEGORY_ID;
import static cwms.radar.api.Controllers.GET_ALL;
import static cwms.radar.api.Controllers.GET_ONE;
import static cwms.radar.api.Controllers.GROUP_ID;
import static cwms.radar.api.Controllers.NOT_SUPPORTED_YET;
import static cwms.radar.api.Controllers.OFFICE;
import static cwms.radar.api.Controllers.RESULTS;
import static cwms.radar.api.Controllers.SIZE;
import static cwms.radar.data.dao.JooqDao.getDslContext;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.radar.api.errors.RadarError;
import cwms.radar.data.dao.TimeSeriesGroupDao;
import cwms.radar.data.dto.TimeSeriesGroup;
import cwms.radar.formatters.ContentType;
import cwms.radar.formatters.Formats;
import io.javalin.apibuilder.CrudHandler;
import io.javalin.core.util.Header;
import io.javalin.http.Context;
import io.javalin.http.HttpCode;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import java.util.List;
import java.util.logging.Logger;
import javax.servlet.http.HttpServletResponse;
import org.jooq.DSLContext;

public class TimeSeriesGroupController implements CrudHandler {
    public static final Logger logger = Logger.getLogger(TimeSeriesGroupController.class.getName());
    public static final String TAG = "Timeseries Groups-Beta";

    private final MetricRegistry metrics;

    private final Histogram requestResultSize;

    public TimeSeriesGroupController(MetricRegistry metrics) {
        this.metrics = metrics;
        String className = this.getClass().getName();

        requestResultSize = this.metrics.histogram((name(className, RESULTS, SIZE)));
    }

    private Timer.Context markAndTime(String subject) {
        return Controllers.markAndTime(metrics, getClass().getName(), subject);
    }

    @OpenApi(queryParams = {
            @OpenApiParam(name = OFFICE, description = "Specifies the owning office of the "
                    + "timeseries group(s) whose data is to be included in the response. If this "
                    + "field is not specified, matching timeseries groups information from all "
                    + "offices shall be returned.")},
            responses = {
                    @OpenApiResponse(status = "200",
                            content = {@OpenApiContent(isArray = true, from =
                                    TimeSeriesGroup.class, type = Formats.JSON)
                            }
                    ),
                    @OpenApiResponse(status = "404", description = "Based on the combination of "
                            + "inputs provided the timeseries group(s) were not found."),
                    @OpenApiResponse(status = "501", description = "request format is not "
                            + "implemented")}, description = "Returns CWMS Timeseries Groups "
            + "Data", tags = {TAG})
    @Override
    public void getAll(Context ctx) {
        try (final Timer.Context timeContext = markAndTime(GET_ALL);
             DSLContext dsl = getDslContext(ctx)) {
            TimeSeriesGroupDao dao = new TimeSeriesGroupDao(dsl);
            String office = ctx.queryParam(OFFICE);

            List<TimeSeriesGroup> grps = dao.getTimeSeriesGroups(office);
            if (grps.isEmpty()) {
                RadarError re = new RadarError("No data found for The provided office");
                logger.info(() -> re + " for request " + ctx.fullUrl());
                ctx.status(HttpCode.NOT_FOUND).json(re);
            } else {
                String formatHeader = ctx.header(Header.ACCEPT);
                ContentType contentType = Formats.parseHeaderAndQueryParm(formatHeader, null);

                String result = Formats.format(contentType, grps, TimeSeriesGroup.class);

                ctx.result(result).contentType(contentType.toString());
                requestResultSize.update(result.length());

                ctx.status(HttpServletResponse.SC_OK);
            }
        }

    }

    @OpenApi(
            pathParams = {
                    @OpenApiParam(name = GROUP_ID, required = true, description = "Specifies "
                            + "the timeseries group whose data is to be included in the response")
            },
            queryParams = {
                    @OpenApiParam(name = OFFICE, required = true, description = "Specifies the "
                            + "owning office of the timeseries group whose data is to be included"
                            + " in the response."),
                    @OpenApiParam(name = CATEGORY_ID, required = true, description = "Specifies"
                            + " the category containing the timeseries group whose data is to be "
                            + "included in the response."),
            },
            responses = {
                    @OpenApiResponse(status = "200", content = {
                            @OpenApiContent(from = TimeSeriesGroup.class, type = Formats.JSON),
                    }

                    )},
            description = "Retrieves requested timeseries group", tags = {"Timeseries Groups"})
    @Override
    public void getOne(Context ctx, String groupId) {
        try (final Timer.Context timeContext = markAndTime(GET_ONE);
             DSLContext dsl = getDslContext(ctx)) {
            TimeSeriesGroupDao dao = new TimeSeriesGroupDao(dsl);
            String office = ctx.queryParam(OFFICE);
            String categoryId = ctx.queryParam(CATEGORY_ID);

            String formatHeader = ctx.header(Header.ACCEPT);
            ContentType contentType = Formats.parseHeaderAndQueryParm(formatHeader, null);

            TimeSeriesGroup group = null;
            List<TimeSeriesGroup> timeSeriesGroups = dao.getTimeSeriesGroups(office, categoryId,
                    groupId);
            if (timeSeriesGroups != null && !timeSeriesGroups.isEmpty()) {
                if (timeSeriesGroups.size() == 1) {
                    group = timeSeriesGroups.get(0);
                } else {
                    // An error. [office, categoryId, groupId] should have, at most, one match
                    String message = String.format(
                            "Multiple TimeSeriesGroups returned from getTimeSeriesGroups "
                                    + "for:%s category:%s groupId:%s At most one match was "
                                    + "expected. Found:%s",
                            office, categoryId, groupId, timeSeriesGroups);
                    throw new IllegalArgumentException(message);
                }
            }
            if (group != null) {
                String result = Formats.format(contentType, group);


                ctx.result(result);
                ctx.contentType(contentType.toString());
                requestResultSize.update(result.length());

                ctx.status(HttpServletResponse.SC_OK);
            } else {
                RadarError re = new RadarError("Unable to find group based on parameters given");
                logger.info(() -> re + System.lineSeparator() + "for request " + ctx.fullUrl());
                ctx.status(HttpServletResponse.SC_NOT_FOUND).json(re);
            }

        }

    }

    @OpenApi(ignore = true)
    @Override
    public void create(Context ctx) {
        throw new UnsupportedOperationException(NOT_SUPPORTED_YET);
    }

    @OpenApi(ignore = true)
    @Override
    public void update(Context ctx, String groupId) {
        throw new UnsupportedOperationException(NOT_SUPPORTED_YET);
    }

    @OpenApi(ignore = true)
    @Override
    public void delete(Context ctx, String groupId) {
        throw new UnsupportedOperationException(NOT_SUPPORTED_YET);
    }
}
