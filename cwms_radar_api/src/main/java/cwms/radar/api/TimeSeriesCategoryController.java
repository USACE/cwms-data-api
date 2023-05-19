package cwms.radar.api;

import static com.codahale.metrics.MetricRegistry.name;

import static cwms.radar.api.Controllers.CATEGORY_ID;
import static cwms.radar.api.Controllers.GET_ALL;
import static cwms.radar.api.Controllers.GET_ONE;
import static cwms.radar.api.Controllers.NOT_SUPPORTED_YET;
import static cwms.radar.api.Controllers.OFFICE;
import static cwms.radar.api.Controllers.RESULTS;
import static cwms.radar.api.Controllers.SIZE;
import static cwms.radar.data.dao.JooqDao.getDslContext;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.radar.api.errors.RadarError;
import cwms.radar.data.dao.TimeSeriesCategoryDao;
import cwms.radar.data.dto.TimeSeriesCategory;
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

public class TimeSeriesCategoryController implements CrudHandler {
    public static final Logger logger =
            Logger.getLogger(TimeSeriesCategoryController.class.getName());
    public static final String TAG = "TimeSeries Categories-Beta";

    private final MetricRegistry metrics;

    private final Histogram requestResultSize;

    public TimeSeriesCategoryController(MetricRegistry metrics) {
        this.metrics = metrics;
        String className = this.getClass().getName();

        requestResultSize = this.metrics.histogram((name(className, RESULTS, SIZE)));
    }

    private Timer.Context markAndTime(String subject) {
        return Controllers.markAndTime(metrics, getClass().getName(), subject);
    }

    @OpenApi(queryParams = {
            @OpenApiParam(name = OFFICE, description = "Specifies the owning office of the "
                    + "timeseries category(ies) whose data is to be included in the response. If "
                    + "this field is not specified, matching timeseries category information from"
                    + " all offices shall be returned."),},
            responses = {@OpenApiResponse(status = "200",
                    content = {@OpenApiContent(isArray = true, from = TimeSeriesCategory.class,
                            type = Formats.JSON)
                    }),
                    @OpenApiResponse(status = "404", description = "Based on the combination of "
                            + "inputs provided the categories were not found."),
                    @OpenApiResponse(status = "501", description = "request format is not "
                            + "implemented")}, description = "Returns CWMS timeseries category "
            + "Data", tags = {TAG})
    @Override
    public void getAll(Context ctx) {
        try (final Timer.Context timeContext = markAndTime(GET_ALL);
             DSLContext dsl = getDslContext(ctx)) {
            TimeSeriesCategoryDao dao = new TimeSeriesCategoryDao(dsl);
            String office = ctx.queryParam(OFFICE);

            List<TimeSeriesCategory> cats = dao.getTimeSeriesCategories(office);

            String formatHeader = ctx.header(Header.ACCEPT);
            ContentType contentType = Formats.parseHeaderAndQueryParm(formatHeader, null);

            String result = Formats.format(contentType, cats, TimeSeriesCategory.class);

            ctx.result(result).contentType(contentType.toString());
            requestResultSize.update(result.length());

            ctx.status(HttpServletResponse.SC_OK);
        }

    }

    @OpenApi(
            pathParams = {
                    @OpenApiParam(name = CATEGORY_ID, required = true, description = "Specifies"
                            + " the Category whose data is to be included in the response."),
            },
            queryParams = {
                    @OpenApiParam(name = OFFICE, required = true, description = "Specifies the "
                            + "owning office of the timeseries category whose data is to be "
                            + "included in the response."),
            },
            responses = {
                    @OpenApiResponse(status = "200",
                            content = {
                                    @OpenApiContent(from = TimeSeriesCategory.class, type =
                                            Formats.JSON)
                            }
                    ),
                    @OpenApiResponse(status = "404", description = "Based on the combination of "
                            + "inputs provided the timeseries category was not found."),
                    @OpenApiResponse(status = "501", description = "request format is not "
                            + "implemented")},
            description = "Retrieves requested timeseries category", tags = {TAG})
    @Override
    public void getOne(Context ctx, String categoryId) {
        try (final Timer.Context timeContext = markAndTime(GET_ONE);
             DSLContext dsl = getDslContext(ctx)) {
            TimeSeriesCategoryDao dao = new TimeSeriesCategoryDao(dsl);
            String office = ctx.queryParam(OFFICE);

            String formatHeader = ctx.header(Header.ACCEPT);
            ContentType contentType = Formats.parseHeaderAndQueryParm(formatHeader, null);

            Optional<TimeSeriesCategory> grp = dao.getTimeSeriesCategory(office, categoryId);
            if (grp.isPresent()) {
                String result = Formats.format(contentType, grp.get());

                ctx.result(result).contentType(contentType.toString());
                requestResultSize.update(result.length());

                ctx.status(HttpServletResponse.SC_OK);
            } else {
                RadarError re = new RadarError("Unable to find category based on parameters given");
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
    public void update(Context ctx, String locationCode) {
        throw new UnsupportedOperationException(NOT_SUPPORTED_YET);
    }

    @OpenApi(ignore = true)
    @Override
    public void delete(Context ctx, String locationCode) {
        throw new UnsupportedOperationException(NOT_SUPPORTED_YET);
    }
}
