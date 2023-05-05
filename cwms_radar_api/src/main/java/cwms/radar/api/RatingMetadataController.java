package cwms.radar.api;

import static com.codahale.metrics.MetricRegistry.name;

import static cwms.radar.api.Controllers.END;
import static cwms.radar.api.Controllers.GET_ALL;
import static cwms.radar.api.Controllers.NOT_SUPPORTED_YET;
import static cwms.radar.api.Controllers.OFFICE;
import static cwms.radar.api.Controllers.PAGE;
import static cwms.radar.api.Controllers.PAGE_SIZE;
import static cwms.radar.api.Controllers.RATING_ID_MASK;
import static cwms.radar.api.Controllers.RESULTS;
import static cwms.radar.api.Controllers.SIZE;
import static cwms.radar.api.Controllers.START;
import static cwms.radar.api.Controllers.TIMEZONE;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.radar.api.errors.RadarError;
import cwms.radar.data.dao.JooqDao;
import cwms.radar.data.dao.RatingMetadataDao;
import cwms.radar.data.dto.rating.RatingMetadataList;
import cwms.radar.formatters.ContentType;
import cwms.radar.formatters.Formats;
import cwms.radar.helpers.DateUtils;
import io.javalin.apibuilder.CrudHandler;
import io.javalin.core.util.Header;
import io.javalin.http.Context;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import java.time.ZonedDateTime;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.servlet.http.HttpServletResponse;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;

public class RatingMetadataController implements CrudHandler {
    private static final Logger logger = Logger.getLogger(RatingMetadataController.class.getName());

    private final MetricRegistry metrics;

    private static final int DEFAULT_PAGE_SIZE = 50;

    private final Histogram requestResultSize;

    public RatingMetadataController(MetricRegistry metrics) {
        this.metrics = metrics;
        String className = this.getClass().getName();
        requestResultSize = this.metrics.histogram((name(className, RESULTS, SIZE)));
    }

    protected DSLContext getDslContext(Context ctx) {
        return JooqDao.getDslContext(ctx);
    }

    private Timer.Context markAndTime(String subject) {
        return Controllers.markAndTime(metrics, getClass().getName(), subject);
    }


    @OpenApi(
            queryParams = {
                    @OpenApiParam(name = OFFICE, description = "Specifies the owning office of "
                            + "the Rating Specs whose data is to be included in the response. If "
                            + "this field is not specified, matching rating information from all "
                            + "offices shall be returned."),
                    @OpenApiParam(name = RATING_ID_MASK, description = "RegExp that specifies "
                            + "the rating IDs to be included in the response. If this field is "
                            + "not specified, all Rating Specs shall be returned."),
                    @OpenApiParam(name = START,  description = "Specifies the "
                            + "start of the time window of the effective dates to be included. "
                            + "If this field is not specified no start time will be used."),
                    @OpenApiParam(name = END,  description = "Specifies the "
                            + "end of the time window for effective dates to be included. "
                            + "If this field is not specified no end time will be used."),
                    @OpenApiParam(name = TIMEZONE,  description = "Specifies "
                            + "the time zone of the values of the begin and end fields (unless "
                            + "otherwise specified). If this field is not specified, "
                            + "the default time zone of UTC shall be used."),
                    @OpenApiParam(name = PAGE,
                            description = "This end point can return a lot of data, this "
                                    + "identifies where in the request you are. This is an opaque"
                                    + " value, and can be obtained from the 'next-page' value in "
                                    + "the response."
                    ),
                    @OpenApiParam(name = PAGE_SIZE, type = Integer.class,
                            description = "How many entries per page returned. "
                                    + "Default " + DEFAULT_PAGE_SIZE + "."
                    ),
            },
            responses = {
                    @OpenApiResponse(status = "200",
                            content = {
                                    @OpenApiContent(type = Formats.JSONV2, from =
                                            RatingMetadataList.class)
                            }
                    )},
            tags = {"Ratings"}
    )
    @Override
    public void getAll(Context ctx) {
        String cursor = ctx.queryParamAsClass(PAGE, String.class).getOrDefault("");
        int pageSize =
                ctx.queryParamAsClass(PAGE_SIZE, Integer.class).getOrDefault(DEFAULT_PAGE_SIZE);

        String office = ctx.queryParam(OFFICE);
        String ratingIdMask = ctx.queryParam(RATING_ID_MASK);

        String timezone = ctx.queryParamAsClass(TIMEZONE, String.class).getOrDefault("UTC");

        ZonedDateTime beginZdt = null;
        String begin = ctx.queryParam(START);
        if (begin != null) {
            beginZdt = DateUtils.parseUserDate(begin, timezone);
        }

        ZonedDateTime endZdt = null;
        String end = ctx.queryParam(END);
        if (end != null) {
            endZdt = DateUtils.parseUserDate(end, timezone);
        }

        String formatHeader = ctx.header(Header.ACCEPT);
        ContentType contentType = Formats.parseHeaderAndQueryParm(formatHeader, "");

        try (final Timer.Context timeContext = markAndTime(GET_ALL);
             DSLContext dsl = getDslContext(ctx)) {
            RatingMetadataDao dao = getDao(dsl);

            RatingMetadataList metadataList = dao.retrieve(cursor, pageSize, office,
                    ratingIdMask, beginZdt, endZdt);

            String result = Formats.format(contentType, metadataList);
            ctx.result(result);

            ctx.contentType(contentType.toString());

            requestResultSize.update(result.length());
            ctx.status(HttpServletResponse.SC_OK);
        } catch (Exception ex) {
            RadarError re =
                    new RadarError("Failed to process request: " + ex.getLocalizedMessage());
            logger.log(Level.SEVERE, re.toString(), ex);
            ctx.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR).json(re);
        }
    }


    @OpenApi(ignore = true)
    @Override
    public void getOne(Context ctx, String ratingId) {
        throw new UnsupportedOperationException(NOT_SUPPORTED_YET); //To change body of
        // generated methods, choose Tools | Specs.
    }

    @NotNull
    protected RatingMetadataDao getDao(DSLContext dsl) {
        return new RatingMetadataDao(dsl, metrics);
    }


    @OpenApi(ignore = true)
    @Override
    public void create(Context ctx) {
        throw new UnsupportedOperationException(NOT_SUPPORTED_YET); //To change body of
        // generated methods, choose Tools | Specs.
    }

    @OpenApi(ignore = true)
    @Override
    public void update(Context ctx, String locationCode) {
        throw new UnsupportedOperationException(NOT_SUPPORTED_YET); //To change body of
        // generated methods, choose Tools | Specs.
    }

    @OpenApi(ignore = true)
    @Override
    public void delete(Context ctx, String locationCode) {
        throw new UnsupportedOperationException(NOT_SUPPORTED_YET); //To change body of
        // generated methods, choose Tools | Specs.
    }

}
