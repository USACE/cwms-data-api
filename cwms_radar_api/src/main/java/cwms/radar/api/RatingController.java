package cwms.radar.api;

import static com.codahale.metrics.MetricRegistry.name;
import static cwms.radar.data.dao.JooqDao.getDslContext;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.radar.api.errors.RadarError;
import cwms.radar.data.dao.JsonRatingUtils;
import cwms.radar.data.dao.RatingDao;
import cwms.radar.data.dao.RatingSetDao;
import cwms.radar.formatters.ContentType;
import cwms.radar.formatters.Formats;
import cwms.radar.formatters.FormattingException;
import hec.data.RatingException;
import hec.data.cwmsRating.RatingSet;
import io.javalin.apibuilder.CrudHandler;
import io.javalin.core.util.Header;
import io.javalin.http.Context;
import io.javalin.http.HttpCode;
import io.javalin.plugin.openapi.annotations.HttpMethod;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiRequestBody;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.servlet.http.HttpServletResponse;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jooq.DSLContext;


public class RatingController implements CrudHandler {
    private static final Logger logger = Logger.getLogger(RatingController.class.getName());
    private final MetricRegistry metrics;

    private final Histogram requestResultSize;

    public RatingController(MetricRegistry metrics) {
        this.metrics = metrics;
        String className = this.getClass().getName();
        requestResultSize = this.metrics.histogram((name(className, "results", "size")));
    }

    @NotNull
    protected RatingDao getRatingDao(DSLContext dsl) {
        return new RatingSetDao(dsl);
    }

    @OpenApi(description = "Create new RatingSet", requestBody = @OpenApiRequestBody(content =
            {@OpenApiContent(type = Formats.XMLV2), @OpenApiContent(type = Formats.JSONV2)},
            required = true), method = HttpMethod.POST, path = "/ratings", tags = {"Ratings"})
    public void create(Context ctx) {

        try (final Timer.Context timeContext = markAndTime("create"); DSLContext dsl =
                getDslContext(ctx)) {
            RatingDao ratingDao = getRatingDao(dsl);
            RatingSet ratingSet = deserializeRatingSet(ctx);
            ratingDao.create(ratingSet);
            ctx.status(HttpServletResponse.SC_ACCEPTED).json("Created RatingSet");
        } catch (IOException | RatingException ex) {
            RadarError re = new RadarError("Failed to process create request");
            logger.log(Level.SEVERE, re.toString(), ex);
            ctx.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR).json(re);
        }
    }

    private Timer.Context markAndTime(String subject) {
        return Controllers.markAndTime(metrics, getClass().getName(), subject);
    }

    @NotNull
    private ContentType getContentType(Context ctx) {
        String requestContent = ctx.req.getContentType();
        String formatHeader = requestContent != null ? requestContent : Formats.JSON;
        ContentType contentType = Formats.parseHeader(formatHeader);
        if (contentType == null) {
            throw new FormattingException("Format header could not be parsed:" + formatHeader);
        }
        return contentType;
    }

    private RatingSet deserializeRatingSet(Context ctx) throws IOException, RatingException {
        return deserializeRatingSet(ctx.body(), getContentType(ctx));
    }


    public RatingSet deserializeRatingSet(String body, ContentType contentType)
            throws IOException, RatingException {
        return deserializeRatingSet(body, contentType.getType());
    }

    public RatingSet deserializeRatingSet(String body, String contentType) throws IOException,
            RatingException {
        RatingSet retval;

        if ((Formats.XML).equals(contentType)) {
            retval = deserializeFromXml(body);
        } else if ((Formats.JSON).equals(contentType)) {
            retval = JsonRatingUtils.fromJson(body);
        } else {
            throw new IOException("Unexpected format:" + contentType);
        }

        return retval;
    }

    @NotNull
    public RatingSet deserializeFromXml(String body) throws RatingException {
        return RatingSet.fromXml(body);
    }

    @OpenApi(queryParams = {@OpenApiParam(name = "office", required = true, description =
            "Specifies the " + "owning office of the rating to be deleted.")}, method =
            HttpMethod.DELETE, tags = {"Ratings"})
    @Override
    public void delete(Context ctx, String ratingSpecId) {
        String office = ctx.queryParam("office");

        try (final Timer.Context timeContext = markAndTime("delete");
             DSLContext dsl = getDslContext(ctx)) {
            RatingDao ratingDao = getRatingDao(dsl);
            ratingDao.delete(office, ratingSpecId);

            ctx.status(HttpServletResponse.SC_ACCEPTED);
            ctx.json("Deleted RatingSet");
        } catch (IOException | RatingException ex) {
            RadarError re = new RadarError("Failed to process delete request");
            logger.log(Level.SEVERE, re.toString(), ex);
            ctx.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR).json(re);
        }
    }

    @OpenApi(queryParams = {
            @OpenApiParam(name = "name", required = false, description = "Specifies the "
                    + "name(s) of the rating whose data is to be included in the response."
                    + " A case insensitive comparison is used to match names."),
            @OpenApiParam(name = "office", required = false, description = "Specifies the "
                    + "owning office of the Rating(s) whose data is to be included in "
                    + "the response. If this field is not specified, matching rating "
                    + "information from all offices shall be returned."),
            @OpenApiParam(name = "unit", required = false, description = "Specifies the "
                    + "unit or unit system of the response. Valid values for the unit "
                    + "field are:\r\n"
                    + "1. EN.   Specifies English unit system.  Rating "
                    + "values will be in the default English units for their "
                    + "parameters.\r\n"
                    + "2. SI.   Specifies the SI unit system.  Rating values "
                    + "will be in the default SI units for their parameters.\r\n"
                    + "3. Other. Any unit returned in the response to the units URI request "
                    + "that is appropriate for the requested parameters."),
            @OpenApiParam(name = "datum", required = false, description = "Specifies the "
                    + "elevation datum of the response. This field affects only elevation"
                    + " Ratings. Valid values for this field are:\r\n1. NAVD88.  The "
                    + "elevation values will in the specified or default units above the "
                    + "NAVD-88 datum.\r\n2. NGVD29.  The elevation values will be in the "
                    + "specified or default units above the NGVD-29 datum."),
            @OpenApiParam(name = "at", required = false, description = "Specifies the "
                    + "start of the time window for data to be included in the response. "
                    + "If this field is not specified, any required time window begins 24"
                    + " hours prior to the specified or default end time."),
            @OpenApiParam(name = "end", required = false, description = "Specifies the "
                    + "end of the time window for data to be included in the response. If"
                    + " this field is not specified, any required time window ends at the"
                    + " current time"),
            @OpenApiParam(name = "timezone", required = false, description = "Specifies "
                    + "the time zone of the values of the begin and end fields (unless "
                    + "otherwise specified), as well as the time zone of any times in the"
                    + " response. If this field is not specified, the default time zone "
                    + "of UTC shall be used."),
            @OpenApiParam(name = "format", required = false, description = "Specifies the"
                    + " encoding format of the response. Valid values for the format "
                    + "field for this URI are:\r\n1.    tab\r\n2.    csv\r\n3.    "
                    + "xml\r\n4.    json (default)")},
            responses = {
                @OpenApiResponse(status = "200"),
                @OpenApiResponse(status = "404", description = "The provided combination of "
                            + "parameters did not find a rating table."),
                @OpenApiResponse(status = "501", description = "Requested format is not "
                            + "implemented")},
            tags = {"Ratings"})
    @Override
    public void getAll(Context ctx) {

        try (final Timer.Context timeContext = markAndTime("getAll");
             DSLContext dsl = getDslContext(ctx)) {
            RatingDao ratingDao = getRatingDao(dsl);

            String format = ctx.queryParamAsClass("format", String.class).getOrDefault("json");
            String names = ctx.queryParam("names");
            String unit = ctx.queryParam("unit");
            String datum = ctx.queryParam("datum");
            String office = ctx.queryParam("office");
            String start = ctx.queryParam("at");
            String end = ctx.queryParam("end");
            String timezone = ctx.queryParamAsClass("timezone", String.class).getOrDefault("UTC");

            switch (format) {
                case "json": {
                    ctx.contentType(Formats.JSON);
                    break;
                }
                case "tab": {
                    ctx.contentType(Formats.TAB);
                    break;
                }
                case "csv": {
                    ctx.contentType(Formats.CSV);
                    break;
                }
                case "xml": {
                    ctx.contentType(Formats.XML);
                    break;
                }
                case "wml2": {
                    ctx.contentType(Formats.WML2);
                    break;
                }
                case "jpg": // same handler
                case "png":
                default: {
                    ctx.status(HttpServletResponse.SC_NOT_IMPLEMENTED)
                            .json(RadarError.notImplemented());
                    return;
                }
            }

            String results = ratingDao.retrieveRatings(format, names, unit, datum, office, start,
                    end, timezone);
            ctx.status(HttpServletResponse.SC_OK);
            ctx.result(results);
            requestResultSize.update(results.length());
        }
    }

    @OpenApi(queryParams = {
            @OpenApiParam(name = "office", required = true, description =
                    "Specifies the " + "owning office of the ratingset to be included in the "
                            + "response."),
            },
            responses = {
                    @OpenApiResponse(status = "200", content = {
                            @OpenApiContent(type = Formats.JSONV2),
                            @OpenApiContent(type = Formats.XMLV2)})},
            description = "Returns CWMS Rating Data",
            tags = {"Ratings"})

    @Override
    public void getOne(Context ctx, String rating) {

        try (final Timer.Context timeContext = markAndTime("getOne")) {
            String officeId = ctx.queryParam("office");

            // If we wanted to do async I think it would be like this
            //   ctx.future(getRatingSetAsync(ctx, officeId, rating));

            String body = getRatingSetString(ctx, officeId, rating);
            if (body != null) {
                ctx.result(body);
                ctx.status(HttpCode.OK);
            }
        }
    }

    public CompletableFuture<String> getRatingSetAsync(Context ctx, String officeId,
                                                       String rating) {
        return CompletableFuture.supplyAsync(() -> getRatingSetString(ctx, officeId, rating));
    }

    @Nullable
    private String getRatingSetString(Context ctx, String officeId, String rating) {
        String retval = null;

        try (final Timer.Context timeContext = markAndTime("getRatingSetString")) {
            String acceptHeader = ctx.header(Header.ACCEPT);

            if (Formats.JSONV2.equals(acceptHeader) || Formats.XMLV2.equals(acceptHeader)) {
                try {
                    RatingSet ratingSet = getRatingSet(ctx, officeId, rating);
                    if (ratingSet != null) {
                        if (Formats.JSONV2.equals(acceptHeader)) {
                            retval = JsonRatingUtils.toJson(ratingSet);
                        } else if (Formats.XMLV2.equals(acceptHeader)) {
                            retval = ratingSet.toXmlString(" ");
                        }
                    } else {
                        ctx.status(HttpCode.NOT_FOUND);
                    }
                } catch (RatingException e) {
                    RadarError re =
                            new RadarError("Failed to process request to retrieve RatingSet");
                    logger.log(Level.SEVERE, re.toString(), e);
                    ctx.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                    ctx.json(re);
                } catch (IOException e) {
                    RadarError re =
                            new RadarError("Failed to process request to retrieve RatingSet");
                    logger.log(Level.SEVERE, re.toString(), e);
                    ctx.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR).json(re);
                }

            } else {
                RadarError re = new RadarError("Currently supporting only: " + Formats.JSONV2
                        + " and " + Formats.XMLV2);
                logger.log(Level.WARNING, "Provided accept header not recognized:"
                                + acceptHeader, re);
                ctx.status(HttpServletResponse.SC_NOT_IMPLEMENTED);
                ctx.json(RadarError.notImplemented());
            }
        }
        return retval;
    }

    private RatingSet getRatingSet(Context ctx, String officeId, String rating)
            throws IOException, RatingException {
        RatingSet ratingSet;
        try (final Timer.Context timeContext = markAndTime("getRatingSet");
             DSLContext dsl = getDslContext(ctx)) {
            RatingDao ratingDao = getRatingDao(dsl);
            ratingSet = ratingDao.retrieve(officeId, rating);
        }

        return ratingSet;
    }

    @OpenApi(description = "Update a RatingSet", requestBody = @OpenApiRequestBody(content =
            {@OpenApiContent(type = Formats.XMLV2), @OpenApiContent(type = Formats.JSONV2)

            }, required = true), method = HttpMethod.PUT, path = "/ratings", tags = {"Ratings"})
    public void update(Context ctx, String ratingId) {

        try (final Timer.Context timeContext = markAndTime("update");
             DSLContext dsl = getDslContext(ctx)) {
            RatingDao ratingDao = getRatingDao(dsl);

            // Retrieve the rating specified by ratingId and then somehow apply the update?
            // RatingSet ratingSet = ratingDao.retrieve(officeId, ratingId);
            // Or just store what they sent us?
            RatingSet ratingSet = deserializeRatingSet(ctx);
            ratingDao.store(ratingSet);
            ctx.status(HttpServletResponse.SC_ACCEPTED).json("Updated RatingSet");
        } catch (IOException | RatingException ex) {
            RadarError re = new RadarError("Failed to process request to update RatingSet");
            logger.log(Level.SEVERE, re.toString(), ex);
            ctx.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR).json(re);
        }
    }

}
