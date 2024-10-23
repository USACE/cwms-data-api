/*
 * MIT License
 *
 * Copyright (c) 2023 Hydrologic Engineering Center
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package cwms.cda.api;

import static com.codahale.metrics.MetricRegistry.name;
import static cwms.cda.api.Controllers.AT;
import static cwms.cda.api.Controllers.BEGIN;
import static cwms.cda.api.Controllers.CREATE;
import static cwms.cda.api.Controllers.DATE_FORMAT;
import static cwms.cda.api.Controllers.DATUM;
import static cwms.cda.api.Controllers.DELETE;
import static cwms.cda.api.Controllers.EFFECTIVE_DATE;
import static cwms.cda.api.Controllers.END;
import static cwms.cda.api.Controllers.EXAMPLE_DATE;
import static cwms.cda.api.Controllers.FORMAT;
import static cwms.cda.api.Controllers.GET_ALL;
import static cwms.cda.api.Controllers.GET_ONE;
import static cwms.cda.api.Controllers.METHOD;
import static cwms.cda.api.Controllers.NAME;
import static cwms.cda.api.Controllers.OFFICE;
import static cwms.cda.api.Controllers.RATING_ID;
import static cwms.cda.api.Controllers.RESULTS;
import static cwms.cda.api.Controllers.SIZE;
import static cwms.cda.api.Controllers.STATUS_200;
import static cwms.cda.api.Controllers.STATUS_404;
import static cwms.cda.api.Controllers.STATUS_501;
import static cwms.cda.api.Controllers.STORE_TEMPLATE;
import static cwms.cda.api.Controllers.TIMEZONE;
import static cwms.cda.api.Controllers.UNIT;
import static cwms.cda.api.Controllers.UPDATE;
import static cwms.cda.api.Controllers.VERSION_DATE;
import static cwms.cda.api.Controllers.addDeprecatedContentTypeWarning;
import static cwms.cda.data.dao.JooqDao.getDslContext;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.cda.api.errors.CdaError;
import cwms.cda.data.dao.JsonRatingUtils;
import cwms.cda.data.dao.RatingDao;
import cwms.cda.data.dao.RatingSetDao;
import cwms.cda.data.dto.CwmsDTOBase;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.annotations.FormattableWith;
import cwms.cda.formatters.json.JsonV2;
import cwms.cda.formatters.xml.XMLv2;
import cwms.cda.helpers.DateUtils;
import hec.data.RatingException;
import hec.data.cwmsRating.RatingSet;
import io.javalin.apibuilder.CrudHandler;
import io.javalin.core.util.Header;
import io.javalin.core.validation.JavalinValidation;
import io.javalin.http.Context;
import io.javalin.http.HttpCode;
import io.javalin.plugin.openapi.annotations.HttpMethod;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiRequestBody;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import java.io.IOException;
import java.time.Instant;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.servlet.http.HttpServletResponse;
import mil.army.usace.hec.cwms.rating.io.xml.RatingXmlFactory;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jooq.DSLContext;



public class RatingController implements CrudHandler {
    private static final Logger logger = Logger.getLogger(RatingController.class.getName());
    private static final String TAG = "Ratings";

    private final MetricRegistry metrics;

    private final Histogram requestResultSize;

    static {
        JavalinValidation.register(RatingSet.DatabaseLoadMethod.class,
                RatingController::getDatabaseLoadMethod);
    }

    public RatingController(MetricRegistry metrics) {
        this.metrics = metrics;
        String className = this.getClass().getName();
        requestResultSize = this.metrics.histogram((name(className, RESULTS, SIZE)));
    }

    private static RatingSet.DatabaseLoadMethod getDatabaseLoadMethod(String input) {
        RatingSet.DatabaseLoadMethod retval = null;

        if (input != null) {
            retval = RatingSet.DatabaseLoadMethod.valueOf(input.toUpperCase());
        }
        return retval;
    }

    @NotNull
    protected RatingDao getRatingDao(DSLContext dsl) {
        return new RatingSetDao(dsl);
    }

    @Override
    @OpenApi(description = "Create new RatingSet",
            requestBody = @OpenApiRequestBody(content = {
                @OpenApiContent(type = Formats.XMLV2),
                @OpenApiContent(type = Formats.JSONV2)},
            required = true),
            queryParams = {
                @OpenApiParam(name = STORE_TEMPLATE, type = Boolean.class,
                        description = "Also store updates to the rating template. Default: true")
            },
            method = HttpMethod.POST, path = "/ratings", tags = {TAG})
    public void create(@NotNull Context ctx) {

        try (final Timer.Context ignored = markAndTime(CREATE)) {
            DSLContext dsl = getDslContext(ctx);
            RatingDao ratingDao = getRatingDao(dsl);
            String ratingSet = deserializeRatingSet(ctx);
            boolean storeTemplate = ctx.queryParamAsClass(STORE_TEMPLATE, Boolean.class).getOrDefault(true);
            ratingDao.create(ratingSet, storeTemplate);
            ctx.status(HttpServletResponse.SC_OK).json("Created RatingSet");
        } catch (IOException | RatingException ex) {
            CdaError re = new CdaError("Failed to process create request");
            logger.log(Level.SEVERE, re.toString(), ex);
            ctx.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR).json(re);
        }
    }

    private Timer.Context markAndTime(String subject) {
        return Controllers.markAndTime(metrics, getClass().getName(), subject);
    }

    private String deserializeRatingSet(Context ctx) throws IOException, RatingException {
        String formatHeader = ctx.req.getContentType();
        //Using placeholder CwmsDTOBase.class since we do not have a RatingSet DTO
        //The contentType will match against the standard listing of Formats constants
        ContentType contentType = Formats.parseHeader(formatHeader, CwmsDTOBase.class);
        String body = ctx.body();
        return deserializeRatingSet(body, contentType.getType());
    }

    //Package private for unit testing
    String deserializeRatingSet(String body, String contentType) throws IOException, RatingException {
        String retval;
        if (Formats.XML.equals(contentType)) {
            retval = body;
        } else if (Formats.JSON.equals(contentType)) {
            retval = RatingXmlFactory.toXml(JsonRatingUtils.fromJson(body), "");
        } else {
            throw new IOException("Unexpected format:" + contentType);
        }

        return retval;
    }

    @OpenApi(
        pathParams = {
            @OpenApiParam(name = RATING_ID, required = true, description = "The rating-id of the "
                    + "effective dates to be deleted. "),
        },
        queryParams = {
            @OpenApiParam(name = OFFICE, required = true, description = "Specifies the office of "
                    + "the ratings to be deleted."),
            @OpenApiParam(name = BEGIN, required = true, description = "The start of the time "
                    + "window to delete. The format for this field is ISO 8601 extended, "
                    + "with optional offset and timezone, i.e., '"
                + DATE_FORMAT + "', e.g., '" + EXAMPLE_DATE + "'."),
            @OpenApiParam(name = END, required = true, description = "The end of the time window"
                    + " to delete. The format for this field is ISO 8601 extended, with optional "
                    + "offset and timezone, i.e., '"
                + DATE_FORMAT + "', e.g., '" + EXAMPLE_DATE + "'."),
            @OpenApiParam(name = TIMEZONE, description = "This field specifies a default "
                    + "timezone to be used if the format of the "
                + BEGIN + ", " + END + ", or " + VERSION_DATE + " parameters do not include "
                    + "offset or time zone information. Defaults to UTC."),
        },
        method = HttpMethod.DELETE,
        tags = {TAG}
    )
    @Override
    public void delete(@NotNull Context ctx, @NotNull String ratingSpecId) {
        try (Timer.Context ignored = markAndTime(DELETE)) {
            DSLContext dsl = getDslContext(ctx);

            String timezone = ctx.queryParamAsClass(TIMEZONE, String.class).getOrDefault("UTC");
            Instant startTimeDate = DateUtils.parseUserDate(ctx.queryParam(BEGIN), timezone).toInstant();
            Instant endTimeDate = DateUtils.parseUserDate(ctx.queryParam(END), timezone).toInstant();
            String office = ctx.queryParam(OFFICE);
            RatingDao ratingDao = getRatingDao(dsl);
            ratingDao.delete(office, ratingSpecId, startTimeDate, endTimeDate);
            ctx.status(HttpServletResponse.SC_NO_CONTENT);
        }
    }

    @OpenApi(queryParams = {
        @OpenApiParam(name = NAME, description = "Specifies the "
                + "name(s) of the rating whose data is to be included in the response."
                + " A case insensitive comparison is used to match names."),
        @OpenApiParam(name = OFFICE,  description = "Specifies the "
                + "owning office of the Rating(s) whose data is to be included in "
                + "the response. If this field is not specified, matching rating "
                + "information from all offices shall be returned."),
        @OpenApiParam(name = UNIT,  description = "Specifies the "
                + "unit or unit system of the response. Valid values for the unit "
                + "field are:"
                + "\n* `EN`  Specifies English unit system.  Rating "
                + "values will be in the default English units for their "
                + "parameters."
                + "\n* `SI`   Specifies the SI unit system.  Rating values "
                + "will be in the default SI units for their parameters."
                + "\n* `Other`  Any unit returned in the response to the units URI request "
                + "that is appropriate for the requested parameters."),
        @OpenApiParam(name = DATUM,  description = "Specifies the "
                + "elevation datum of the response. This field affects only elevation"
                + " Ratings. Valid values for this field are:"
                + "\n* `NAVD88`  The elevation values will in the "
                + "specified or default units above the NAVD-88 datum."
                + "\n* `NGVD29`  The elevation values will be in the "
                + "specified or default units above the NGVD-29 datum."),
        @OpenApiParam(name = AT,  description = "Specifies the "
                + "start of the time window for data to be included in the response. "
                + "If this field is not specified, any required time window begins 24"
                + " hours prior to the specified or default end time."),
        @OpenApiParam(name = END,  description = "Specifies the "
                + "end of the time window for data to be included in the response. If"
                + " this field is not specified, any required time window ends at the"
                + " current time"),
        @OpenApiParam(name = TIMEZONE,  description = "Specifies "
                + "the time zone of the values of the begin and end fields (unless "
                + "otherwise specified), as well as the time zone of any times in the"
                + " response. If this field is not specified, the default time zone "
                + "of UTC shall be used."),
        @OpenApiParam(name = FORMAT,  description = "Specifies the"
                + " encoding format of the response. Valid values for the format "
                + "field for this URI are:"
                + "\n* `tab`"
                + "\n* `csv`"
                + "\n* `xml`"
                + "\n* `json` (default)")},
            responses = {
                @OpenApiResponse(status = STATUS_200, content = {
                    @OpenApiContent(type = Formats.JSON),
                    @OpenApiContent(type = Formats.XML),
                    @OpenApiContent(type = Formats.TAB),
                    @OpenApiContent(type = Formats.CSV)
                }),
                @OpenApiResponse(status = STATUS_404, description = "The provided combination of "
                            + "parameters did not find a rating table."),
                @OpenApiResponse(status = STATUS_501, description = "Requested format is not "
                            + "implemented")},
            tags = {TAG})
    @Override
    public void getAll(@NotNull Context ctx) {

        try (final Timer.Context ignored = markAndTime(GET_ALL)) {
            DSLContext dsl = getDslContext(ctx);

            RatingDao ratingDao = getRatingDao(dsl);

            String names = ctx.queryParamAsClass(NAME, String.class).getOrDefault("*");
            String unit = ctx.queryParam(UNIT);
            String datum = ctx.queryParam(DATUM);
            String office = ctx.queryParam(OFFICE);
            String start = ctx.queryParam(AT);
            String end = ctx.queryParam(END);
            String timezone = ctx.queryParamAsClass(TIMEZONE, String.class).getOrDefault("UTC");

            String format = ctx.queryParamAsClass(FORMAT, String.class).getOrDefault("");
            String header = ctx.header(Header.ACCEPT);

            ContentType contentType = Formats.parseHeaderAndQueryParm(header, format, RatingAliasMarker.class);

            if (format.isEmpty()) {
                //Use the full content type here (i.e. application/json;version=2)
                ctx.contentType(contentType.toString());
            } else {
                //Legacy content type only includes the basic type (i.e. application/json)
                ctx.contentType(contentType.getType());
            }

            //At the moment, we still use the legacy formatting here, since we don't have a newer API for
            // serializing/deserializing a collection of rating sets - unlike getOne.
            String legacyFormat = Formats.getLegacyTypeFromContentType(contentType);
            String results = ratingDao.retrieveRatings(legacyFormat, names, unit, datum, office, start,
                    end, timezone);

            ctx.status(HttpServletResponse.SC_OK);
            ctx.result(results);
            addDeprecatedContentTypeWarning(ctx, contentType);
            requestResultSize.update(results.length());
        }
    }

    @OpenApi(
            pathParams = {
                @OpenApiParam(name = RATING_ID, required = true, description = "The rating-id of the effective "
                        + "dates to be retrieve. "),
            },
            queryParams = {
                @OpenApiParam(name = OFFICE, required = true, description =
                        "Specifies the owning office of the ratingset to be included in the "
                                + "response."),
                @OpenApiParam(name = BEGIN, description = "Specifies the "
                        + "start of the time window for data to be included in the response. "
                        + "If this field is not specified no start time will be used."),
                @OpenApiParam(name = END, description = "Specifies the "
                        + "end of the time window for data to be included in the response. If"
                        + " this field is not specified no end time will be used."),
                @OpenApiParam(name = TIMEZONE, description = "Specifies "
                        + "the time zone of the values of the begin and end fields (unless "
                        + "otherwise specified), as well as the time zone of any times in the"
                        + " response. If this field is not specified, the default time zone "
                        + "of UTC shall be used."),
                @OpenApiParam(name = EFFECTIVE_DATE, description = "Specifies the "
                        + "date to find the closest match to for retrieving a specific rating curve. This date is used "
                        + "instead of the time window specified by start and end. "
                        + "The format for this field is ISO 8601 extended."),
                @OpenApiParam(name = METHOD, description = "Specifies "
                        + "the retrieval method used.  If no method is provided EAGER will be used.",
                        type = RatingSet.DatabaseLoadMethod.class),
            },
            responses = {
                @OpenApiResponse(status = STATUS_200, content = {
                    @OpenApiContent(type = Formats.JSONV2),
                    @OpenApiContent(type = Formats.XMLV2)})},
            description = "Returns CWMS Rating Data",
            tags = {TAG})

    @Override
    public void getOne(@NotNull Context ctx, @NotNull String rating) {

        try (final Timer.Context ignored = markAndTime(GET_ONE)) {

            String timezone = ctx.queryParamAsClass(TIMEZONE, String.class).getOrDefault("UTC");
            Instant effectiveDate = null;
            String effectiveDateParam = ctx.queryParam(EFFECTIVE_DATE);
            if (effectiveDateParam != null) {
                effectiveDate = DateUtils.parseUserDate(effectiveDateParam, timezone).toInstant();
            }

            Instant beginInstant = null;
            String begin = ctx.queryParam(BEGIN);
            if (begin != null) {
                beginInstant = DateUtils.parseUserDate(begin, timezone).toInstant();
            }

            Instant endInstant = null;
            String end = ctx.queryParam(END);
            if (end != null) {
                endInstant = DateUtils.parseUserDate(end, timezone).toInstant();
            }

            String officeId = ctx.queryParam(OFFICE);

            RatingSet.DatabaseLoadMethod method = ctx.queryParamAsClass(METHOD,
                    RatingSet.DatabaseLoadMethod.class)
                    .getOrDefault(RatingSet.DatabaseLoadMethod.EAGER);

            String body = getRatingSetString(ctx, method, officeId, rating, beginInstant, endInstant, effectiveDate);
            if (body != null) {
                ctx.result(body);
                ctx.status(HttpCode.OK);
            }
        }
    }


    @Nullable
    private String getRatingSetString(Context ctx, RatingSet.DatabaseLoadMethod method,
                                      String officeId, String rating, Instant begin,
                                      Instant end, Instant effectiveDate) {
        String retval = null;

        try (final Timer.Context ignored = markAndTime("getRatingSetString")) {
            String acceptHeader = ctx.header(Header.ACCEPT);
            ContentType contentType = Formats.parseHeader(acceptHeader, RatingAliasMarker.class);

            boolean isJson = Formats.JSONV2.equals(contentType.toString());
            boolean isXml = Formats.XMLV2.equals(contentType.toString());

            if (isJson || isXml) {
                ctx.contentType(contentType.toString());
                try {
                    RatingSet ratingSet = getRatingSet(ctx, method, officeId, rating, begin, end, effectiveDate);
                    if (ratingSet != null) {
                        if (isJson) {
                            retval = JsonRatingUtils.toJson(ratingSet);
                        } else {
                            retval = RatingXmlFactory.toXml(ratingSet, " ");
                        }
                    } else {
                        ctx.status(HttpCode.NOT_FOUND);
                    }
                } catch (RatingException e) {
                    CdaError re =
                            new CdaError("Failed to process request to retrieve RatingSet");
                    logger.log(Level.SEVERE, re.toString(), e);
                    ctx.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                    ctx.json(re);
                } catch (IOException e) {
                    CdaError re =
                            new CdaError("Failed to process request to retrieve RatingSet");
                    logger.log(Level.SEVERE, re.toString(), e);
                    ctx.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR).json(re);
                }

            } else {
                CdaError re = new CdaError("Currently supporting only: " + Formats.JSONV2
                        + " and " + Formats.XMLV2);
                logger.log(Level.WARNING, String.format("Provided accept header not recognized: %s", acceptHeader), re);
                ctx.status(HttpServletResponse.SC_NOT_IMPLEMENTED);
                ctx.json(CdaError.notImplemented());
            }

            addDeprecatedContentTypeWarning(ctx, contentType);
        }
        return retval;
    }

    private RatingSet getRatingSet(Context ctx, RatingSet.DatabaseLoadMethod method,
                                   String officeId, String rating, Instant begin,
                                   Instant end, Instant effectiveDate) throws IOException, RatingException {
        RatingSet ratingSet;
        try (final Timer.Context ignored = markAndTime("getRatingSet")) {
            DSLContext dsl = getDslContext(ctx);

            RatingDao ratingDao = getRatingDao(dsl);
            ratingSet = ratingDao.retrieve(method, officeId, rating, begin, end, effectiveDate);
        }

        return ratingSet;
    }

    @Override
    @OpenApi(description = "Update a RatingSet",
            requestBody = @OpenApiRequestBody(content = {
                @OpenApiContent(type = Formats.XMLV2),
                @OpenApiContent(type = Formats.JSONV2)
            }, required = true),
            queryParams = {
                @OpenApiParam(name = STORE_TEMPLATE, type = Boolean.class,
                        description = "Also store updates to the rating template. Default: true")
            },
            method = HttpMethod.PUT, path = "/ratings", tags = {TAG})
    public void update(@NotNull Context ctx, @NotNull String ratingId) {

        try (final Timer.Context ignored = markAndTime(UPDATE)) {
            DSLContext dsl = getDslContext(ctx);

            RatingDao ratingDao = getRatingDao(dsl);

            boolean storeTemplate = ctx.queryParamAsClass(STORE_TEMPLATE, Boolean.class)
                    .getOrDefault(true);
            String ratingSet = deserializeRatingSet(ctx);
            ratingDao.store(ratingSet, storeTemplate);
            ctx.status(HttpServletResponse.SC_OK).json("Updated RatingSet");
        } catch (IOException | RatingException ex) {
            CdaError re = new CdaError("Failed to process request to update RatingSet");
            logger.log(Level.SEVERE, re.toString(), ex);
            ctx.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR).json(re);
        }
    }

    /**
     * This is a marker class only used by <code>Formats</code> to support <code>ContentType</code> aliases.
     */
    @FormattableWith(contentType = Formats.JSONV2, formatter = JsonV2.class, aliases = {Formats.JSON})
    @FormattableWith(contentType = Formats.XMLV2, formatter = XMLv2.class, aliases = {Formats.XML, Formats.DEFAULT})
    private static final class RatingAliasMarker extends CwmsDTOBase { }
}
