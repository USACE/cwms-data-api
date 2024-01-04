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

import static cwms.cda.api.Controllers.CREATE;
import static cwms.cda.api.Controllers.DELETE;
import static cwms.cda.api.Controllers.END;
import static cwms.cda.api.Controllers.FAIL_IF_EXISTS;
import static cwms.cda.api.Controllers.GET_ALL;
import static cwms.cda.api.Controllers.MAX_VERSION;
import static cwms.cda.api.Controllers.NOT_SUPPORTED_YET;
import static cwms.cda.api.Controllers.OFFICE;
import static cwms.cda.api.Controllers.START;
import static cwms.cda.api.Controllers.STATUS_200;
import static cwms.cda.api.Controllers.TIMEZONE;
import static cwms.cda.api.Controllers.VERSION_DATE;
import static cwms.cda.data.dao.JooqDao.getDslContext;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import cwms.cda.api.errors.CdaError;
import cwms.cda.data.dao.texttimeseries.TimeSeriesTextMode;
import cwms.cda.data.dao.texttimeseries.TimeSeriesTextDao;
import cwms.cda.data.dto.texttimeseries.TextTimeSeries;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.json.JsonV2;
import cwms.cda.helpers.DateUtils;
import io.javalin.apibuilder.CrudHandler;
import io.javalin.core.util.Header;
import io.javalin.http.Context;
import io.javalin.plugin.openapi.annotations.HttpMethod;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiRequestBody;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import java.time.ZonedDateTime;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.servlet.http.HttpServletResponse;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jooq.DSLContext;


public class TextTimeSeriesController implements CrudHandler {
    private static final Logger logger = Logger.getLogger(TextTimeSeriesController.class.getName());
    private static final String TAG = "Text-TimeSeries";
    public static final String TS_ID = "ts-id";
    public static final String TEXT_TIMESERIES_ID = "text-timeseries-id";
    public static final String REPLACE_ALL = "replace-all";
    private final MetricRegistry metrics;



    public TextTimeSeriesController(MetricRegistry metrics) {
        this.metrics = metrics;
    }

    @NotNull
    protected TimeSeriesTextDao getDao(DSLContext dsl) {
        return new TimeSeriesTextDao(dsl);
    }


    private Timer.Context markAndTime(String subject) {
        return Controllers.markAndTime(metrics, getClass().getName(), subject);
    }


    @OpenApi(
            queryParams = {
                    @OpenApiParam(name = OFFICE, required = true, description = "Specifies the owning office of "
                            + "the Text TimeSeries whose data is to be included in the response."),
                    @OpenApiParam(name = TS_ID, required = true, description = "Specifies the ts-id of the "
                            + "text timeseries"),
                    @OpenApiParam(name = TIMEZONE,  description = "Specifies "
                            + "the time zone of the values of the begin and end fields (unless "
                            + "otherwise specified). If this field is not specified, "
                            + "the default time zone of UTC shall be used."),
                    @OpenApiParam(name = START, required = true, description = "The start of the time window"),
                    @OpenApiParam(name = END, description = "The end of the time window. If specified the text associated with all times from start to end (inclusive) is deleted."),
                    @OpenApiParam(name = VERSION_DATE, description = "The version date for the time series.  If not specified, the minimum or maximum version date (depending on p_max_version) is used."),
                    @OpenApiParam(name = MAX_VERSION, type = Boolean.class, description = "Whether to use the maximum version date if p_version_date is not specified."),
                    @OpenApiParam(name = Controllers.MIN_ATTRIBUTE, type = Long.class, description = "The minimum attribute value to delete. If not specified, no minimum value is used."),
                    @OpenApiParam(name = Controllers.MAX_ATTRIBUTE, type = Long.class, description = "The maximum attribute value to delete. If not specified, no maximum value is used.")
            },
            responses = {
                    @OpenApiResponse(status = STATUS_200,
                            content = {
                                    @OpenApiContent(type = Formats.JSONV2, from = TextTimeSeries.class)
                            }
                    )},
            tags = {TAG}
    )
    @Override
    public void getAll(Context ctx) {
        String office = ctx.queryParam(OFFICE);
        String tsId = ctx.queryParam(TS_ID);

        ZonedDateTime beginZdt = queryParamAsZdt(ctx, START);
        ZonedDateTime endZdt = queryParamAsZdt(ctx, END);
        ZonedDateTime versionZdt = queryParamAsZdt(ctx, VERSION_DATE);
        Long minAttr = ctx.queryParamAsClass(Controllers.MIN_ATTRIBUTE, Long.class).getOrDefault(null);
        Long maxAttr = ctx.queryParamAsClass(Controllers.MAX_ATTRIBUTE, Long.class).getOrDefault(null);

        String formatHeader = ctx.header(Header.ACCEPT);
        ContentType contentType = Formats.parseHeaderAndQueryParm(formatHeader, "");
        try (Timer.Context timeContext = markAndTime(GET_ALL)) {
            DSLContext dsl = getDslContext(ctx);
            TimeSeriesTextDao dao = getDao(dsl);

            TextTimeSeries textTimeSeries = dao.retrieveFromView(office, tsId, beginZdt, endZdt, versionZdt, minAttr, maxAttr);

            ctx.contentType(contentType.toString());

            String result = Formats.format(contentType, textTimeSeries);
            ctx.result(result);

            ctx.status(HttpServletResponse.SC_OK);
        } catch (Exception ex) {
            CdaError re =
                    new CdaError("Failed to process request: " + ex.getLocalizedMessage());
            logger.log(Level.SEVERE, re.toString(), ex);
            ctx.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR).json(re);
        }

    }

    @Nullable
    private static ZonedDateTime queryParamAsZdt(Context ctx, String param, String timezone) {
        ZonedDateTime beginZdt = null;
        String begin = ctx.queryParam(param);
        if (begin != null) {
            beginZdt = DateUtils.parseUserDate(begin, timezone);
        }
        return beginZdt;
    }

    @Nullable
    private static ZonedDateTime queryParamAsZdt(Context ctx, String param) {
        return queryParamAsZdt(ctx, param, ctx.queryParamAsClass(TIMEZONE, String.class).getOrDefault("UTC"));
    }

    @OpenApi(ignore = true)
    @Override
    public void getOne(@NotNull Context ctx, @NotNull String templateId) {
        throw new UnsupportedOperationException(NOT_SUPPORTED_YET);
    }

    @OpenApi(
        description = "Create new TextTimeSeries",
        requestBody = @OpenApiRequestBody(
            content = {
                @OpenApiContent(from = TextTimeSeries.class, type = Formats.JSONV2)
            },
            required = true),
        queryParams = {
            @OpenApiParam(name = FAIL_IF_EXISTS, type = Boolean.class,
                description = "Create will fail if provided ID already exists. Default: true"),
            @OpenApiParam(name = MAX_VERSION, type = Boolean.class, description = "Whether to use the maximum version date if p_version_date is not specified."),
            @OpenApiParam(name = REPLACE_ALL, type = Boolean.class)
                        },
        method = HttpMethod.POST,
        tags = {TAG}
    )
    @Override
    public void create(@NotNull Context ctx) {
        try (Timer.Context ignored = markAndTime(CREATE)) {
            DSLContext dsl = getDslContext(ctx);

            String reqContentType = ctx.req.getContentType();
            String formatHeader = reqContentType != null ? reqContentType : Formats.JSONV2;
            String body = ctx.body();
            TextTimeSeries tts = deserialize(body, formatHeader);
            TimeSeriesTextDao dao = getDao(dsl);

            boolean maxVersion = ctx.queryParamAsClass(MAX_VERSION, Boolean.class).getOrDefault(false);
            boolean replaceAll = ctx.queryParamAsClass(REPLACE_ALL, Boolean.class).getOrDefault(false);
            dao.create(tts, maxVersion, replaceAll);
            ctx.status(HttpServletResponse.SC_CREATED);
        } catch (JsonProcessingException ex) {
            CdaError re = new CdaError("Failed to process create request");
            logger.log(Level.SEVERE, re.toString(), ex);
            ctx.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR).json(re);
        }
    }

    @OpenApi(
        description = "Updates a text timeseries",
        pathParams = {
            @OpenApiParam(name = TS_ID, description = "The text timeseries to be updated"),
        },
//        queryParams = {
//            @OpenApiParam(name = OFFICE, required = true, description = "Specifies the "
//                + "owning office of the text timeseries to be renamed"),
//            @OpenApiParam(name = TEXT_TIMESERIES_ID, description = "The new text timeseries id.")
//        },
        method = HttpMethod.PATCH,
        tags = {TAG}
    )
    @Override
    public void update(@NotNull Context ctx, @NotNull String oldTextTimeSeriesId) {
            throw new UnsupportedOperationException(NOT_SUPPORTED_YET);
//        try (Timer.Context ignored = markAndTime(UPDATE)) {
//            DSLContext dsl = getDslContext(ctx);
//
//            TimeSeriesTextDao dao = getDao(dsl);
////            String newTextTimeSeriesId = ctx.queryParam(TEXT_TIMESERIES_ID);
////            String office = ctx.queryParam(OFFICE);
//         //   dao.update(oldTextTimeSeriesId, newTextTimeSeriesId, office);
////            ctx.status(HttpServletResponse.SC_NO_CONTENT);
//        }
    }


    @OpenApi(
        description = "Deletes requested text timeseries id",
        pathParams = {
            @OpenApiParam(name = TS_ID, description = "The time series identifier to be deleted"),
        },
        queryParams = {
            @OpenApiParam(name = OFFICE, required = true, description = "Specifies the "
                + "owning office of the timeseries identifier to be deleted"),
            @OpenApiParam(name = Controllers.TEXT_MASK, required = true, description = "The standard text pattern to match. "
                    + "Use glob-style wildcard characters instead of sql-style wildcard characters for pattern matching."
                    + "  For StandardTextTimeSeries this should be the Standard_Text_Id (such as 'E' for ESTIMATED)"),
                @OpenApiParam(name = Controllers.DELETE_MODE, required = true, type = TimeSeriesTextMode.class,
                        description = "Type of delete to perform. Options are:\n"
                                + "DELETE_ALL\n"
                                + " - Delete Standard and Regular text timeseries values for the specified time series.\n"
                                + "DELETE_STANDARD\n"
                                + " - Delete Standard text time series for the specified text timeseries.\n"
                                + "DELETE_REGULAR\n"
                                + " - Delete Regular text timeseries values for the specified time series.\n"
                ),
            @OpenApiParam(name = TIMEZONE,  description = "Specifies "
                    + "the time zone of the values of the begin and end fields (unless "
                    + "otherwise specified). If this field is not specified, "
                    + "the default time zone of UTC shall be used."),
            @OpenApiParam(name = START, required = true, description = "The start of the time window"),
            @OpenApiParam(name = END, description = "The end of the time window. If specified the text associated with all times from start to end (inclusive) is deleted."),
            @OpenApiParam(name = VERSION_DATE, description = "The version date for the time series.  If not specified, the minimum or maximum version date (depending on p_max_version) is used."),
            @OpenApiParam(name = MAX_VERSION, type = Boolean.class, description = "Whether to use the maximum version date if p_version_date is not specified."),
            @OpenApiParam(name = Controllers.MIN_ATTRIBUTE, type = Long.class, description = "The minimum attribute value to delete. If not specified, no minimum value is used."),
            @OpenApiParam(name = Controllers.MAX_ATTRIBUTE, type = Long.class, description = "The maximum attribute value to delete. If not specified, no maximum value is used.")
        },
        method = HttpMethod.DELETE,
        tags = {TAG}
    )
    @Override
    public void delete(@NotNull Context ctx, @NotNull String textTimeSeriesId) {
        try (Timer.Context ignored = markAndTime(DELETE)) {
            DSLContext dsl = getDslContext(ctx);
            String office = ctx.queryParam(OFFICE);
            String mask = ctx.queryParam(Controllers.TEXT_MASK);
            TimeSeriesTextMode mode = ctx.queryParamAsClass(Controllers.DELETE_MODE, TimeSeriesTextMode.class).get();

            boolean maxVersion = ctx.queryParamAsClass(MAX_VERSION, Boolean.class).getOrDefault(true);

            ZonedDateTime beginZdt = queryParamAsZdt(ctx, START);
            ZonedDateTime endZdt = queryParamAsZdt(ctx, END);
            ZonedDateTime versionZdt = queryParamAsZdt(ctx, VERSION_DATE);

            Long minAttr = ctx.queryParamAsClass(Controllers.MIN_ATTRIBUTE, Long.class).getOrDefault(null);
            Long maxAttr = ctx.queryParamAsClass(Controllers.MAX_ATTRIBUTE, Long.class).getOrDefault(null);

            TimeSeriesTextDao dao = getDao(dsl);

            dao.delete(mode, office, textTimeSeriesId, mask, beginZdt, endZdt, versionZdt, maxVersion, minAttr, maxAttr);

            ctx.status(HttpServletResponse.SC_NO_CONTENT);
        }
    }

    private static TextTimeSeries deserialize(String body, String format) throws JsonProcessingException {
        TextTimeSeries retval;
        if (ContentType.equivalent(Formats.JSONV2, format)) {
            ObjectMapper om = JsonV2.buildObjectMapper();
            retval = om.readValue(body, TextTimeSeries.class);
        } else {
            throw new IllegalArgumentException("Unsupported format: " + format);
        }
        return retval;
    }

}
