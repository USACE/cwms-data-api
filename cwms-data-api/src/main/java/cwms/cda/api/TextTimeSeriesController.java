/*
 * MIT License
 *
 * Copyright (c) 2024 Hydrologic Engineering Center
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

import static cwms.cda.api.Controllers.BEGIN;
import static cwms.cda.api.Controllers.CREATE;
import static cwms.cda.api.Controllers.DELETE;
import static cwms.cda.api.Controllers.END;
import static cwms.cda.api.Controllers.GET_ALL;
import static cwms.cda.api.Controllers.NAME;
import static cwms.cda.api.Controllers.NOT_SUPPORTED_YET;
import static cwms.cda.api.Controllers.OFFICE;
import static cwms.cda.api.Controllers.STATUS_200;
import static cwms.cda.api.Controllers.TIMESERIES;
import static cwms.cda.api.Controllers.TIMEZONE;
import static cwms.cda.api.Controllers.UPDATE;
import static cwms.cda.api.Controllers.VERSION_DATE;
import static cwms.cda.api.Controllers.queryParamAsZdt;
import static cwms.cda.api.Controllers.requiredParam;
import static cwms.cda.api.Controllers.requiredZdt;
import static cwms.cda.data.dao.JooqDao.getDslContext;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import cwms.cda.api.errors.CdaError;
import cwms.cda.data.dao.texttimeseries.TimeSeriesTextDao;
import cwms.cda.data.dto.texttimeseries.TextTimeSeries;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.json.JsonV2;
import cwms.cda.helpers.DateUtils;
import cwms.cda.helpers.ReplaceUtils;
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
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.servlet.http.HttpServletResponse;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;


public class TextTimeSeriesController implements CrudHandler {
    private static final Logger logger = Logger.getLogger(TextTimeSeriesController.class.getName());
    static final String TAG = "Text-TimeSeries";

    public static final String REPLACE_ALL = "replace-all";


    private final MetricRegistry metrics;

    public static final boolean DEFAULT_CREATE_REPLACE_ALL = false;
    public static final boolean DEFAULT_UPDATE_REPLACE_ALL = true;
    private final String contextPath;

    public static final String CONTEXT_TOKEN = "{context-path}";
    public static final String OFFICE_TOKEN = "{office}";
    public static final String CLOB_TOKEN = "{clob-id}";
    public final static String clobTemplate = String.format("%s/clob/ignored?clob-id=%s&office-id=%s", CONTEXT_TOKEN, CLOB_TOKEN, OFFICE_TOKEN);

    public TextTimeSeriesController(MetricRegistry metrics, String contextPath) {
        this.metrics = metrics;
        this.contextPath = contextPath;  // like /spk-data
    }

    @NotNull
    protected TimeSeriesTextDao getDao(DSLContext dsl) {
        return new TimeSeriesTextDao(dsl, clobTemplate );
    }


    private Timer.Context markAndTime(String subject) {
        return Controllers.markAndTime(metrics, getClass().getName(), subject);
    }


    @OpenApi(
            queryParams = {
                    @OpenApiParam(name = OFFICE, required = true, description = "Specifies the owning office of "
                            + "the Text TimeSeries whose data is to be included in the response."),
                    @OpenApiParam(name = NAME, required = true, description = "Specifies the ts-id of the "
                            + "text timeseries"),
                    @OpenApiParam(name = TIMEZONE,  description = "Specifies "
                            + "the time zone of the values of the begin and end fields (unless "
                            + "otherwise specified). If this field is not specified, "
                            + "the default time zone of UTC shall be used."),
                    @OpenApiParam(name = BEGIN, required = true, description = "The start of the time window"),
                    @OpenApiParam(name = END, required = true, description = "The end of the time window."),
                    @OpenApiParam(name = VERSION_DATE, description = "The version date for the time series.  If not specified, the maximum version date is used."),
                    @OpenApiParam(name = Controllers.MIN_ATTRIBUTE, type = Long.class, description = "The minimum attribute value to retrieve. If not specified, no minimum value is used."),
                    @OpenApiParam(name = Controllers.MAX_ATTRIBUTE, type = Long.class, description = "The maximum attribute value to retrieve. If not specified, no maximum value is used."),

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
    public void getAll(@NotNull Context ctx) {

        String office = requiredParam(ctx, OFFICE);
        String tsId = requiredParam(ctx, NAME);
        ZonedDateTime beginZdt = requiredZdt(ctx, BEGIN);
        ZonedDateTime endZdt = requiredZdt(ctx, END);
        ZonedDateTime versionZdt = queryParamAsZdt(ctx, VERSION_DATE);
        boolean maxVersion = versionZdt == null;

        Long minAttr = ctx.queryParamAsClass(Controllers.MIN_ATTRIBUTE, Long.class).getOrDefault(null);
        Long maxAttr = ctx.queryParamAsClass(Controllers.MAX_ATTRIBUTE, Long.class).getOrDefault(null);


        String formatHeader = ctx.header(Header.ACCEPT);
        ContentType contentType = Formats.parseHeaderAndQueryParm(formatHeader, "");
        try (Timer.Context ignored = markAndTime(GET_ALL)) {
            DSLContext dsl = getDslContext(ctx);
            TimeSeriesTextDao dao = getDao(dsl);

            String textMask = "*";

//            String contextPath = this.contextPath;
//            String protocol = ctx.protocol();// HTTP/1.1
//            String host = ctx.host();// localhost:7000
//            String requestURI = ctx.req.getRequestURI();// /spk-data/timeseries/text
//            String pathInfo = ctx.req.getPathInfo();// "/text"
//            String servletPath = ctx.req.getServletPath();// "/timeseries"

            String requestTemplate = ReplaceUtils.replace(clobTemplate, CONTEXT_TOKEN, contextPath, false);
            requestTemplate = ReplaceUtils.replace(requestTemplate, OFFICE_TOKEN, office);
            Function<String, String> idToUrl = ReplaceUtils.replace(requestTemplate, CLOB_TOKEN);
            TextTimeSeries textTimeSeries = dao.retrieveFromDao( office, tsId, textMask,
                    beginZdt, endZdt, versionZdt,
                    maxVersion, minAttr, maxAttr, idToUrl);

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

            @OpenApiParam(name = REPLACE_ALL, type = Boolean.class, description = "Whether to replace any and all existing text with the specified text. Default is " + DEFAULT_CREATE_REPLACE_ALL)
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

            boolean maxVersion = true;

            boolean replaceAll = ctx.queryParamAsClass(REPLACE_ALL, Boolean.class).getOrDefault(DEFAULT_CREATE_REPLACE_ALL);
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
            @OpenApiParam(name = TIMESERIES, description = "The id of the text timeseries to be updated"),
        },
            queryParams = {
                    @OpenApiParam(name = REPLACE_ALL, type = Boolean.class, description = "Whether to replace any and all existing text with the specified text. Default is:" + DEFAULT_UPDATE_REPLACE_ALL)
            },
        requestBody = @OpenApiRequestBody(
                content = {
                        @OpenApiContent(from = TextTimeSeries.class, type = Formats.JSONV2),
                },
                required = true
        ),
        method = HttpMethod.PATCH,
        tags = {TAG}
    )
    @Override
    public void update(@NotNull Context ctx, @NotNull String oldTextTimeSeriesId) {

        try (Timer.Context ignored = markAndTime(UPDATE)) {
            boolean maxVersion = true;
            boolean replaceAll = ctx.queryParamAsClass(REPLACE_ALL, Boolean.class).getOrDefault(DEFAULT_UPDATE_REPLACE_ALL);
            String reqContentType = ctx.req.getContentType();
            String formatHeader = reqContentType != null ? reqContentType : Formats.JSONV2;
            String body = ctx.body();

            TextTimeSeries tts = deserialize(body, formatHeader);
            DSLContext dsl = getDslContext(ctx);

            TimeSeriesTextDao dao = getDao(dsl);
            dao.store(tts,maxVersion, replaceAll);

        } catch (JsonProcessingException e) {
            CdaError re = new CdaError("Failed to process create request");
            logger.log(Level.SEVERE, re.toString(), e);
            ctx.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR).json(re);
        }
    }


    @OpenApi(
        description = "Deletes requested text timeseries id",
        pathParams = {
            @OpenApiParam(name = TIMESERIES, description = "The time series identifier to be deleted"),
        },
        queryParams = {
                @OpenApiParam(name = OFFICE, required = true, description = "Specifies the "
                        + "owning office of the timeseries identifier to be deleted"),
                @OpenApiParam(name = Controllers.TEXT_MASK, required = true, description = "The "
                        + "standard text pattern to match. "
                        + "Use glob-style wildcard characters instead of sql-style wildcard "
                        + "characters for pattern matching."
                        + "  For StandardTextTimeSeries this should be the Standard_Text_Id (such"
                        + " as 'E' for ESTIMATED)"),

                @OpenApiParam(name = TIMEZONE, description = "Specifies "
                        + "the time zone of the values of the begin and end fields (unless "
                        + "otherwise specified). If this field is not specified, "
                        + "the default time zone of UTC shall be used."),
                @OpenApiParam(name = BEGIN, required = true, description = "The start of the time"
                        + " window"),
                @OpenApiParam(name = END, required = true, description = "The end of the time window." ),
                @OpenApiParam(name = VERSION_DATE, description = "The version date for the time "
                        + "series.  If not specified, maximum version date is used."),
                @OpenApiParam(name = Controllers.MIN_ATTRIBUTE, type = Long.class, description =
                        "The minimum attribute value to delete. If not specified, no minimum "
                                + "value is used."),
                @OpenApiParam(name = Controllers.MAX_ATTRIBUTE, type = Long.class, description =
                        "The maximum attribute value to delete. If not specified, no maximum "
                                + "value is used.")
        },
        method = HttpMethod.DELETE,
        tags = {TAG}
    )
    @Override
    public void delete(@NotNull Context ctx, @NotNull String textTimeSeriesId) {
        try (Timer.Context ignored = markAndTime(DELETE)) {
            DSLContext dsl = getDslContext(ctx);
            String office = requiredParam(ctx, OFFICE);
            String mask = requiredParam(ctx, Controllers.TEXT_MASK);


            ZonedDateTime beginZdt = requiredZdt(ctx, BEGIN);
            ZonedDateTime endZdt = requiredZdt(ctx, END);
            ZonedDateTime versionZdt = queryParamAsZdt(ctx, VERSION_DATE);

            boolean maxVersion = versionZdt == null;

            Long minAttr = ctx.queryParamAsClass(Controllers.MIN_ATTRIBUTE, Long.class).getOrDefault(null);
            Long maxAttr = ctx.queryParamAsClass(Controllers.MAX_ATTRIBUTE, Long.class).getOrDefault(null);

            TimeSeriesTextDao dao = getDao(dsl);

            dao.delete( office, textTimeSeriesId, mask, beginZdt, endZdt, versionZdt, maxVersion, minAttr, maxAttr);

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
