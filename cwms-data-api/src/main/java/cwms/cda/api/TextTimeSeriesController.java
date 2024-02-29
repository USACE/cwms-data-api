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
import com.fasterxml.jackson.databind.ObjectMapper;
import cwms.cda.api.errors.CdaError;
import cwms.cda.data.dao.texttimeseries.TimeSeriesTextDao;
import cwms.cda.data.dao.texttimeseries.TimeSeriesTextMode;
import cwms.cda.data.dto.texttimeseries.TextTimeSeries;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.json.JsonV2;
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
import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.servlet.http.HttpServletResponse;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;


public class TextTimeSeriesController implements CrudHandler {
    private static final Logger logger = Logger.getLogger(TextTimeSeriesController.class.getName());
    private static final String TAG = "Text-TimeSeries";

    public static final String REPLACE_ALL = "replace-all";
    public static final String MODE = "mode";

    private final MetricRegistry metrics;

    public static final boolean DEFAULT_CREATE_REPLACE_ALL = false;
    public static final boolean DEFAULT_UPDATE_REPLACE_ALL = true;

    public static final String CONTEXT_TOKEN = "{context-path}";
    public static final String OFFICE_TOKEN = "{office}";
    public static final String CLOB_TOKEN = "{clob-id}";
    public static final String URL_TEMPLATE = String.format("%s/clob/ignored?clob-id=%s&office-id=%s", CONTEXT_TOKEN, CLOB_TOKEN, OFFICE_TOKEN);
    private ReplaceUtils.OperatorBuilder urlBuilder;

    public TextTimeSeriesController(MetricRegistry metrics, String contextPath) {
        this.metrics = metrics;

        urlBuilder = new ReplaceUtils.OperatorBuilder()
                .withTemplate(URL_TEMPLATE)
                .withOperatorKey(CLOB_TOKEN)
                .replace(CONTEXT_TOKEN, contextPath, false);

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
                    @OpenApiParam(name = MODE, required = true, type = TimeSeriesTextMode.class,
                            description = "Type of Text TimeSeries to retrieve."
                                    + "<table>\n"
                                    + "<tr><td>ALL</td><td>Retrieve Standard and Regular text timeseries.</td></tr>\n"
                                    + "<tr><td>STANDARD</td><td>Retrieve Standard text timeseries.</td></tr>\n"
                                    + "<tr><td>REGULAR</td><td>Retrieve Regular text timeseries.</td></tr>\n"
                                    + "</table>"
                    ),
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
        TimeSeriesTextMode mode = ctx.queryParamAsClass(MODE, TimeSeriesTextMode.class).getOrDefault(TimeSeriesTextMode.ALL);


        String formatHeader = ctx.header(Header.ACCEPT);
        ContentType contentType = Formats.parseHeaderAndQueryParm(formatHeader, "");
        try (Timer.Context ignored = markAndTime(GET_ALL)) {
            DSLContext dsl = getDslContext(ctx);
            TimeSeriesTextDao dao = getDao(dsl);

            String textMask = "*";

            urlBuilder.replace(OFFICE_TOKEN, office);

            TextTimeSeries textTimeSeries = dao.retrieveFromDao(mode, office, tsId, textMask,
                    beginZdt, endZdt, versionZdt,
                    maxVersion, minAttr, maxAttr, urlBuilder.build());

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
            TextTimeSeries tts = deserializeBody(ctx, formatHeader);
            TimeSeriesTextDao dao = getDao(dsl);

            boolean maxVersion = true;

            boolean replaceAll = ctx.queryParamAsClass(REPLACE_ALL, Boolean.class).getOrDefault(DEFAULT_CREATE_REPLACE_ALL);
            dao.create(tts, maxVersion, replaceAll);
            ctx.status(HttpServletResponse.SC_CREATED);
        } catch (IOException ex) {
            CdaError re = new CdaError("Failed to process create request");
            logger.log(Level.SEVERE, re.toString(), ex);
            ctx.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR).json(re);
        }
    }

    private static TextTimeSeries deserializeBody(@NotNull Context ctx, String formatHeader) throws IOException {
        TextTimeSeries tts;

        if (ContentType.equivalent(Formats.JSONV2, formatHeader)) {
            ObjectMapper om = JsonV2.buildObjectMapper();
            tts = om.readValue(ctx.bodyAsInputStream(), TextTimeSeries.class);
        } else {
            throw new IllegalArgumentException("Unsupported format: " + formatHeader);
        }

        return tts;
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
            TextTimeSeries tts = deserializeBody(ctx, formatHeader);
            DSLContext dsl = getDslContext(ctx);

            TimeSeriesTextDao dao = getDao(dsl);
            dao.store(tts,maxVersion, replaceAll);

        } catch (IOException e) {
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
                @OpenApiParam(name = MODE, required = true, type =
                        TimeSeriesTextMode.class,
                        description = "Type of delete to perform. \n"
                                + "<table>\n"
                                + "<tr><td>ALL</td><td>Delete Standard and Regular text timeseries values for the specified time series.</td></tr>\n"
                                + "<tr><td>STANDARD</td><td>Delete Standard text time series for the specified text timeseries.</td></tr>\n"
                                + "<tr><td>REGULAR</td><td>Delete Regular text timeseries values for the specified time series.</td></tr>\n"
                                + "</table>"
                ),
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

            TimeSeriesTextMode mode = ctx.queryParamAsClass(MODE, TimeSeriesTextMode.class).get();

            ZonedDateTime beginZdt = requiredZdt(ctx, BEGIN);
            ZonedDateTime endZdt = requiredZdt(ctx, END);
            ZonedDateTime versionZdt = queryParamAsZdt(ctx, VERSION_DATE);

            boolean maxVersion = versionZdt == null;

            Long minAttr = ctx.queryParamAsClass(Controllers.MIN_ATTRIBUTE, Long.class).getOrDefault(null);
            Long maxAttr = ctx.queryParamAsClass(Controllers.MAX_ATTRIBUTE, Long.class).getOrDefault(null);

            TimeSeriesTextDao dao = getDao(dsl);

            dao.delete(mode, office, textTimeSeriesId, mask, beginZdt, endZdt, versionZdt, maxVersion, minAttr, maxAttr);

            ctx.status(HttpServletResponse.SC_NO_CONTENT);
        }
    }

}
