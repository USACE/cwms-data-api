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

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.databind.ObjectMapper;
import cwms.cda.api.errors.CdaError;
import cwms.cda.data.dao.binarytimeseries.TimeSeriesBinaryDao;
import cwms.cda.data.dto.binarytimeseries.BinaryTimeSeries;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.json.JsonV2;
import cwms.cda.helpers.ReplaceUtils;
import io.javalin.apibuilder.CrudHandler;
import io.javalin.core.util.Header;
import io.javalin.http.Context;
import io.javalin.http.HttpCode;
import io.javalin.http.HttpResponseException;
import io.javalin.plugin.openapi.annotations.HttpMethod;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiRequestBody;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.time.Instant;
import java.util.logging.Level;
import java.util.logging.Logger;

import static cwms.cda.api.Controllers.*;
import static cwms.cda.data.dao.JooqDao.getDslContext;


public class BinaryTimeSeriesController implements CrudHandler {
    private static final Logger logger = Logger.getLogger(BinaryTimeSeriesController.class.getName());
    private static final String TAG = "Binary-TimeSeries";

    public static final String REPLACE_ALL = "replace-all";
    private static final String DEFAULT_BIN_TYPE_MASK = "*" ;
    public static final String BINARY_TYPE_MASK = "binary-type-mask";
    private static final String CONTEXT_TOKEN = "{context-path}";
    private static final String OFFICE_TOKEN = "{office}";
    private static final String NAME_TOKEN = "{name}";
    private static final String VERSION_DATE_TOKEN = "{version-date}";
    private static final String DATE_TOKEN = "{date}";
    private static final String URL_TEMPLATE = String.format("%s/blob/ignored?%s=%s&%s=%s&%s=%s&%s=%s",
            CONTEXT_TOKEN, OFFICE, OFFICE_TOKEN, NAME, NAME_TOKEN, VERSION_DATE, VERSION_DATE_TOKEN,
            DATE, DATE_TOKEN);
    private final MetricRegistry metrics;
    private final ReplaceUtils.OperatorBuilder urlBuilder;


    public BinaryTimeSeriesController(MetricRegistry metrics, String contextPath) {
        this.metrics = metrics;
        urlBuilder = new ReplaceUtils.OperatorBuilder()
                .withTemplate(URL_TEMPLATE)
                .withOperatorKey(DATE_TOKEN)
                .replace(CONTEXT_TOKEN, contextPath, false);
    }

    @NotNull
    protected TimeSeriesBinaryDao getDao(DSLContext dsl) {
        return new TimeSeriesBinaryDao(dsl);
    }


    private Timer.Context markAndTime(String subject) {
        return Controllers.markAndTime(metrics, getClass().getName(), subject);
    }


    @OpenApi(
            queryParams = {
                    @OpenApiParam(name = OFFICE, required = true, description = "Specifies the owning office of "
                            + "the Binary TimeSeries whose data is to be included in the response."),
                    @OpenApiParam(name = NAME, required = true, description = "Specifies the id of the "
                            + "binary timeseries"),
                    @OpenApiParam(name = BINARY_TYPE_MASK, description = "The "
                            + "data type pattern expressed as either an internet media type "
                            + "(e.g. 'image/*') or a file extension (e.g. '.*'). Use glob-style "
                            + "wildcard characters as shown above instead of sql-style wildcard "
                        + "characters for pattern matching. Default is:" + DEFAULT_BIN_TYPE_MASK),
                    @OpenApiParam(name = TIMEZONE,  description = "Specifies "
                            + "the time zone of the values of the begin and end fields (unless "
                            + "otherwise specified). If this field is not specified, "
                            + "the default time zone of UTC shall be used."),
                    @OpenApiParam(name = BEGIN, required = true, description = "The start of the time window"),
                    @OpenApiParam(name = END, required = true, description = "The end of the time window"),
                    @OpenApiParam(name = VERSION_DATE, description = "The version date for the time series."),
                    @OpenApiParam(name = LENGTH_LIMIT, description = "Maximum number of kilobytes to return for each row in the initial payload. " +
                            "Binary values over this threshold can be streamed using a URL reference in the row level DTO. " +
                            "The default is 64 kilobytes.", type = Integer.class)

            },
            responses = {
                    @OpenApiResponse(status = STATUS_200,
                            content = {
                                    @OpenApiContent(type = Formats.JSONV2, from = BinaryTimeSeries.class)
                            }
                    )},
            tags = {TAG}
    )
    @Override
    public void getAll(@NotNull Context ctx) {
        String office = requiredParam(ctx, OFFICE);
        String tsId = requiredParam(ctx, NAME);
        Instant begin = requiredInstant(ctx, BEGIN);
        Instant end = requiredInstant(ctx, END);
        Instant version = queryParamAsInstant(ctx, VERSION_DATE);
        String binTypeMask = ctx.queryParamAsClass(BINARY_TYPE_MASK, String.class).getOrDefault(DEFAULT_BIN_TYPE_MASK);
        int kiloByteLimit = ctx.queryParamAsClass(LENGTH_LIMIT, Integer.class).getOrDefault(64);

        String formatHeader = ctx.header(Header.ACCEPT);
        ContentType contentType = Formats.parseHeaderAndQueryParm(formatHeader, "");
        try (Timer.Context ignored = markAndTime(GET_ALL)) {
            urlBuilder.replace(OFFICE_TOKEN, office)
                    .replace(NAME_TOKEN, tsId)
                    .replace(VERSION_DATE_TOKEN, ctx.queryParam(VERSION_DATE));
            DSLContext dsl = getDslContext(ctx);
            TimeSeriesBinaryDao dao = getDao(dsl);

            BinaryTimeSeries binaryTimeSeries = dao.retrieve(office, tsId, binTypeMask,
                    begin,
                    end,
                    version==null? null: version,
                    kiloByteLimit, urlBuilder);

            ctx.contentType(contentType.toString());

            String result = Formats.format(contentType, binaryTimeSeries);
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
            description = "Create new BinaryTimeSeries",
            requestBody = @OpenApiRequestBody(
                    content = {
                            @OpenApiContent(from = BinaryTimeSeries.class, type = Formats.JSONV2)
                    },
                    required = true),
            queryParams = {

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
            BinaryTimeSeries tts = deserializeBody(ctx, formatHeader);
            TimeSeriesBinaryDao dao = getDao(dsl);

            boolean maxVersion = true;
            boolean replaceAll = ctx.queryParamAsClass(REPLACE_ALL, Boolean.class).getOrDefault(false);

            dao.store(tts, maxVersion, replaceAll);
            ctx.status(HttpServletResponse.SC_CREATED);
        } catch (IOException ex) {
            throw new HttpResponseException(HttpCode.NOT_ACCEPTABLE.getStatus(),"Unable to parse request body");
        }
    }

    @OpenApi(
            description = "Updates a binary timeseries",
            pathParams = {
                    @OpenApiParam(name = TIMESERIES, description = "The id of the binary timeseries to be updated"),
            },
            queryParams = {

                    @OpenApiParam(name = REPLACE_ALL, type = Boolean.class)
            },
            requestBody = @OpenApiRequestBody(
                    content = {
                            @OpenApiContent(from = BinaryTimeSeries.class, type = Formats.JSONV2),
                    },
                    required = true
            ),
            method = HttpMethod.PATCH,
            path = "/timeseries/binary/{timeseries}",
            tags = {TAG}
    )
    @Override
    public void update(@NotNull Context ctx, @NotNull String oldBinaryTimeSeriesId) {

        try (Timer.Context ignored = markAndTime(UPDATE)) {
            boolean maxVersion = true;
            boolean replaceAll = ctx.queryParamAsClass(REPLACE_ALL, Boolean.class).getOrDefault(false);
            String reqContentType = ctx.req.getContentType();
            String formatHeader = reqContentType != null ? reqContentType : Formats.JSONV2;
            BinaryTimeSeries tts = deserializeBody(ctx, formatHeader);
            DSLContext dsl = getDslContext(ctx);

            TimeSeriesBinaryDao dao = getDao(dsl);
            dao.store(tts,maxVersion, replaceAll);

        } catch (IOException e) {
            throw new HttpResponseException(HttpCode.NOT_ACCEPTABLE.getStatus(),"Unable to parse request body");
        }
    }


    @OpenApi(
            description = "Deletes requested binary timeseries id",
            pathParams = {
                    @OpenApiParam(name = TIMESERIES, description = "The time series identifier to be deleted"),
            },
            queryParams = {
                    @OpenApiParam(name = OFFICE, required = true, description = "Specifies the "
                            + "owning office of the timeseries identifier to be deleted"),
                    @OpenApiParam(name = BINARY_TYPE_MASK, description= "The data "
                            + "type pattern expressed as either an internet media type "
                            + "(e.g. 'image/*') or a file extension (e.g. '.*'). Use glob-style "
                            + "wildcard characters as shown above instead of sql-style wildcard "
                            + "characters for pattern matching. Default:" + DEFAULT_BIN_TYPE_MASK),
                    @OpenApiParam(name = TIMEZONE, description = "Specifies "
                            + "the time zone of the values of the begin and end fields (unless "
                            + "otherwise specified). If this field is not specified, "
                            + "the default time zone of UTC shall be used."),
                    @OpenApiParam(name = BEGIN, required = true, description = "The start of the time"
                            + " window"),
                    @OpenApiParam(name = END, required = true, description = "The end of the time window. "),
                    @OpenApiParam(name = VERSION_DATE, description = "The version date for the time "
                            + "series.  If not specified, the minimum or maximum version date "
                            + "(depending on p_max_version) is used.")
            },
            method = HttpMethod.DELETE,
            tags = {TAG}
    )
    @Override
    public void delete(@NotNull Context ctx, @NotNull String binaryTimeSeriesId) {
        try (Timer.Context ignored = markAndTime(DELETE)) {
            DSLContext dsl = getDslContext(ctx);
            String office = requiredParam(ctx, OFFICE);

            String mask = ctx.queryParamAsClass(BINARY_TYPE_MASK, String.class).getOrDefault(DEFAULT_BIN_TYPE_MASK);

            Instant begin = requiredInstant(ctx, BEGIN);
            Instant end = requiredInstant(ctx, END);
            Instant version = queryParamAsInstant(ctx, VERSION_DATE);

            TimeSeriesBinaryDao dao = getDao(dsl);

            dao.delete(office, binaryTimeSeriesId, mask, begin, end, version);

            ctx.status(HttpServletResponse.SC_NO_CONTENT);
        }
    }
    private static BinaryTimeSeries deserializeBody(@NotNull Context ctx, String formatHeader) throws IOException {
        BinaryTimeSeries bts;


        if (ContentType.equivalent(Formats.JSONV2, formatHeader)) {
            ObjectMapper om = JsonV2.buildObjectMapper();

             /*
              If the body is more than 1Mb then this:
              bts = om.readValue(ctx.body(), BinaryTimeSeries.class) generates a warning that
              looks like:  WARNING [http-nio-auto-1-exec-3] io.javalin.core.util.JavalinLogger.warn Body greater than max size (1000000 bytes)
              Javalin will then automatically return 413 and close the connection.
                 HTTP/1.1 413
                 Strict-Transport-Security: max-age=31536000;includeSubDomains
                 X-Frame-Options: SAMEORIGIN
                 X-Content-Type-Options: nosniff
                 X-XSS-Protection: 1; mode=block
                 Content-Type: application/json
                 Content-Length: 138
                 Date: Wed, 28 Feb 2024 21:27:33 GMT
                 Connection: close
                 {
                     "title": "Payload Too Large",
                     "status": 413,
                     "type": "https://javalin.io/documentation#error-responses",
                     "details": {
                     }
                 }
                 In ApiServlet we can adjust the maxRequest size via code like:
                  config.maxRequestSize = 2000000L;  but that just sets the bar slightly higher.
                  Javalin doesn't want to read big bodies b/c I think it holds on to them.
                  We know this end-point can potentially deal with big bodies so the solution is
                  just read the object from the body as an input stream.
              */
            bts = om.readValue(ctx.bodyAsInputStream(), BinaryTimeSeries.class);
        } else {
            throw new IllegalArgumentException("Unsupported format: " + formatHeader);
        }

        return bts;
    }

}
