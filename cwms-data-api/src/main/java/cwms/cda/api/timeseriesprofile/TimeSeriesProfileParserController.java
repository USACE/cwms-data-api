/*
 *
 * MIT License
 *
 * Copyright (c) 2024 Hydrologic Engineering Center
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
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
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE
 * SOFTWARE.
 */

package cwms.cda.api.timeseriesprofile;

import static cwms.cda.api.Controllers.CREATE;
import static cwms.cda.api.Controllers.DELETE;
import static cwms.cda.api.Controllers.FAIL_IF_EXISTS;
import static cwms.cda.api.Controllers.GET_ALL;
import static cwms.cda.api.Controllers.GET_ONE;
import static cwms.cda.api.Controllers.LOCATION_ID;
import static cwms.cda.api.Controllers.LOCATION_MASK;
import static cwms.cda.api.Controllers.OFFICE;
import static cwms.cda.api.Controllers.OFFICE_MASK;
import static cwms.cda.api.Controllers.STATUS_200;
import static cwms.cda.api.Controllers.STATUS_204;
import static cwms.cda.api.Controllers.STATUS_404;
import static cwms.cda.api.Controllers.STATUS_501;
import static cwms.cda.api.Controllers.requiredParam;
import static cwms.cda.data.dao.JooqDao.getDslContext;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.cda.api.Controllers;
import cwms.cda.data.dao.timeseriesprofile.TimeSeriesProfileParserDao;
import cwms.cda.data.dto.timeseriesprofile.TimeSeriesProfileParser;
import cwms.cda.data.dto.timeseriesprofile.TimeSeriesProfileParserColumnar;
import cwms.cda.data.dto.timeseriesprofile.TimeSeriesProfileParserIndexed;
import cwms.cda.data.dto.timeseriesprofile.TimeSeriesProfileParserList;
import cwms.cda.data.dto.timeseriesprofile.TimeSeriesProfileParsers;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import io.javalin.apibuilder.CrudHandler;
import io.javalin.core.util.Header;
import io.javalin.http.Context;
import io.javalin.plugin.openapi.annotations.HttpMethod;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiRequestBody;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import java.util.List;
import javax.servlet.http.HttpServletResponse;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;


public final class TimeSeriesProfileParserController implements CrudHandler {
    public static final String PARAMETER_ID_MASK = "parameter-id-mask";
    public static final String PARAMETER_ID = "parameter-id";
    public static final String TAG = "TimeSeries";
    private final MetricRegistry metrics;


    public TimeSeriesProfileParserController(MetricRegistry metrics) {
        this.metrics = metrics;
    }

    private Timer.Context markAndTime(String subject) {
        return Controllers.markAndTime(metrics, getClass().getName(), subject);
    }

    @OpenApi(
        queryParams = {
            @OpenApiParam(name = FAIL_IF_EXISTS, type = boolean.class, description = "If true, the parser will"
                    + " fail to save if the TimeSeriesProfileParser already exists"),
        },
        requestBody = @OpenApiRequestBody(
            content = {
                @OpenApiContent(from = TimeSeriesProfileParserIndexed.class, type = Formats.JSONV1),
                @OpenApiContent(from = TimeSeriesProfileParserColumnar.class, type = Formats.JSONV1),
            },
            required = true
        ),
        path = "/timeseries/parser",
        method = HttpMethod.POST,
        summary = "Parse TimeSeriesProfileData into a new Time Series Profile",
        tags = {TAG}
    )
    @Override
    public void create(@NotNull Context ctx) {
        try (final Timer.Context ignored = markAndTime(CREATE)) {
            DSLContext dsl = getDslContext(ctx);

            boolean failIfExists = ctx.queryParamAsClass(FAIL_IF_EXISTS, Boolean.class).getOrDefault(true);

            TimeSeriesProfileParserColumnar tspParserColumnar = null;
            TimeSeriesProfileParserIndexed tspParserIndexed = null;
            TimeSeriesProfileParserDao tspParserDao = new TimeSeriesProfileParserDao(dsl);
            try {
                tspParserColumnar = Formats.parseContent(Formats.parseHeader(Formats.JSONV1,
                        TimeSeriesProfileParserColumnar.class), ctx.body(), TimeSeriesProfileParserColumnar.class);

            } catch (Exception e) {
                tspParserIndexed = Formats.parseContent(Formats.parseHeader(Formats.JSONV1,
                        TimeSeriesProfileParserIndexed.class), ctx.body(), TimeSeriesProfileParserIndexed.class);
            }
            if (tspParserColumnar != null) {
                tspParserDao.storeTimeSeriesProfileParser(tspParserColumnar, failIfExists);
            } else {
                tspParserDao.storeTimeSeriesProfileParser(tspParserIndexed, failIfExists);
            }
            ctx.status(HttpServletResponse.SC_CREATED);
        }
    }

    @OpenApi(ignore = true)
    @Override
    public void update(@NotNull Context ctx, @NotNull String id) {
        ctx.status(HttpServletResponse.SC_NOT_IMPLEMENTED);
    }

    @OpenApi(
        queryParams = {
            @OpenApiParam(name = OFFICE, required = true, description = "The office associated with the "
                    + "TimeSeriesProfile"),
            @OpenApiParam(name = LOCATION_ID, required = true, description = "The location ID associated"
                    + " with the TimeSeriesProfile"),
        },
        pathParams = {
            @OpenApiParam(name = PARAMETER_ID, required = true, description = "The parameter ID "
                + "of the TimeSeriesProfileParser parameter"),
        },
        responses = {
            @OpenApiResponse(status = STATUS_204, description = "The TimeSeriesProfileParser was successfully deleted"),
            @OpenApiResponse(status = STATUS_404, description = "The provided ID did not find a"
                    + " TimeSeriesProfileParser object"),
            @OpenApiResponse(status = STATUS_501, description = "Requested format is not implemented")
        },
        path = "/timeseries/parser/{parameter-id}",
        method = HttpMethod.DELETE,
        summary = "Delete a TimeSeriesProfileParser by ID",
        tags = {TAG}
    )
    @Override
    public void delete(@NotNull Context ctx, @NotNull String parameterId) {
        try (final Timer.Context ignored = markAndTime(DELETE)) {
            DSLContext dsl = getDslContext(ctx);
            TimeSeriesProfileParserDao tspParserDao = new TimeSeriesProfileParserDao(dsl);
            String locationId = requiredParam(ctx, LOCATION_ID);
            String officeId = requiredParam(ctx, OFFICE);
            tspParserDao.deleteTimeSeriesProfileParser(locationId, parameterId, officeId);
            ctx.status(HttpServletResponse.SC_NO_CONTENT);
        }
    }

    @OpenApi(
        queryParams = {
            @OpenApiParam(name = PARAMETER_ID_MASK, description = "The ID mask of the TimeSeriesProfileParser"
                    + " parameter. Default is *"),
            @OpenApiParam(name = OFFICE_MASK, description = "The office mask associated with the "
                    + "TimeSeriesProfile. Default is *"),
            @OpenApiParam(name = LOCATION_MASK, description = "The location ID mask associated"
                    + " with the TimeSeriesProfile. Default is *"),
        },
        responses = {
            @OpenApiResponse(status = STATUS_200,
                description = "A TimeSeriesProfileParser object",
                content = {
                    @OpenApiContent(from = TimeSeriesProfileParsers.class, type = Formats.JSONV1),
                }),
            @OpenApiResponse(status = STATUS_404, description = "The provided combination of parameters did not"
                    + " find a TimeSeriesProfileParser object"),
            @OpenApiResponse(status = STATUS_501, description = "Requested format is not "
                    + "implemented")
        },
        path = "/timeseries/parser",
        method = HttpMethod.GET,
        summary = "Get a TimeSeriesProfileParser by ID",
        tags = {TAG}
    )
    @Override
    public void getAll(@NotNull Context ctx) {
        try (final Timer.Context ignored = markAndTime(GET_ALL)) {
            DSLContext dsl = getDslContext(ctx);

            String officeIdMask = ctx.queryParamAsClass(OFFICE_MASK, String.class).getOrDefault("*");
            String locationId = ctx.queryParamAsClass(LOCATION_MASK, String.class).getOrDefault("*");
            String parameterIdMask = ctx.queryParamAsClass(PARAMETER_ID_MASK, String.class).getOrDefault("*");

            TimeSeriesProfileParserDao tspParserDao = new TimeSeriesProfileParserDao(dsl);
            List<TimeSeriesProfileParser> tspParsers = tspParserDao.catalogTimeSeriesProfileParsers(locationId,
                    officeIdMask, parameterIdMask, true);
            String acceptHeader = ctx.header(Header.ACCEPT);
            // Added custom List wrapper due to serialization issues with List for TimeSeriesProfileParser Type handling.
            // Related to Jackson subclassing annotations @JSONTypeInfo and @JSONSubTypes
            // See issue: https://github.com/FasterXML/jackson-databind/issues/2185
            TimeSeriesProfileParserList parserList = new TimeSeriesProfileParserList(tspParsers);
            ContentType contentType = Formats.parseHeader(acceptHeader, TimeSeriesProfileParsers.class);
            String result = Formats.format(contentType, parserList, TimeSeriesProfileParsers.class);
            ctx.status(HttpServletResponse.SC_OK);
            ctx.result(result);
        }
    }

    @OpenApi(
        queryParams = {
            @OpenApiParam(name = OFFICE, required = true, description = "The office associated with the "
                    + "TimeSeriesProfile"),
            @OpenApiParam(name = LOCATION_ID, required = true, description = "The location ID associated"
                    + " with the TimeSeriesProfile"),
        },
        pathParams = {
            @OpenApiParam(name = PARAMETER_ID, required = true, description = "The ID of the TimeSeriesProfileParser")
        },
        responses = {
            @OpenApiResponse(status = STATUS_200,
                description = "A TimeSeriesProfileParser object",
                content = {
                    @OpenApiContent(from = TimeSeriesProfileParser.class, type = Formats.JSONV1),
                }),
            @OpenApiResponse(status = STATUS_404, description = "The provided combination of parameters did not"
                + " find a TimeSeriesProfileParser object"),
            @OpenApiResponse(status = STATUS_501, description = "Requested format is not "
                + "implemented")

        },
        path = "/timeseries/parser/{parameter-id}",
        method = HttpMethod.GET,
        summary = "Get a TimeSeriesProfileParser by ID",
        tags = {TAG}
    )

    @Override
    public void getOne(@NotNull Context ctx, @NotNull String parameterId) {
        try (final Timer.Context ignored = markAndTime(GET_ONE)) {
            DSLContext dsl = getDslContext(ctx);

            String officeId = requiredParam(ctx, OFFICE);
            String locationId = requiredParam(ctx, LOCATION_ID);
            TimeSeriesProfileParserDao tspParserDao = new TimeSeriesProfileParserDao(dsl);
            TimeSeriesProfileParser tspParser = tspParserDao.retrieveTimeSeriesProfileParser(locationId,
                    parameterId, officeId);
            String acceptHeader = ctx.header(Header.ACCEPT);
            ContentType contentType = Formats.parseHeader(acceptHeader, TimeSeriesProfileParser.class);
            String result = Formats.format(contentType, tspParser);
            ctx.status(HttpServletResponse.SC_OK);
            ctx.result(result);
        }
    }

}
