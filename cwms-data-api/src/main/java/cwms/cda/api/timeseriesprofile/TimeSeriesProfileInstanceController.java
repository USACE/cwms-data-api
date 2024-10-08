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

import static cwms.cda.api.Controllers.*;
import static cwms.cda.data.dao.JooqDao.getDslContext;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.cda.data.dao.timeseriesprofile.TimeSeriesProfileInstanceDao;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.timeseriesprofile.TimeSeriesProfileInstance;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import io.javalin.core.util.Header;
import io.javalin.http.Context;
import io.javalin.http.Handler;
import io.javalin.plugin.openapi.annotations.HttpMethod;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import javax.servlet.http.HttpServletResponse;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;


public final class TimeSeriesProfileInstanceController extends TimeSeriesProfileInstanceBase implements Handler {

    public TimeSeriesProfileInstanceController(MetricRegistry metrics) {
        tspMetrics(metrics);
    }

    @OpenApi(
        queryParams = {
            @OpenApiParam(name = OFFICE, description = "The office associated with the"
                    + " time series profile instance.", required = true),
            @OpenApiParam(name = TIMEZONE,  description = "Specifies "
                + "the time zone of the values of the begin and end fields (unless "
                + "otherwise specified). If this field is not specified, the default time zone "
                + "of UTC shall be used."),
            @OpenApiParam(name = VERSION_DATE, type = Instant.class, description = "The version date of the"
                + " time series profile instance. Default is the min or max version date, depending on the maxVersion"),
            @OpenApiParam(name = UNIT, description = "The units of the"
                + " time series profile instance. Provided as a list separated by ','", required = true),
            @OpenApiParam(name = START_INCLUSIVE, type = Boolean.class, description = "The start inclusive of the"
                + " time series profile instance. Default is true"),
            @OpenApiParam(name = END_INCLUSIVE, type = Boolean.class, description = "The end inclusive of the"
                + " time series profile instance. Default is true"),
            @OpenApiParam(name = PREVIOUS, type = boolean.class, description = "Whether to include the previous "
                + " time window of the time series profile instance. Default is false"),
            @OpenApiParam(name = NEXT, type = boolean.class, description = "Whether to include the next time window "
                    + "of the time series profile instance. Default is false"),
            @OpenApiParam(name = MAX_VERSION, type = boolean.class, description = "Whether to use the max version"
                + " date of the time series profile instance. Default is false. If no version date is provided, and"
                    + " maxVersion is false, the min version date will be used."),
            @OpenApiParam(name = START, type = Instant.class, description = "The start of the"
                + " time series profile instance. Default is the year 1800"),
            @OpenApiParam(name = END, type = Instant.class, description = "The end of the"
                + " time series profile instance. Default is the year 3000"),
            @OpenApiParam(name = PAGE, description = "The page of the"
                + " time series profile instance."),
            @OpenApiParam(name = PAGE_SIZE, type = Integer.class, description = "The page size of the"
                + " time series profile instance. Default is 500"),
        },
        pathParams = {
            @OpenApiParam(name = LOCATION_ID, description = "The location ID of the"
                    + " time series profile instance.", required = true),
            @OpenApiParam(name = PARAMETER_ID, description = "The key parameter of the"
                    + " time series profile instance.", required = true),
            @OpenApiParam(name = VERSION, description = "The version of the"
                    + " time series profile instance.", required = true),
        },
        method = HttpMethod.GET,
        summary = "Get all time series profile instances",
        tags = {TAG},
        responses = {
            @OpenApiResponse(status = STATUS_200,
                description = "A TimeSeriesProfileParser object",
                content = {
                    @OpenApiContent(from = TimeSeriesProfileInstance.class, type = Formats.JSONV1),
                    @OpenApiContent(from = TimeSeriesProfileInstance.class, type = Formats.XMLV2),
                }),
            @OpenApiResponse(status = "400", description = "Invalid input")
        }
    )
    @Override
    public void handle(@NotNull Context ctx) {
        try (final Timer.Context ignored = markAndTime(GET_ONE)) {
            DSLContext dsl = getDslContext(ctx);
            TimeSeriesProfileInstanceDao tspInstanceDao = new TimeSeriesProfileInstanceDao(dsl);
            String officeId = requiredParam(ctx, OFFICE);
            String locationId = ctx.pathParam(LOCATION_ID);
            String keyParameter = ctx.pathParam(PARAMETER_ID);
            String version = ctx.pathParam(VERSION);
            List<String> unit = Arrays.asList(requiredParam(ctx, UNIT).split(","));
            Instant startTime = Instant.ofEpochMilli(Long.parseLong(ctx.queryParamAsClass(START, String.class)
                    .getOrDefault(String.valueOf(Instant.now().toEpochMilli()))));
            Instant endTime = Instant.ofEpochMilli(Long.parseLong(ctx.queryParamAsClass(END, String.class)
                    .getOrDefault(String.valueOf(Instant.now().toEpochMilli()))));
            String timeZone = ctx.queryParamAsClass(TIMEZONE, String.class).getOrDefault("UTC");
            boolean startInclusive = ctx.queryParamAsClass(START_INCLUSIVE, boolean.class)
                    .getOrDefault(true);
            boolean endInclusive = ctx.queryParamAsClass(END_INCLUSIVE, boolean.class)
                    .getOrDefault(true);
            boolean previous = ctx.queryParamAsClass(PREVIOUS, boolean.class).getOrDefault(false);
            boolean next = ctx.queryParamAsClass(NEXT, boolean.class).getOrDefault(false);
            Instant versionDate = ctx.queryParamAsClass(VERSION_DATE, String.class).getOrDefault(null) == null
                    ? null : Instant.ofEpochMilli(Long.parseLong(ctx.queryParamAsClass(VERSION_DATE, String.class)
                    .getOrDefault(null)));
            boolean maxVersion = ctx.queryParamAsClass(MAX_VERSION, boolean.class)
                    .getOrDefault(false);
            String page = ctx.queryParam(PAGE);
            int pageSize = ctx.queryParamAsClass(PAGE_SIZE, Integer.class).getOrDefault(500);
            CwmsId tspIdentifier = new CwmsId.Builder().withOfficeId(officeId).withName(locationId).build();
            TimeSeriesProfileInstance returnedInstance = tspInstanceDao.retrieveTimeSeriesProfileInstance(tspIdentifier,
                    keyParameter, version, unit, startTime, endTime, timeZone, startInclusive, endInclusive,
                    previous, next, versionDate, maxVersion, page, pageSize);
            String acceptHeader = ctx.header(Header.ACCEPT);
            ContentType contentType = Formats.parseHeader(acceptHeader, TimeSeriesProfileInstance.class);
            String result = Formats.format(contentType, returnedInstance);
            ctx.result(result);
            ctx.status(HttpServletResponse.SC_OK);
        }
    }
}
