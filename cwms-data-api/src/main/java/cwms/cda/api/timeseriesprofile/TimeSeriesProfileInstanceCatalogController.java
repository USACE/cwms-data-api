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
import static cwms.cda.api.Controllers.LOCATION_MASK;
import static cwms.cda.data.dao.JooqDao.getDslContext;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.cda.data.dao.timeseriesprofile.TimeSeriesProfileInstanceDao;
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
import java.util.List;
import javax.servlet.http.HttpServletResponse;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;


public final class TimeSeriesProfileInstanceCatalogController extends TimeSeriesProfileInstanceBase implements Handler {
    public TimeSeriesProfileInstanceCatalogController(MetricRegistry metrics) {
        tspMetrics(metrics);
    }

    @OpenApi(
        queryParams = {
            @OpenApiParam(name = OFFICE_MASK, description = "The office mask of the"
                + " time series profile instance. Default is *"),
            @OpenApiParam(name = LOCATION_MASK, description = "The location ID mask of the"
                + " time series profile instance. Default is *"),
            @OpenApiParam(name = PARAMETER_ID_MASK, description = "The parameter ID mask of the"
                + " time series profile instance. Default is *"),
            @OpenApiParam(name = VERSION_MASK, description = "The version mask of the"
                + " time series profile instance. Default is *"),
        },
        method = HttpMethod.GET,
        summary = "Get all time series profile instances. Masks can be provided to filter the results, but are"
            + "not necessary to get a response.",
        tags = {TAG},
        responses = {
            @OpenApiResponse(status = STATUS_200,
                description = "A TimeSeriesProfileInstance object",
                content = {
                    @OpenApiContent(from = TimeSeriesProfileInstance.class, type = Formats.JSONV1),
                    @OpenApiContent(from = TimeSeriesProfileInstance.class, type = Formats.XMLV2),
                }),
            @OpenApiResponse(status = "400", description = "Invalid input")
        }
    )
    @Override
    public void handle(@NotNull Context ctx) {
        try (final Timer.Context ignored = markAndTime(GET_ALL)) {
            DSLContext dsl = getDslContext(ctx);
            TimeSeriesProfileInstanceDao tspInstanceDao = getProfileInstanceDao(dsl);
            String officeMask = ctx.queryParamAsClass(OFFICE_MASK, String.class).getOrDefault("*");
            String locationMask = ctx.queryParamAsClass(LOCATION_MASK, String.class).getOrDefault("*");
            String parameterIdMask = ctx.queryParamAsClass(PARAMETER_ID_MASK, String.class).getOrDefault("*");
            String versionMask = ctx.queryParamAsClass(VERSION_MASK, String.class).getOrDefault("*");
            List<TimeSeriesProfileInstance> retrievedInstances = tspInstanceDao
                    .catalogTimeSeriesProfileInstances(officeMask, locationMask, parameterIdMask, versionMask);
            String acceptHeader = ctx.header(Header.ACCEPT);
            ContentType contentType = Formats.parseHeader(acceptHeader, TimeSeriesProfileInstance.class);
            String result = Formats.format(contentType, retrievedInstances, TimeSeriesProfileInstance.class);
            ctx.result(result);
            ctx.status(HttpServletResponse.SC_OK);
        }
    }
}
