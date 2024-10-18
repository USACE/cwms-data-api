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
import cwms.cda.data.dao.timeseriesprofile.TimeSeriesProfileParserDao;
import io.javalin.http.Context;
import io.javalin.http.Handler;
import io.javalin.plugin.openapi.annotations.HttpMethod;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import javax.servlet.http.HttpServletResponse;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;


public final class TimeSeriesProfileParserDeleteController extends TimeSeriesProfileParserBase implements Handler {

    public TimeSeriesProfileParserDeleteController(MetricRegistry metrics) {
        tspMetrics(metrics);
    }

    @OpenApi(
        queryParams = {
            @OpenApiParam(name = OFFICE, required = true, description = "The office associated with the "
                + "TimeSeriesProfile"),
        },
        pathParams = {
            @OpenApiParam(name = PARAMETER_ID, required = true, description = "The parameter ID "
                + "of the TimeSeriesProfileParser parameter"),
            @OpenApiParam(name = LOCATION_ID, required = true, description = "The location ID associated"
                + " with the TimeSeriesProfile"),
        },
        responses = {
            @OpenApiResponse(status = STATUS_204, description = "The TimeSeriesProfileParser was successfully deleted"),
            @OpenApiResponse(status = STATUS_404, description = "The provided ID did not find a"
                + " TimeSeriesProfileParser object"),
            @OpenApiResponse(status = STATUS_501, description = "Requested format is not implemented")
        },
        method = HttpMethod.DELETE,
        summary = "Delete a TimeSeriesProfileParser by ID",
        tags = {TAG}
    )
    @Override
    public void handle(@NotNull Context ctx) {
        try (final Timer.Context ignored = markAndTime(DELETE)) {
            DSLContext dsl = getDslContext(ctx);
            TimeSeriesProfileParserDao tspParserDao = getParserDao(dsl);
            String parameterId = ctx.pathParam(PARAMETER_ID);
            String locationId = ctx.pathParam(LOCATION_ID);
            String officeId = requiredParam(ctx, OFFICE);
            tspParserDao.deleteTimeSeriesProfileParser(locationId, parameterId, officeId);
            ctx.status(HttpServletResponse.SC_NO_CONTENT);
        }
    }
}
