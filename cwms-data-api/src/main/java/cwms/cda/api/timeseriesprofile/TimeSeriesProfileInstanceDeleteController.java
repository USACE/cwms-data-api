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

import static cwms.cda.api.Controllers.DATE;
import static cwms.cda.api.Controllers.DELETE;
import static cwms.cda.api.Controllers.LOCATION_ID;
import static cwms.cda.api.Controllers.OFFICE;
import static cwms.cda.api.Controllers.OVERRIDE_PROTECTION;
import static cwms.cda.api.Controllers.PARAMETER_ID;
import static cwms.cda.api.Controllers.TIMEZONE;
import static cwms.cda.api.Controllers.VERSION;
import static cwms.cda.api.Controllers.VERSION_DATE;
import static cwms.cda.api.Controllers.requiredInstant;
import static cwms.cda.api.Controllers.requiredParam;
import static cwms.cda.data.dao.JooqDao.getDslContext;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.cda.data.dao.timeseriesprofile.TimeSeriesProfileInstanceDao;
import cwms.cda.data.dto.CwmsId;
import io.javalin.http.Context;
import io.javalin.http.Handler;
import io.javalin.plugin.openapi.annotations.HttpMethod;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import java.time.Instant;
import javax.servlet.http.HttpServletResponse;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;


public final class TimeSeriesProfileInstanceDeleteController extends TimeSeriesProfileInstanceBase implements Handler {

    public TimeSeriesProfileInstanceDeleteController(MetricRegistry metrics) {
        tspMetrics(metrics);
    }

    @OpenApi(
        queryParams = {
            @OpenApiParam(name = OFFICE, description = "The office associated with the"
                + " time series profile instance.", required = true),
            @OpenApiParam(name = TIMEZONE, description = "Specifies "
                    + "the time zone of the values of the begin and end fields. If this field is not specified, "
                    + "the default time zone of UTC shall be used."),
            @OpenApiParam(name = VERSION_DATE, type = Instant.class, description = "The version date of the"
                + " time series profile instance.", required = true),
            @OpenApiParam(name = DATE, type = Instant.class, description = "The first date of the"
                + " time series profile instance.", required = true),
            @OpenApiParam(name = OVERRIDE_PROTECTION, type = Boolean.class, description = "Override protection"
                + " for the time series profile instance. Default is true")
        },
        pathParams = {
            @OpenApiParam(name = LOCATION_ID, description = "The location ID of the"
                + " time series profile instance.", required = true),
            @OpenApiParam(name = PARAMETER_ID, description = "The key parameter of the"
                + " time series profile instance.", required = true),
            @OpenApiParam(name = VERSION, description = "The version of the"
                + " time series profile instance.", required = true),
        },
        method = HttpMethod.DELETE,
        summary = "Get all time series profile instances",
        tags = {TAG},
        responses = {
            @OpenApiResponse(status = "400", description = "Invalid input")
        }
    )
    @Override
    public void handle(@NotNull Context ctx) {
        try (final Timer.Context ignored = markAndTime(DELETE)) {
            DSLContext dsl = getDslContext(ctx);
            TimeSeriesProfileInstanceDao tspInstanceDao = getProfileInstanceDao(dsl);
            String office = requiredParam(ctx, OFFICE);
            String locationId = ctx.pathParam(LOCATION_ID);
            String timeZone = ctx.queryParamAsClass(TIMEZONE, String.class).getOrDefault("UTC");
            String version = ctx.pathParam(VERSION);
            String keyParameter = ctx.pathParam(PARAMETER_ID);
            Instant versionDate = requiredInstant(ctx, VERSION_DATE);
            Instant firstDate = requiredInstant(ctx, DATE);
            CwmsId tspIdentifier = new CwmsId.Builder().withName(locationId).withOfficeId(office).build();
            boolean overrideProtection = ctx.queryParamAsClass(OVERRIDE_PROTECTION, boolean.class).getOrDefault(true);
            tspInstanceDao.deleteTimeSeriesProfileInstance(tspIdentifier, keyParameter, version, firstDate, timeZone,
                    overrideProtection, versionDate);
            ctx.status(HttpServletResponse.SC_NO_CONTENT);
        }
    }
}
