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
import static cwms.cda.api.Controllers.METHOD;
import static cwms.cda.api.Controllers.OVERRIDE_PROTECTION;
import static cwms.cda.api.Controllers.PROFILE_DATA;
import static cwms.cda.api.Controllers.VERSION;
import static cwms.cda.api.Controllers.VERSION_DATE;
import static cwms.cda.api.Controllers.requiredInstant;
import static cwms.cda.api.Controllers.requiredParam;
import static cwms.cda.data.dao.JooqDao.getDslContext;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.cda.data.dao.StoreRule;
import cwms.cda.data.dao.timeseriesprofile.TimeSeriesProfileInstanceDao;
import cwms.cda.data.dto.timeseriesprofile.TimeSeriesProfile;
import cwms.cda.formatters.Formats;
import io.javalin.http.Context;
import io.javalin.http.Handler;
import io.javalin.plugin.openapi.annotations.HttpMethod;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiRequestBody;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import java.time.Instant;
import javax.servlet.http.HttpServletResponse;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;


public final class TimeSeriesProfileInstanceCreateController extends TimeSeriesProfileInstanceBase implements Handler {
    public TimeSeriesProfileInstanceCreateController(MetricRegistry metrics) {
        tspMetrics(metrics);
    }

    @OpenApi(
        queryParams = {
            @OpenApiParam(name = METHOD, type = StoreRule.class, description = "The method of storing the"
                + " time series profile instance. Default is REPLACE_ALL"),
            @OpenApiParam(name = OVERRIDE_PROTECTION, type = Boolean.class, description = "Override protection"
                + " for the time series profile instance. Default is false"),
            @OpenApiParam(name = VERSION_DATE, type = Long.class, description = "The version date of the"
                + " time series profile instance.", required = true),
            @OpenApiParam(name = PROFILE_DATA, required = true, description = "The profile data of the"
                + " time series profile instance"),
            @OpenApiParam(name = VERSION, description = "The version of the"
                + " time series profile instance.", required = true),
        },
        method = HttpMethod.POST,
        summary = "Create a new time series profile instance",
        tags = {TAG},
        requestBody = @OpenApiRequestBody(content = {@OpenApiContent(from = TimeSeriesProfile.class)}),
        responses = {
            @OpenApiResponse(status = "201", description = "Time series profile instance created"),
            @OpenApiResponse(status = "400", description = "Invalid input"),
            @OpenApiResponse(status = "409", description = "Time series profile instance already exists")
        }
    )
    @Override
    public void handle(@NotNull Context ctx) {
        try (final Timer.Context ignored = markAndTime(CREATE)) {
            DSLContext dsl = getDslContext(ctx);
            TimeSeriesProfileInstanceDao tspInstanceDao = getProfileInstanceDao(dsl);
            TimeSeriesProfile timeSeriesProfile = Formats.parseContent(Formats.parseHeader(Formats.JSONV1,
                    TimeSeriesProfile.class), ctx.body(), TimeSeriesProfile.class);
            String profileData = requiredParam(ctx, PROFILE_DATA);
            String versionId = requiredParam(ctx, VERSION);
            Instant versionDate = requiredInstant(ctx, VERSION_DATE);
            StoreRule storeRule = ctx.queryParamAsClass(METHOD, StoreRule.class).getOrDefault(StoreRule.REPLACE_ALL);
            boolean overrideProtection = ctx.queryParamAsClass(OVERRIDE_PROTECTION, boolean.class)
                    .getOrDefault(false);
            tspInstanceDao.storeTimeSeriesProfileInstance(timeSeriesProfile, profileData, versionDate,
                    versionId, storeRule.toString(), overrideProtection);
            ctx.status(HttpServletResponse.SC_CREATED);
        }
    }
}
