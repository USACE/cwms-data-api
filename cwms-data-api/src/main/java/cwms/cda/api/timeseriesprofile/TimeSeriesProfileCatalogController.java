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

import static cwms.cda.api.Controllers.GET_ALL;
import static cwms.cda.api.Controllers.LOCATION_MASK;
import static cwms.cda.api.Controllers.OFFICE_MASK;
import static cwms.cda.api.Controllers.PAGE;
import static cwms.cda.api.Controllers.PAGE_SIZE;
import static cwms.cda.api.Controllers.STATUS_200;
import static cwms.cda.api.Controllers.STATUS_400;
import static cwms.cda.api.Controllers.STATUS_404;
import static cwms.cda.data.dao.JooqDao.getDslContext;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.cda.data.dao.timeseriesprofile.TimeSeriesProfileDao;
import cwms.cda.data.dto.timeseriesprofile.TimeSeriesProfileList;
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
import javax.servlet.http.HttpServletResponse;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;


public final class TimeSeriesProfileCatalogController extends TimeSeriesProfileBase implements Handler {
    public static final String PARAMETER_ID_MASK = "parameter-id-mask";

    public TimeSeriesProfileCatalogController(MetricRegistry metrics) {
        tspMetrics(metrics);
    }

    @OpenApi(
        queryParams = {
            @OpenApiParam(name = OFFICE_MASK, description = "The office mask for the time series profile. "
                    + "Default is *"),
            @OpenApiParam(name = LOCATION_MASK, description = "The location mask for the time series profile. "
                    + "Default is *"),
            @OpenApiParam(name = PARAMETER_ID_MASK, description = "The key parameter mask for the time series "
                    + "profile. Default is *"),
            @OpenApiParam(name = PAGE, description = "The page cursor. Default is null"),
            @OpenApiParam(name = PAGE_SIZE, description = "The page size. Default is 500")
        },
        method = HttpMethod.GET,
        summary = "Get a time series profile",
        tags = {TAG},
        responses = {
            @OpenApiResponse(status = STATUS_200,
                description = "A TimeSeriesProfile object",
                content = {
                    @OpenApiContent(from = TimeSeriesProfileList.class, type = Formats.JSONV1),
                    @OpenApiContent(from = TimeSeriesProfileList.class, type = Formats.JSON),
                }),
            @OpenApiResponse(status = STATUS_400, description = "Invalid input"),
            @OpenApiResponse(status = STATUS_404, description = "No data matching input parameters found")
        }
    )
    @Override
    public void handle(@NotNull Context ctx) {
        try (final Timer.Context ignored = markAndTime(GET_ALL)) {
            DSLContext dsl = getDslContext(ctx);
            TimeSeriesProfileDao tspDao = getProfileDao(dsl);
            String officeMask = ctx.queryParamAsClass(OFFICE_MASK, String.class).getOrDefault("*");
            String locationMask = ctx.queryParamAsClass(LOCATION_MASK, String.class).getOrDefault("*");
            String parameterIdMask = ctx.queryParamAsClass(PARAMETER_ID_MASK, String.class).getOrDefault("*");
            String cursor = ctx.queryParam(PAGE);
            int pageSize = ctx.queryParamAsClass(PAGE_SIZE, Integer.class).getOrDefault(500);
            TimeSeriesProfileList retrievedProfiles = tspDao.catalogTimeSeriesProfiles(locationMask,
                    parameterIdMask, officeMask, cursor, pageSize);
            String acceptHeader = ctx.header(Header.ACCEPT);
            ContentType contentType = Formats.parseHeader(acceptHeader, TimeSeriesProfileList.class);
            String results = Formats.format(contentType, retrievedProfiles);
            ctx.status(HttpServletResponse.SC_OK);
            ctx.result(results);
        }
    }
}
