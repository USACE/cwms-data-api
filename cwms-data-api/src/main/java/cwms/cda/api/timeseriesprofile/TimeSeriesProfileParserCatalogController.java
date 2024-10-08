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
import cwms.cda.data.dao.timeseriesprofile.TimeSeriesProfileParserDao;
import cwms.cda.data.dto.timeseriesprofile.TimeSeriesProfileParser;
import cwms.cda.data.dto.timeseriesprofile.TimeSeriesProfileParserList;
import cwms.cda.data.dto.timeseriesprofile.TimeSeriesProfileParsers;
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


public final class TimeSeriesProfileParserCatalogController extends TimeSeriesProfileParserBase implements Handler {

    public TimeSeriesProfileParserCatalogController(MetricRegistry metrics) {
        tspMetrics(metrics);
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
            method = HttpMethod.GET,
            summary = "Get a TimeSeriesProfileParser by ID",
            tags = {TAG}
    )
    @Override
    public void handle(@NotNull Context ctx) {
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
}
