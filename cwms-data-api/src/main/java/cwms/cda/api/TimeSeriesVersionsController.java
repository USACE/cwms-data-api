/*
 * MIT License
 *
 * Copyright (c) 2026 Hydrologic Engineering Center
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

import static com.codahale.metrics.MetricRegistry.name;
import static cwms.cda.api.Controllers.BEGIN;
import static cwms.cda.api.Controllers.END;
import static cwms.cda.api.Controllers.GET_ALL;
import static cwms.cda.api.Controllers.NAME;
import static cwms.cda.api.Controllers.OFFICE;
import static cwms.cda.api.Controllers.PAGE;
import static cwms.cda.api.Controllers.PAGE_SIZE;
import static cwms.cda.api.Controllers.RESULTS;
import static cwms.cda.api.Controllers.SIZE;
import static cwms.cda.api.Controllers.STATUS_200;
import static cwms.cda.api.Controllers.TIME_FORMAT_DESC;
import static cwms.cda.api.Controllers.queryParamAsClass;
import static cwms.cda.api.Controllers.queryParamAsInstant;
import static cwms.cda.api.Controllers.requiredParam;
import static cwms.cda.api.Controllers.validateTimeSeriesPageSize;
import static cwms.cda.api.TimeSeriesController.DEFAULT_PAGE_SIZE;
import static cwms.cda.data.dao.JooqDao.getDslContext;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.cda.data.dao.TimeSeriesDao;
import cwms.cda.data.dao.TimeSeriesDaoImpl;
import cwms.cda.data.dto.TimeSeriesVersions;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import io.javalin.core.util.Header;
import io.javalin.http.Context;
import io.javalin.http.Handler;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;

import java.time.Instant;
import javax.servlet.http.HttpServletResponse;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;

public final class TimeSeriesVersionsController implements Handler {
    private final MetricRegistry metrics;
    private final Histogram requestResultSize;
    private final String className = this.getClass().getName();

    public TimeSeriesVersionsController(MetricRegistry metrics) {
        this.metrics = metrics;
        requestResultSize = this.metrics.histogram(MetricRegistry.name(className, RESULTS, SIZE));
    }

    private Timer.Context markAndTime(String subject) {
        return Controllers.markAndTime(metrics, getClass().getName(), subject);
    }

    private TimeSeriesDao getTimeSeriesDao(DSLContext dsl) {
        return new TimeSeriesDaoImpl(dsl, metrics);
    }

    @OpenApi(
            description = "Returns TimeSeries versions and their extents for a given TimeSeries identifier. Aliases are supported for the TimeSeries identifier.",
            queryParams = {
                    @OpenApiParam(name = NAME, required = true, description = "Specifies the "
                            + "name of the time series whose data is to be included in the "
                            + "response. A case insensitive comparison is used to match names."),
                    @OpenApiParam(name = OFFICE,  required = true, description = "Specifies the"
                            + " owning office of the time series(s) whose data is to be included "
                            + "in the response."),
                    @OpenApiParam(name = BEGIN,  description = "Specifies the "
                            + "start of the time window for data to be included in the response. "
                            + TIME_FORMAT_DESC),
                    @OpenApiParam(name = END,  description = "Specifies the "
                            + "end of the time window for data to be included in the response. "
                            + TIME_FORMAT_DESC),
                    @OpenApiParam(name = PAGE, description = "This end point can return large amounts "
                            + "of data as a series of pages. This parameter is used to describes the "
                            + "current location in the response stream.  This is an opaque "
                            + "value, and can be obtained from the 'next-page' value in the response."),
                    @OpenApiParam(name = PAGE_SIZE, type = Integer.class, description = "How many entries per page returned. "
                            + "For paging, this controls page size. "
                            + "Default " + DEFAULT_PAGE_SIZE +". Use 0 to return an empty values array, "
                            + "or -1 to return the entire window in one response without a next-page cursor. "
                            + "Values less than -1 are invalid."),
            },
            responses = {
                    @OpenApiResponse(status = STATUS_200, content = {
                            @OpenApiContent(type = Formats.JSONV1, from = TimeSeriesVersions.class)
                    })
            },
            tags = {TimeSeriesController.TAG}
    )
    @Override
    public void handle(@NotNull Context ctx) throws Exception {
        try (Timer.Context ignored = markAndTime(GET_ALL)) {
            DSLContext dsl = getDslContext(ctx);
            TimeSeriesDao dao = getTimeSeriesDao(dsl);

            String tsId = requiredParam(ctx, NAME);
            String office = requiredParam(ctx, OFFICE);
            Instant begin = queryParamAsInstant(ctx, BEGIN);
            Instant end = queryParamAsInstant(ctx, END);
            String cursor = ctx.queryParam(PAGE);
            int pageSize = validateTimeSeriesPageSize(queryParamAsClass(ctx,
                    new String[]{PAGE_SIZE}, Integer.class, DEFAULT_PAGE_SIZE, metrics,
                    name(TimeSeriesVersionsController.class.getName(), GET_ALL)));
            TimeSeriesVersions versions = dao.getTimeSeriesVersions(cursor, pageSize, tsId, office, begin, end);

            String formatHeader = ctx.header(Header.ACCEPT);
            ContentType contentType = Formats.parseHeader(formatHeader, TimeSeriesVersions.class);
            ctx.contentType(contentType.toString());

            String serialized = Formats.format(contentType, versions);
            ctx.result(serialized);
            ctx.status(HttpServletResponse.SC_OK);
            requestResultSize.update(serialized.length());
        }
    }
}
