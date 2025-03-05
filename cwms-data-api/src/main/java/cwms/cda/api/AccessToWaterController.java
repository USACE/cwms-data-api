/*
 * MIT License
 *
 * Copyright (c) 2025 Hydrologic Engineering Center
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

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import static com.codahale.metrics.MetricRegistry.name;
import com.codahale.metrics.Timer;
import static cwms.cda.api.Controllers.CREATE;
import static cwms.cda.api.Controllers.CURSOR;
import static cwms.cda.api.Controllers.DELETE;
import static cwms.cda.api.Controllers.GET_ALL;
import static cwms.cda.api.Controllers.GET_ONE;
import static cwms.cda.api.Controllers.LOCATION_MASK;
import static cwms.cda.api.Controllers.NOT_SUPPORTED_YET;
import static cwms.cda.api.Controllers.OFFICE_MASK;
import static cwms.cda.api.Controllers.PAGE;
import static cwms.cda.api.Controllers.PAGE_SIZE;
import static cwms.cda.api.Controllers.RESULTS;
import static cwms.cda.api.Controllers.SIZE;
import static cwms.cda.api.Controllers.STATUS_200;
import static cwms.cda.api.Controllers.UPDATE;
import static cwms.cda.api.Controllers.queryParamAsClass;
import cwms.cda.api.errors.CdaError;
import cwms.cda.data.dao.AccessToWaterRetrievalParameters;
import static cwms.cda.data.dao.JooqDao.getDslContext;
import cwms.cda.data.dao.AccessToWaterTimeSeriesDao;
import cwms.cda.data.dto.CwmsDTOPaginated;
import cwms.cda.data.dto.TimeSeriesIdentifiersByTypeList;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import io.javalin.apibuilder.CrudHandler;
import io.javalin.core.util.Header;
import io.javalin.http.Context;
import io.javalin.http.HttpCode;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import javax.servlet.http.HttpServletResponse;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;

public final class AccessToWaterController implements CrudHandler {
    private static final String TAG = "AccessToWater";
    private final MetricRegistry metrics;
    private final Histogram requestResultSize;
    private static final int DEFAULT_PAGE_SIZE = 20;

    public AccessToWaterController(MetricRegistry metrics) {
        this.metrics = metrics;
        String className = this.getClass().getName();
        requestResultSize = this.metrics.histogram(name(className, RESULTS, SIZE));
    }

    private Timer.Context markAndTime(String subject) {
        return Controllers.markAndTime(metrics, getClass().getName(), subject);
    }

    @OpenApi(ignore = true)
    @Override
    public void create(@NotNull Context context) {
        try (final Timer.Context ignored = markAndTime(CREATE)) {
            throw new UnsupportedOperationException(NOT_SUPPORTED_YET);
        }
    }

    @OpenApi(ignore = true)
    @Override
    public void delete(@NotNull Context context, @NotNull String s) {
        try (final Timer.Context ignored = markAndTime(DELETE)) {
            throw new UnsupportedOperationException(NOT_SUPPORTED_YET);
        }
    }

    @OpenApi(
            queryParams = {
                    @OpenApiParam(name = OFFICE_MASK, description = "Office Id used to filter the results."),
                    @OpenApiParam(name = LOCATION_MASK, description = "Location Id used to filter the results."),
                    @OpenApiParam(name = PAGE,
                            description = "This end point can return a lot of data, this "
                                    + "identifies where in the request you are. This is an opaque "
                                    + "value, and can be obtained from the 'next-page' value in "
                                    + "the response."),
                    @OpenApiParam(name = PAGE_SIZE,
                            type = Integer.class,
                            description = "How many entries per page returned. Default "
                                    + DEFAULT_PAGE_SIZE + "."),
            },
            responses = {
                    @OpenApiResponse(status = STATUS_200, content = {
                            @OpenApiContent(isArray = true, type = Formats.JSONV1, from = TimeSeriesIdentifiersByTypeList.class)
                    })
            },
            description = "Returns matching time series identifiers for access to water.",
            tags = {TAG}
    )
    @Override
    public void getAll(@NotNull Context ctx) {
        String officeIdMask = ctx.queryParam(OFFICE_MASK);
        String locationIdMask = ctx.queryParam(LOCATION_MASK);
        String cursor = queryParamAsClass(ctx, new String[]{PAGE, CURSOR},
                String.class, "", metrics, name(AccessToWaterController.class.getName(), GET_ALL));
        if (!CwmsDTOPaginated.CURSOR_CHECK.invoke(cursor)) {
            ctx.json(new CdaError("cursor or page passed in but failed validation"))
                    .status(HttpCode.BAD_REQUEST);
            return;
        }
        int pageSize = queryParamAsClass(ctx, new String[]{PAGE_SIZE}, Integer.class, DEFAULT_PAGE_SIZE, metrics,
                name(AccessToWaterController.class.getName(), GET_ALL));

        try (Timer.Context ignored = markAndTime(GET_ALL)) {
            DSLContext dsl = getDslContext(ctx);
            AccessToWaterTimeSeriesDao dao = new AccessToWaterTimeSeriesDao(dsl);
            AccessToWaterRetrievalParameters retrievalParams = new AccessToWaterRetrievalParameters(officeIdMask, locationIdMask);
            TimeSeriesIdentifiersByTypeList result = dao.retrieveAccessToWaterTimeSeriesIds(retrievalParams, cursor, pageSize);

            String formatHeader = ctx.header(Header.ACCEPT);
            ContentType contentType = Formats.parseHeader(formatHeader, TimeSeriesIdentifiersByTypeList.class);
            ctx.contentType(contentType.toString());
            String serialized = Formats.format(contentType, result);
            ctx.result(serialized);
            ctx.status(HttpServletResponse.SC_OK);
            requestResultSize.update(serialized.length());
        }
    }

    @OpenApi(ignore = true)
    @Override
    public void getOne(@NotNull Context context, @NotNull String s) {
        try (final Timer.Context ignored = markAndTime(GET_ONE)) {
            throw new UnsupportedOperationException(NOT_SUPPORTED_YET);
        }
    }

    @OpenApi(ignore = true)
    @Override
    public void update(@NotNull Context context, @NotNull String s) {
        try (final Timer.Context ignored = markAndTime(UPDATE)) {
            throw new UnsupportedOperationException(NOT_SUPPORTED_YET);
        }
    }
}
