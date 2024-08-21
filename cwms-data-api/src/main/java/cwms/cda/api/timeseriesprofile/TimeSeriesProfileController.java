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
import static cwms.cda.api.Controllers.STATUS_204;
import static cwms.cda.api.Controllers.STATUS_400;
import static cwms.cda.api.Controllers.STATUS_404;
import static cwms.cda.api.Controllers.STATUS_501;
import static cwms.cda.api.Controllers.requiredParam;
import static cwms.cda.data.dao.JooqDao.getDslContext;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.cda.api.Controllers;
import cwms.cda.api.errors.NotFoundException;
import cwms.cda.data.dao.timeseriesprofile.TimeSeriesProfileDao;
import cwms.cda.data.dto.timeseriesprofile.TimeSeriesProfile;
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




public final class TimeSeriesProfileController implements CrudHandler {
    public static final String PARAMETER_ID_MASK = "parameter-id-mask";
    public static final String PARAMETER_ID = "parameter-id";
    public static final String TAG = "TimeSeries";
    private final MetricRegistry metrics;

    public TimeSeriesProfileController(MetricRegistry metrics) {
        this.metrics = metrics;
    }

    private Timer.Context markAndTime(String subject) {
        return Controllers.markAndTime(metrics, getClass().getName(), subject);
    }

    @OpenApi(
        queryParams = {
            @OpenApiParam(name = FAIL_IF_EXISTS, type = boolean.class, description = "If true, the parser will "
                    + "fail to save if the TimeSeriesProfile already exists. Default true."),
        },
        path = "/timeseries/profile",
        method = HttpMethod.POST,
        summary = "Create a new time series profile",
        tags = {TAG},
        requestBody = @OpenApiRequestBody(content = {@OpenApiContent(from = TimeSeriesProfile.class)}),
        responses = {
            @OpenApiResponse(status = "400", description = "Invalid input")
        }
    )
    @Override
    public void create(@NotNull Context ctx) {
        try (final Timer.Context ignored = markAndTime(CREATE)) {
            DSLContext dsl = getDslContext(ctx);
            TimeSeriesProfile timeSeriesProfile = Formats.parseContent(Formats.parseHeader(Formats.JSONV2,
                    TimeSeriesProfile.class), ctx.body(), TimeSeriesProfile.class);
            boolean failIfExists = ctx.queryParamAsClass(FAIL_IF_EXISTS, boolean.class).getOrDefault(true);
            TimeSeriesProfileDao tspDao = new TimeSeriesProfileDao(dsl);
            try {
                tspDao.storeTimeSeriesProfile(timeSeriesProfile, failIfExists);
            } catch (NotFoundException e) {
                ctx.status(HttpServletResponse.SC_NOT_FOUND);
                return;
            }
            ctx.status(HttpServletResponse.SC_CREATED);
        }
    }

    @OpenApi(
        queryParams = {
            @OpenApiParam(name = OFFICE, description = "The office associated with the time series profile"),
            @OpenApiParam(name = LOCATION_ID, description = "The location ID associated with the time series profile"),
        },
        pathParams = {
            @OpenApiParam(name = PARAMETER_ID, description = "The key parameter associated with the "
                    + "time series profile")
        },
        path = "/timeseries/profile/{parameter-id}",
        method = HttpMethod.DELETE,
        summary = "Update a time series profile",
        tags = {TAG},
        requestBody = @OpenApiRequestBody(content = {@OpenApiContent(from = TimeSeriesProfile.class)}),
        responses = {
            @OpenApiResponse(status = STATUS_400, description = "Invalid input"),
            @OpenApiResponse(status = STATUS_204, description = "Time series profile deleted"),
            @OpenApiResponse(status = STATUS_404, description = "Time series profile not found"),
            @OpenApiResponse(status = STATUS_501, description = "Internal server error")
        }
    )
    @Override
    public void delete(@NotNull Context ctx, @NotNull String keyParameter) {
        try (final Timer.Context ignored = markAndTime(DELETE)) {
            DSLContext dsl = getDslContext(ctx);
            String office = requiredParam(ctx, OFFICE);
            String locationId = requiredParam(ctx, LOCATION_ID);
            TimeSeriesProfileDao tspDao = new TimeSeriesProfileDao(dsl);
            tspDao.deleteTimeSeriesProfile(locationId, keyParameter, office);
            ctx.status(HttpServletResponse.SC_NO_CONTENT);
        }
    }

    @OpenApi(
        queryParams = {
            @OpenApiParam(name = OFFICE_MASK, description = "The office mask for the time series profile"),
            @OpenApiParam(name = LOCATION_MASK, description = "The location mask for the time series profile"),
            @OpenApiParam(name = PARAMETER_ID_MASK, description = "The key parameter mask for the time series profile")
        },
        path = "/timeseries/profile",
        method = HttpMethod.GET,
        summary = "Get a time series profile",
        tags = {TAG},
        responses = {
            @OpenApiResponse(status = STATUS_400, description = "Invalid input"),
            @OpenApiResponse(status = STATUS_404, description = "No data matching input parameters found")
        }
    )
    @Override
    public void getAll(@NotNull Context ctx) {
        try (final Timer.Context ignored = markAndTime(GET_ALL)) {
            DSLContext dsl = getDslContext(ctx);
            TimeSeriesProfileDao tspDao = new TimeSeriesProfileDao(dsl);
            String officeMask = ctx.queryParam(OFFICE_MASK);
            String locationMask = ctx.queryParam(LOCATION_MASK);
            String parameterIdMask = ctx.queryParam(PARAMETER_ID_MASK);
            List<TimeSeriesProfile> retrievedProfiles = tspDao.catalogTimeSeriesProfiles(locationMask,
                    parameterIdMask, officeMask);
            String acceptHeader = ctx.header(Header.ACCEPT);
            ContentType contentType = Formats.parseHeader(acceptHeader, TimeSeriesProfile.class);
            String results = Formats.format(contentType, retrievedProfiles, TimeSeriesProfile.class);
            ctx.status(HttpServletResponse.SC_OK);
            ctx.result(results);
        }
    }

    @OpenApi(
        queryParams = {
            @OpenApiParam(name = OFFICE, description = "The office ID associated with the time series profile"),
            @OpenApiParam(name = LOCATION_ID, description = "The location ID associated with the "
                    + "time series profile")
        },
        pathParams = {
            @OpenApiParam(name = PARAMETER_ID, description = "The key parameter ID associated with the time "
                    + "series profile")
        },
        path = "/timeseries/profile/{parameter-id}",
        method = HttpMethod.GET,
        summary = "Get a time series profile",
        tags = {TAG},
        responses = {
            @OpenApiResponse(status = "400", description = "Invalid input")
        }
    )
    @Override
    public void getOne(@NotNull Context ctx, @NotNull String parameterId) {
        try (final Timer.Context ignored = markAndTime(GET_ONE)) {
            DSLContext dsl = getDslContext(ctx);
            TimeSeriesProfileDao tspDao = new TimeSeriesProfileDao(dsl);
            String office = requiredParam(ctx, OFFICE);
            String locationId = requiredParam(ctx, LOCATION_ID);
            TimeSeriesProfile returned = tspDao.retrieveTimeSeriesProfile(locationId, parameterId, office);
            String acceptHeader = ctx.header(Header.ACCEPT);
            ContentType contentType = Formats.parseHeader(acceptHeader, TimeSeriesProfile.class);
            String result = Formats.format(contentType, returned);
            ctx.status(HttpServletResponse.SC_OK);
            ctx.result(result);
        }
    }

    @OpenApi(ignore = true)
    @Override
    public void update(@NotNull Context ctx, @NotNull String id) {
        ctx.status(HttpServletResponse.SC_NOT_IMPLEMENTED);
    }
}
