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
import static cwms.cda.api.Controllers.DATE;
import static cwms.cda.api.Controllers.DELETE;
import static cwms.cda.api.Controllers.END;
import static cwms.cda.api.Controllers.GET_ALL;
import static cwms.cda.api.Controllers.GET_ONE;
import static cwms.cda.api.Controllers.LOCATION_MASK;
import static cwms.cda.api.Controllers.MAX_VERSION;
import static cwms.cda.api.Controllers.METHOD;
import static cwms.cda.api.Controllers.OFFICE;
import static cwms.cda.api.Controllers.OFFICE_MASK;
import static cwms.cda.api.Controllers.OVERRIDE_PROTECTION;
import static cwms.cda.api.Controllers.PAGE;
import static cwms.cda.api.Controllers.PAGE_SIZE;
import static cwms.cda.api.Controllers.START;
import static cwms.cda.api.Controllers.STATUS_200;
import static cwms.cda.api.Controllers.TIMESERIES_ID;
import static cwms.cda.api.Controllers.TIMEZONE;
import static cwms.cda.api.Controllers.UNIT;
import static cwms.cda.api.Controllers.VERSION;
import static cwms.cda.api.Controllers.VERSION_DATE;
import static cwms.cda.api.Controllers.requiredParam;
import static cwms.cda.data.dao.JooqDao.getDslContext;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.cda.api.Controllers;
import cwms.cda.api.errors.RequiredQueryParameterException;
import cwms.cda.data.dao.StoreRule;
import cwms.cda.data.dao.timeseriesprofile.TimeSeriesProfileInstanceDao;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.timeseriesprofile.TimeSeriesProfile;
import cwms.cda.data.dto.timeseriesprofile.TimeSeriesProfileInstance;
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
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import javax.servlet.http.HttpServletResponse;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;
import usace.cwms.db.dao.util.OracleTypeMap;


public final class TimeSeriesProfileInstanceController implements CrudHandler {
    public static final String PROFILE_DATA = "profile-data";
    public static final String PARAMETER_ID_MASK = "parameter-id-mask";
    public static final String VERSION_MASK = "version-mask";
    public static final String PARAMETER_ID = "parameter-id";
    public static final String START_INCLUSIVE = "start-inclusive";
    public static final String END_INCLUSIVE = "end-inclusive";
    public static final String PREVIOUS = "previous";
    public static final String NEXT = "next";
    public static final String TAG = "TimeSeries";
    private final MetricRegistry metrics;


    public TimeSeriesProfileInstanceController(MetricRegistry metrics) {
        this.metrics = metrics;
    }

    private Timer.Context markAndTime(String subject) {
        return Controllers.markAndTime(metrics, getClass().getName(), subject);
    }

    @OpenApi(
            queryParams = {
                @OpenApiParam(name = METHOD, type = StoreRule.class, description = "The method of storing the"
                        + " time series profile instance. Default is REPLACE_ALL"),
                @OpenApiParam(name = OVERRIDE_PROTECTION, type = Boolean.class, description = "Override protection"
                    + " for the time series profile instance. Default is false"),
                @OpenApiParam(name = VERSION_DATE, type = Long.class, description = "The version date of the"
                    + " time series profile instance. Default is the current date and time"),
                @OpenApiParam(name = PROFILE_DATA, required = true, description = "The profile data of the"
                    + " time series profile instance"),
                @OpenApiParam(name = VERSION, description = "The version of the"
                    + " time series profile instance.", required = true),
            },
            path = "/timeseries/instance",
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
    public void create(@NotNull Context ctx) {
        try (final Timer.Context ignored = markAndTime(CREATE)) {
            DSLContext dsl = getDslContext(ctx);
            TimeSeriesProfileInstanceDao tspInstanceDao = new TimeSeriesProfileInstanceDao(dsl);
            TimeSeriesProfile timeSeriesProfile = Formats.parseContent(Formats.parseHeader(Formats.JSONV1,
                    TimeSeriesProfile.class), ctx.body(), TimeSeriesProfile.class);
            String profileData = requiredParam(ctx, PROFILE_DATA);
            String versionId = requiredParam(ctx, VERSION);
            Instant versionDate = Instant.ofEpochMilli(ctx.queryParamAsClass(VERSION_DATE, Long.class)
                    .getOrDefault(Instant.now().toEpochMilli()));
            StoreRule storeRule = ctx.queryParamAsClass(METHOD, StoreRule.class).getOrDefault(StoreRule.REPLACE_ALL);
            boolean overrideProtection = ctx.queryParamAsClass(OVERRIDE_PROTECTION, boolean.class)
                    .getOrDefault(false);
            tspInstanceDao.storeTimeSeriesProfileInstance(timeSeriesProfile, profileData, versionDate,
                    versionId, storeRule.toString(), overrideProtection);
            ctx.status(HttpServletResponse.SC_CREATED);
        }
    }

    @OpenApi(
            queryParams = {
                @OpenApiParam(name = OFFICE, description = "The office associated with the"
                        + " time series profile instance.", required = true),
                @OpenApiParam(name = VERSION, description = "The version of the"
                        + " time series profile instance.", required = true),
                @OpenApiParam(name = TIMEZONE, description = "The timezone of the"
                        + " time series profile instance. Default is UTC"),
                @OpenApiParam(name = VERSION_DATE, type = Instant.class, description = "The version date of the"
                        + " time series profile instance. Default is current time"),
                @OpenApiParam(name = DATE, type = Instant.class, description = "The first date of the"
                    + " time series profile instance. Default is current time"),
                @OpenApiParam(name = PARAMETER_ID, description = "The key parameter of the"
                    + " time series profile instance.", required = true),
                @OpenApiParam(name = OVERRIDE_PROTECTION, type = Boolean.class, description = "Override protection"
                    + " for the time series profile instance. Default is true")
            },
            pathParams = {
                @OpenApiParam(name = TIMESERIES_ID, description = "The time series ID of the"
                        + " time series profile instance. Default is null")
            },
            path = "/timeseries/instance/{timeseries-id}",
            method = HttpMethod.DELETE,
            summary = "Get all time series profile instances",
            tags = {TAG},
            responses = {
                @OpenApiResponse(status = "400", description = "Invalid input")
            }
    )
    @Override
    public void delete(@NotNull Context ctx, @NotNull String timeseriesId) {
        try (final Timer.Context ignored = markAndTime(DELETE)) {
            DSLContext dsl = getDslContext(ctx);
            TimeSeriesProfileInstanceDao tspInstanceDao = new TimeSeriesProfileInstanceDao(dsl);
            String office = requiredParam(ctx, OFFICE);
            String timeZone = ctx.queryParamAsClass(TIMEZONE, String.class).getOrDefault("UTC");
            String version = ctx.queryParam(VERSION);
            String keyParameter = requiredParam(ctx, PARAMETER_ID);
            Instant versionDate = Instant.ofEpochMilli(ctx.queryParamAsClass(VERSION_DATE, Long.class)
                    .getOrDefault(Instant.now().toEpochMilli()));
            Instant firstDate = Instant.ofEpochMilli(ctx.queryParamAsClass(DATE, Long.class)
                    .getOrDefault(Instant.now().toEpochMilli()));
            CwmsId tspIdentifier = new CwmsId.Builder().withName(timeseriesId).withOfficeId(office).build();
            boolean overrideProtection = ctx.queryParamAsClass(OVERRIDE_PROTECTION, boolean.class).getOrDefault(true);
            tspInstanceDao.deleteTimeSeriesProfileInstance(tspIdentifier, keyParameter, version, firstDate, timeZone,
                    overrideProtection, versionDate);
            ctx.status(HttpServletResponse.SC_NO_CONTENT);
        }
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
            path = "/timeseries/instance",
            method = HttpMethod.GET,
            summary = "Get all time series profile instances",
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
    public void getAll(@NotNull Context ctx) {
        try (final Timer.Context ignored = markAndTime(GET_ALL)) {
            DSLContext dsl = getDslContext(ctx);
            TimeSeriesProfileInstanceDao tspInstanceDao = new TimeSeriesProfileInstanceDao(dsl);
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

    @OpenApi(
        queryParams = {
            @OpenApiParam(name = OFFICE, description = "The office associated with the"
                    + " time series profile instance.", required = true),
            @OpenApiParam(name = PARAMETER_ID, description = "The key parameter of the"
                    + " time series profile instance.", required = true),
            @OpenApiParam(name = VERSION, description = "The version of the"
                    + " time series profile instance.", required = true),
            @OpenApiParam(name = TIMEZONE, description = "The timezone of the"
                    + " time series profile instance. Default is UTC"),
            @OpenApiParam(name = VERSION_DATE, type = Instant.class, description = "The version date of the"
                + " time series profile instance. Default is current time"),
            @OpenApiParam(name = UNIT, description = "The units of the"
                + " time series profile instance. Provided as a list separated by ','", required = true, type = List.class),
            @OpenApiParam(name = START_INCLUSIVE, type = Boolean.class, description = "The start inclusive of the"
                + " time series profile instance. Default is true"),
            @OpenApiParam(name = END_INCLUSIVE, type = Boolean.class, description = "The end inclusive of the"
                + " time series profile instance. Default is true"),
            @OpenApiParam(name = PREVIOUS, type = boolean.class, description = "The previous of the"
                + " time series profile instance. Default is false"),
            @OpenApiParam(name = NEXT, type = boolean.class, description = "The next of the"
                + " time series profile instance. Default is false"),
            @OpenApiParam(name = MAX_VERSION, type = boolean.class, description = "Whether the provided version"
                + " is the max version of the time series profile instance. Default is false"),
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
            @OpenApiParam(name = TIMESERIES_ID, description = "The time series ID of the"
                    + " time series profile instance.", required = true)
        },
        path = "/timeseries/instance/{timeseries-id}",
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
    public void getOne(@NotNull Context ctx, @NotNull String timeSeriesId) {
        try (final Timer.Context ignored = markAndTime(GET_ONE)) {
            DSLContext dsl = getDslContext(ctx);
            TimeSeriesProfileInstanceDao tspInstanceDao = new TimeSeriesProfileInstanceDao(dsl);
            String officeId = requiredParam(ctx, OFFICE);
            String keyParameter = requiredParam(ctx, PARAMETER_ID);
            String version = requiredParam(ctx, VERSION);
            List<String> unit = ctx.queryParam(UNIT) != null ? Arrays.asList(ctx.queryParam(UNIT).split(",")) : null;
            if (unit.isEmpty()) {
                throw new RequiredQueryParameterException(UNIT);
            }
            Instant startTime = Instant.ofEpochMilli(Long.parseLong(ctx.queryParamAsClass(START, String.class)
                    .getOrDefault(String.valueOf(Instant.now().toEpochMilli()))));
            Instant endTime = Instant.ofEpochMilli(Long.parseLong(ctx.queryParamAsClass(END, String.class)
                    .getOrDefault(String.valueOf(Instant.now().toEpochMilli()))));
            String timeZone = ctx.queryParamAsClass(TIMEZONE, String.class).getOrDefault("UTC");
            String startInclusive = OracleTypeMap.formatBool(ctx.queryParamAsClass(START_INCLUSIVE, boolean.class)
                    .getOrDefault(true));
            String endInclusive = OracleTypeMap.formatBool(ctx.queryParamAsClass(END_INCLUSIVE, boolean.class)
                    .getOrDefault(true));
            String previous = OracleTypeMap.formatBool(ctx.queryParamAsClass(PREVIOUS, boolean.class)
                    .getOrDefault(false));
            String next = OracleTypeMap.formatBool(ctx.queryParamAsClass(NEXT, boolean.class).getOrDefault(false));
            Instant versionDate = Instant.ofEpochMilli(Long.parseLong(ctx.queryParamAsClass(VERSION_DATE, String.class)
                    .getOrDefault(String.valueOf(Instant.now().toEpochMilli()))));
            String maxVersion = OracleTypeMap.formatBool(ctx.queryParamAsClass(MAX_VERSION, boolean.class)
                    .getOrDefault(false));
            String page = ctx.queryParam(PAGE);
            int pageSize = ctx.queryParamAsClass(PAGE_SIZE, Integer.class).getOrDefault(500);
            CwmsId tspIdentifier = new CwmsId.Builder().withOfficeId(officeId).withName(timeSeriesId).build();
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

    @OpenApi(ignore = true)
    @Override
    public void update(@NotNull Context ctx, @NotNull String id) {
        ctx.status(HttpServletResponse.SC_NOT_IMPLEMENTED);
    }
}
