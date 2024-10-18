/*
 * MIT License
 *
 * Copyright (c) 2024 Hydrologic Engineering Center
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
import static cwms.cda.api.Controllers.AGENCY;
import static cwms.cda.api.Controllers.BEGIN;
import static cwms.cda.api.Controllers.CREATE;
import static cwms.cda.api.Controllers.DATE_FORMAT;
import static cwms.cda.api.Controllers.DELETE;
import static cwms.cda.api.Controllers.EXAMPLE_DATE;
import static cwms.cda.api.Controllers.FAIL_IF_EXISTS;
import static cwms.cda.api.Controllers.GET_ALL;
import static cwms.cda.api.Controllers.GET_ONE;
import static cwms.cda.api.Controllers.ID_MASK;
import static cwms.cda.api.Controllers.LOCATION_ID;
import static cwms.cda.api.Controllers.END;
import static cwms.cda.api.Controllers.MAX_FLOW;
import static cwms.cda.api.Controllers.MAX_HEIGHT;
import static cwms.cda.api.Controllers.MIN_FLOW;
import static cwms.cda.api.Controllers.MIN_HEIGHT;
import static cwms.cda.api.Controllers.NOT_SUPPORTED_YET;
import static cwms.cda.api.Controllers.MIN_NUMBER;
import static cwms.cda.api.Controllers.MAX_NUMBER;
import static cwms.cda.api.Controllers.OFFICE;
import static cwms.cda.api.Controllers.OFFICE_MASK;
import static cwms.cda.api.Controllers.QUALITY;
import static cwms.cda.api.Controllers.TIMEZONE;
import static cwms.cda.api.Controllers.UNIT_SYSTEM;
import static cwms.cda.api.Controllers.queryParamAsDouble;
import static cwms.cda.api.Controllers.queryParamAsInstant;
import static cwms.cda.api.Controllers.requiredParam;
import cwms.cda.api.enums.UnitSystem;
import cwms.cda.data.dao.MeasurementDao;
import cwms.cda.data.dto.measurement.Measurement;
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
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;

import javax.servlet.http.HttpServletResponse;
import java.util.List;

import static cwms.cda.data.dao.JooqDao.getDslContext;

public final class MeasurementController implements CrudHandler {

    static final String TAG = "Measurements";

    private final MetricRegistry metrics;
    private final Histogram requestResultSize;

    public MeasurementController(MetricRegistry metrics) {
        this.metrics = metrics;
        String className = this.getClass().getName();
        requestResultSize = this.metrics.histogram(name(className, "results", "size"));
    }

    private Timer.Context markAndTime(String subject) {
        return Controllers.markAndTime(metrics, getClass().getName(), subject);
    }

    @OpenApi(
            queryParams = {
                    @OpenApiParam(name = OFFICE_MASK, description = "Office id mask for filtering measurements. Use null to retrieve measurements for all offices."),
                    @OpenApiParam(name = ID_MASK, description = "Location id mask for filtering measurements. Use null to retrieve measurements for all locations."),
                    @OpenApiParam(name = MIN_NUMBER, description = "Minimum measurement number-id for filtering measurements."),
                    @OpenApiParam(name = MAX_NUMBER, description = "Maximum measurement number-id for filtering measurements."),
                    @OpenApiParam(name = BEGIN, description = "The start of the time "
                            + "window to delete. The format for this field is ISO 8601 extended, with "
                            + "optional offset and timezone, i.e., '" + DATE_FORMAT + "', e.g., '"
                            + EXAMPLE_DATE + "'. A null value is treated as an unbounded start."),
                    @OpenApiParam(name = END, description = "The end of the time "
                            + "window to delete.The format for this field is ISO 8601 extended, with "
                            + "optional offset and timezone, i.e., '" + DATE_FORMAT + "', e.g., '"
                            + EXAMPLE_DATE + "'.A null value is treated as an unbounded end."),
                    @OpenApiParam(name = TIMEZONE, description = "This field specifies a default timezone "
                            + "to be used if the format of the " + BEGIN + "and " + END
                            + " parameters do not include offset or time zone information. "
                            + "Defaults to UTC."),
                    @OpenApiParam(name = MIN_HEIGHT, description = "Minimum height for filtering measurements."),
                    @OpenApiParam(name = MAX_HEIGHT, description = "Maximum height for filtering measurements."),
                    @OpenApiParam(name = MIN_FLOW, description = "Minimum flow for filtering measurements."),
                    @OpenApiParam(name = MAX_FLOW, description = "Maximum flow for filtering measurements."),
                    @OpenApiParam(name = AGENCY, description = "Agencies for filtering measurements."),
                    @OpenApiParam(name = QUALITY, description = "Quality for filtering measurements."),
                    @OpenApiParam(name = UNIT_SYSTEM, description = "Specifies the unit system"
                            + " of the response. Valid values for the unit field are: "
                            + "\n* `EN`  Specifies English unit system.  Location values will be in the "
                            + "default English units for their parameters."
                            + "\n* `SI`  Specifies the SI unit system.  Location values will be in the "
                            + "default SI units for their parameters. If not specified, EN is used.")
            },
            responses = {
                    @OpenApiResponse(status = "200", content = {
                            @OpenApiContent(isArray = true, type = Formats.JSONV1, from = Measurement.class),
                            @OpenApiContent(isArray = true, type = Formats.JSON, from = Measurement.class)
                    })
            },
            description = "Returns matching measurement data.",
            tags = {TAG}
    )
    @Override
    public void getAll(@NotNull Context ctx) {
        String officeId = ctx.queryParam(OFFICE_MASK);
        String locationId = ctx.queryParam(ID_MASK);
        String unitSystem = ctx.queryParamAsClass(UNIT_SYSTEM, String.class).getOrDefault(UnitSystem.EN.value());
        Instant minDate = queryParamAsInstant(ctx, BEGIN);
        Instant maxDate = queryParamAsInstant(ctx, END);
        String minNum = ctx.queryParam(MIN_NUMBER);
        String maxNum = ctx.queryParam(MAX_NUMBER);
        Number minHeight = queryParamAsDouble(ctx, MIN_HEIGHT);
        Number maxHeight = queryParamAsDouble(ctx, MAX_HEIGHT);
        Number minFlow = queryParamAsDouble(ctx, MIN_FLOW);
        Number maxFlow = queryParamAsDouble(ctx, MAX_FLOW);
        String agency = ctx.queryParam(AGENCY);
        String quality = ctx.queryParam(QUALITY);
        try (Timer.Context ignored = markAndTime(GET_ALL)) {
            DSLContext dsl = getDslContext(ctx);
            MeasurementDao dao = new MeasurementDao(dsl);
            List<Measurement> measurements = dao.retrieveMeasurements(officeId, locationId, minDate, maxDate, unitSystem,
                    minHeight, maxHeight, minFlow, maxFlow, minNum, maxNum, agency, quality);
            String formatHeader = ctx.header(Header.ACCEPT);
            ContentType contentType = Formats.parseHeader(formatHeader, Measurement.class);
            ctx.contentType(contentType.toString());
            String serialized = Formats.format(contentType, measurements, Measurement.class);
            ctx.result(serialized);
            ctx.status(HttpServletResponse.SC_OK);
            requestResultSize.update(serialized.length());
        }
    }

    @OpenApi(ignore = true)
    @Override
    public void getOne(@NotNull Context ctx, @NotNull String locationId) {
        try (final Timer.Context ignored = markAndTime(GET_ONE)) {
            throw new UnsupportedOperationException(NOT_SUPPORTED_YET);
        }

    }

    @OpenApi(
            requestBody = @OpenApiRequestBody(
                    content = {
                            @OpenApiContent(isArray = true, from = Measurement.class, type = Formats.JSONV1),
                            @OpenApiContent(isArray = true, from = Measurement.class, type = Formats.JSON)
                    },
                    required = true),
            queryParams = {
                    @OpenApiParam(name = FAIL_IF_EXISTS, type = Boolean.class,
                            description = "Create will fail if provided Measurement(s) already exist. Default: true")
            },
            description = "Create new measurement(s).",
            method = HttpMethod.POST,
            tags = {TAG},
            responses = {
                    @OpenApiResponse(status = "204", description = "Measurement(s) successfully stored.")
            }
    )
    @Override
    public void create(Context ctx) {

        try (Timer.Context ignored = markAndTime(CREATE)) {
            String formatHeader = ctx.req.getContentType();
            ContentType contentType = Formats.parseHeader(formatHeader, Measurement.class);
            List<Measurement> measurements = Formats.parseContentList(contentType, ctx.body(), Measurement.class);
            boolean failIfExists = ctx.queryParamAsClass(FAIL_IF_EXISTS, Boolean.class).getOrDefault(true);
            DSLContext dsl = getDslContext(ctx);
            MeasurementDao dao = new MeasurementDao(dsl);
            dao.storeMeasurements(measurements, failIfExists);
            String statusMsg = "Created Measurement";
            if(measurements.size() > 1)
            {
                statusMsg += "s";
            }
            ctx.status(HttpServletResponse.SC_CREATED).json(statusMsg);
        }
    }

    @OpenApi(ignore = true)
    @Override
    public void update(@NotNull Context ctx, @NotNull String locationId) {
        try (final Timer.Context ignored = markAndTime(GET_ONE)) {
            throw new UnsupportedOperationException(NOT_SUPPORTED_YET);
        }
    }

    @OpenApi(
            pathParams = {
                    @OpenApiParam(name = LOCATION_ID, description = "Specifies the location-id of "
                            + "the measurement(s) to be deleted."),
            },
            queryParams = {
                    @OpenApiParam(name = OFFICE, required = true, description = "Specifies the office of the measurements to delete"),
                    @OpenApiParam(name = BEGIN, required = true, description = "The start of the time "
                            + "window to delete. The format for this field is ISO 8601 extended, with "
                            + "optional offset and timezone, i.e., '" + DATE_FORMAT + "', e.g., '"
                            + EXAMPLE_DATE + "'."),
                    @OpenApiParam(name = END, required = true, description = "The end of the time "
                            + "window to delete.The format for this field is ISO 8601 extended, with "
                            + "optional offset and timezone, i.e., '" + DATE_FORMAT + "', e.g., '"
                            + EXAMPLE_DATE + "'."),
                    @OpenApiParam(name = TIMEZONE, description = "This field specifies a default timezone "
                            + "to be used if the format of the " + BEGIN + "and " + END
                            + " parameters do not include offset or time zone information. "
                            + "Defaults to UTC."),
                    @OpenApiParam(name = MIN_NUMBER, description = "Specifies the min number-id of the measurement to delete."),
                    @OpenApiParam(name = MAX_NUMBER, description = "Specifies the max number-id of the measurement to delete."),
            },
            description = "Delete an existing measurement.",
            method = HttpMethod.DELETE,
            tags = {TAG},
            responses = {
                    @OpenApiResponse(status = "204", description = "Measurement successfully deleted."),
                    @OpenApiResponse(status = "404", description = "Measurement not found.")
            }
    )
    @Override
    public void delete(@NotNull Context ctx, @NotNull String locationId) {
        String officeId = requiredParam(ctx, OFFICE);
        String minNum = ctx.queryParam(MIN_NUMBER);
        String maxNum = ctx.queryParam(MAX_NUMBER);
        Instant minDate = queryParamAsInstant(ctx, BEGIN);
        Instant maxDate = queryParamAsInstant(ctx, END);
        try (Timer.Context ignored = markAndTime(DELETE)) {
            DSLContext dsl = getDslContext(ctx);
            MeasurementDao dao = new MeasurementDao(dsl);
            dao.deleteMeasurements(officeId, locationId, minDate, maxDate,minNum, maxNum);
            ctx.status(HttpServletResponse.SC_NO_CONTENT).json( "Measurements for " + locationId + " Deleted");
        }
    }

}
