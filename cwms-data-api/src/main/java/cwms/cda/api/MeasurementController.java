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
import static cwms.cda.api.Controllers.CREATE;
import static cwms.cda.api.Controllers.DELETE;
import static cwms.cda.api.Controllers.FAIL_IF_EXISTS;
import static cwms.cda.api.Controllers.GET_ALL;
import static cwms.cda.api.Controllers.GET_ONE;
import static cwms.cda.api.Controllers.ID_MASK;
import static cwms.cda.api.Controllers.MAX_DATE;
import static cwms.cda.api.Controllers.MIN_DATE;
import static cwms.cda.api.Controllers.NOT_SUPPORTED_YET;
import static cwms.cda.api.Controllers.MIN_NUMBER;
import static cwms.cda.api.Controllers.MAX_NUMBER;
import static cwms.cda.api.Controllers.OFFICE;
import static cwms.cda.api.Controllers.OFFICE_MASK;
import static cwms.cda.api.Controllers.UNIT_SYSTEM;
import static cwms.cda.api.Controllers.requiredParam;
import cwms.cda.api.enums.UnitSystem;
import cwms.cda.data.dao.MeasurementDao;
import cwms.cda.data.dto.measurement.Measurement;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.FormattingException;
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
import java.util.ArrayList;
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
                    @OpenApiParam(name = MIN_DATE, description = "Minimum date for filtering measurements in ISO-8601 format."),
                    @OpenApiParam(name = MAX_DATE, description = "Maximum date for filtering measurements in ISO-8601 format."),
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
        Instant minDate = parseInstant(ctx.queryParam(MIN_DATE));
        Instant maxDate = parseInstant(ctx.queryParam(MAX_DATE));
        String minNum = ctx.queryParam(MIN_NUMBER);
        String maxNum = ctx.queryParam(MAX_NUMBER);
        Number minHeight = null;
        Number maxHeight = null;
        Number minFlow = null;
        Number maxFlow = null;
        String agencies = null;
        String qualities = null;
        try (Timer.Context ignored = markAndTime(GET_ALL)) {
            DSLContext dsl = getDslContext(ctx);
            MeasurementDao dao = new MeasurementDao(dsl);
            List<Measurement> measurements = dao.retrieveMeasurements(officeId, locationId, minDate, maxDate, unitSystem,
                    minHeight, maxHeight, minFlow, maxFlow, minNum, maxNum, agencies, qualities);
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
                            @OpenApiContent(from = Measurement.class, type = Formats.JSON)
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
            List<Measurement> measurements = parseMeasurements(ctx, contentType);
            boolean failIfExists = ctx.queryParamAsClass(FAIL_IF_EXISTS, Boolean.class).getOrDefault(true);
            DSLContext dsl = getDslContext(ctx);
            MeasurementDao dao = new MeasurementDao(dsl);
            if(measurements.size() == 1) {
                dao.storeMeasurement(measurements.get(0), failIfExists);
                ctx.status(HttpServletResponse.SC_CREATED).json("Created Measurement");
            } else {
                dao.storeMeasurements(measurements, failIfExists);
                ctx.status(HttpServletResponse.SC_CREATED).json("Created Measurements");
            }
        }
    }

    @OpenApi(ignore = true)
    @Override
    public void update(@NotNull Context ctx, @NotNull String locationId) {
        try (final Timer.Context ignored = markAndTime(GET_ONE)) {
            throw new UnsupportedOperationException("Not supported with required location Id");
        }
    }

    @OpenApi(
            queryParams = {
                    @OpenApiParam(name = OFFICE, required = true, description = "Specifies the office of the measurements to delete"),
                    @OpenApiParam(name = MIN_NUMBER, description = "Specifies the min number-id of the measurement to delete."),
                    @OpenApiParam(name = MAX_NUMBER, description = "Specifies the max number-id of the measurement to delete."),
                    @OpenApiParam(name = MIN_DATE, description = "Specifies the minimum date (in ISO-8601 format) of the measurement to delete."),
                    @OpenApiParam(name = MAX_DATE, description = "Specifies the maximum date (in ISO-8601 format) of the measurement to delete."),
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
        Instant minDate = parseInstant(ctx.queryParam(MIN_DATE));
        Instant maxDate = parseInstant(ctx.queryParam(MAX_DATE));
        try (Timer.Context ignored = markAndTime(DELETE)) {
            DSLContext dsl = getDslContext(ctx);
            MeasurementDao dao = new MeasurementDao(dsl);
            dao.deleteMeasurements(officeId, locationId, minDate, maxDate, UnitSystem.EN.getValue(), null,
                    null, null, null, minNum, maxNum, null, null);
            ctx.status(HttpServletResponse.SC_NO_CONTENT).json( "Measurements for " + locationId + " Deleted");
        }
    }

    private Instant parseInstant(String date) {
        Instant retVal = null;
        if(date != null && !date.isEmpty()) {
            retVal = Instant.parse(date);
        }
        return retVal;
    }

    static List<Measurement> parseMeasurements(@NotNull Context ctx, ContentType contentType) {
        List<Measurement> measurements;
        try {
            measurements = Formats.parseContentList(contentType, ctx.body(), Measurement.class);
        } catch (FormattingException e) {
            Measurement measurement = Formats.parseContent(contentType, ctx.body(), Measurement.class);
            measurements = new ArrayList<>();
            measurements.add(measurement);
        }
        return measurements;
    }
}
