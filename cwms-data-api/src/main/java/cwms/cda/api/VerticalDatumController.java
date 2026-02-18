/*
 * MIT License
 *
 * Copyright (c) 2026 Hydrologic Engineering Center
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to do so, subject to the
 * following conditions:
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
import com.codahale.metrics.Timer;
import com.google.common.flogger.FluentLogger;
import static com.codahale.metrics.MetricRegistry.name;
import static cwms.cda.api.Controllers.CREATE;
import static cwms.cda.api.Controllers.DELETE;
import static cwms.cda.api.Controllers.GET_ONE;
import static cwms.cda.api.Controllers.LOCATION_ID;
import static cwms.cda.api.Controllers.OFFICE;
import static cwms.cda.api.Controllers.RESULTS;
import static cwms.cda.api.Controllers.SIZE;
import static cwms.cda.api.Controllers.UNIT;
import static cwms.cda.api.Controllers.UPDATE;
import static cwms.cda.api.Controllers.requiredParam;
import static cwms.cda.data.dao.JooqDao.getDslContext;

import cwms.cda.api.errors.CdaError;
import cwms.cda.data.dao.VerticalDatumDao;
import cwms.cda.data.dto.StatusResponse;
import cwms.cda.data.dto.VerticalDatumInfo;
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
import javax.servlet.http.HttpServletResponse;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;

public final class VerticalDatumController implements CrudHandler {

    private static final FluentLogger logger = FluentLogger.forEnclosingClass();
    private final MetricRegistry metrics;
    private final Histogram requestResultSize;

    public VerticalDatumController(MetricRegistry metrics) {
        this.metrics = metrics;
        String className = this.getClass().getName();
        requestResultSize = this.metrics.histogram((name(className, RESULTS, SIZE)));
    }

    private Timer.Context markAndTime(String subject) {
        return Controllers.markAndTime(metrics, getClass().getName(), subject);
    }

    @Override
    public void getAll(@NotNull Context ctx) {
        ctx.status(HttpServletResponse.SC_NOT_IMPLEMENTED).json(CdaError.notImplemented());
    }

    @OpenApi(
            pathParams = {
                    @OpenApiParam(name = LOCATION_ID, description = "Specifies the location-id.")
            },
            queryParams = {
                    @OpenApiParam(name = OFFICE, required = true, description = "Specifies the owning office."),
                    @OpenApiParam(name = UNIT, description = "Specifies the unit of measure for elevation/offsets (e.g., m or ft). Default is m.")
            },
            responses = {
                    @OpenApiResponse(status = Controllers.STATUS_200,
                            content = {@OpenApiContent(type = Formats.JSONV1, from = VerticalDatumInfo.class),
                                       @OpenApiContent(type = Formats.JSON, from = VerticalDatumInfo.class),
                                       @OpenApiContent(type = Formats.XMLV1, from = VerticalDatumInfo.class),
                                       @OpenApiContent(type = Formats.XML, from = VerticalDatumInfo.class)})
            },
            description = "Returns Vertical Datum Info for the specified location.",
            tags = {"Locations"}
    )
    @Override
    public void getOne(@NotNull Context ctx, @NotNull String locationId) {
        String office = requiredParam(ctx, OFFICE);
        String units = ctx.queryParamAsClass(UNIT, String.class).getOrDefault("m");
        try (Timer.Context ignored = markAndTime(GET_ONE)) {
            DSLContext dsl = getDslContext(ctx);
            VerticalDatumDao dao = new VerticalDatumDao(dsl);
            VerticalDatumInfo info = dao.retrieveVerticalDatumInfo(office, locationId, units);
            String formatHeader = ctx.header(Header.ACCEPT);
            ContentType contentType = Formats.parseHeader(formatHeader, VerticalDatumInfo.class);
            ctx.contentType(contentType.toString());
            String serialized = Formats.format(contentType, info);
            ctx.result(serialized);
            ctx.status(HttpServletResponse.SC_OK);
            requestResultSize.update(serialized.length());
        }
    }

    @OpenApi(
            requestBody = @OpenApiRequestBody(
                    content = {
                            @OpenApiContent(from = VerticalDatumInfo.class, type = Formats.JSONV1),
                            @OpenApiContent(from = VerticalDatumInfo.class, type = Formats.XMLV1)
                    },
                    required = true),
            queryParams = {
                    @OpenApiParam(name = LOCATION_ID, required = true, description = "Specifies the location id for this vertical-datum-info."),
                    @OpenApiParam(name = OFFICE, required = true, description = "Specifies the owning office.")
            },
            description = "Create Vertical Datum Info for a Location",
            method = HttpMethod.POST,
            tags = {"Locations"},
            responses = {
                    @OpenApiResponse(status = Controllers.STATUS_201, description = "Vertical Datum Info successfully stored to CWMS.")
            }
    )
    @Override
    public void create(@NotNull Context ctx) {
        try (Timer.Context ignored = markAndTime(CREATE)) {
            String formatHeader = ctx.req.getContentType();
            ContentType contentType = Formats.parseHeader(formatHeader, VerticalDatumInfo.class);
            VerticalDatumInfo info = Formats.parseContent(contentType, ctx.body(), VerticalDatumInfo.class);
            //allow locationId and office to be specified in either the body or as query params, but require them to be present in one of those places
            String locationId = info.getLocation();
            String office = info.getOffice();
            if(locationId == null || locationId.isBlank()) {
                locationId = requiredParam(ctx, LOCATION_ID);
            }
            if(office == null || office.isBlank()) {
                office = requiredParam(ctx, OFFICE);
            }
            DSLContext dsl = getDslContext(ctx);
            VerticalDatumDao dao = new VerticalDatumDao(dsl);
            dao.createVerticalDatumInfo(office, locationId, info);
            StatusResponse re = new StatusResponse(office,
                    "Vertical Datum Info successfully stored to CWMS.", locationId);
            ctx.status(HttpServletResponse.SC_CREATED).json(re);
        }
    }

    @OpenApi(
            requestBody = @OpenApiRequestBody(
                    content = {
                            @OpenApiContent(from = VerticalDatumInfo.class, type = Formats.JSONV1),
                            @OpenApiContent(from = VerticalDatumInfo.class, type = Formats.XMLV1)
                    },
                    required = true),
            pathParams = {
                    @OpenApiParam(name = LOCATION_ID, description = "The ID of the location to update")
            },
            queryParams = {
                    @OpenApiParam(name = OFFICE, required = true, description = "Specifies the owning office.")
            },
            description = "Update Vertical Datum Info for a Location",
            method = HttpMethod.PATCH,
            tags = {"Locations"},
            responses = {
                    @OpenApiResponse(status = Controllers.STATUS_200, description = "Updated Vertical Datum Info")
            }
    )
    @Override
    public void update(@NotNull Context ctx, @NotNull String locationId) {
        try (Timer.Context ignored = markAndTime(UPDATE)) {
            String formatHeader = ctx.req.getContentType();
            ContentType contentType = Formats.parseHeader(formatHeader, VerticalDatumInfo.class);
            VerticalDatumInfo info = Formats.parseContent(contentType, ctx.body(), VerticalDatumInfo.class);
            //allow locationId and office to be specified in either the body or as query params, but require them to be present in one of those places
            String office = info.getOffice();
            if(office == null || office.isBlank()) {
                office = requiredParam(ctx, OFFICE);
            }
            DSLContext dsl = getDslContext(ctx);
            VerticalDatumDao dao = new VerticalDatumDao(dsl);
            dao.updateVerticalDatumInfo(office, locationId, info);
            StatusResponse re = new StatusResponse(office,
                    "Updated Vertical Datum Info", locationId);
            ctx.status(HttpServletResponse.SC_OK).json(re);
        }
    }

    @OpenApi(
            pathParams = {
                    @OpenApiParam(name = LOCATION_ID, required = true, description = "Specifies the location-id for the vertical-datum being deleted.")
            },
            queryParams = {
                    @OpenApiParam(name = OFFICE, required = true, description = "Specifies the owning office.")
            },
            description = "Delete Vertical Datum Info for a Location",
            method = HttpMethod.DELETE,
            tags = {"Locations"},
            responses = {
                    @OpenApiResponse(status = Controllers.STATUS_200, description = "Vertical Datum Info successfully deleted from CWMS."),
                    @OpenApiResponse(status = Controllers.STATUS_404, description = "Vertical Datum Info not found.")
            }
    )
    @Override
    public void delete(@NotNull Context ctx, @NotNull String locationId) {
        String office = requiredParam(ctx, OFFICE);
        try (Timer.Context ignored = markAndTime(DELETE)) {
            DSLContext dsl = getDslContext(ctx);
            VerticalDatumDao dao = new VerticalDatumDao(dsl);
            dao.deleteVerticalDatumInfo(office, locationId);
            StatusResponse re = new StatusResponse(office,
                    "Vertical Datum Info successfully deleted from CWMS.", locationId);
            ctx.status(HttpServletResponse.SC_OK).json(re);
        }
    }
}
