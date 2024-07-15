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
import static cwms.cda.api.Controllers.AREA_UNIT;
import static cwms.cda.api.Controllers.CREATE;
import static cwms.cda.api.Controllers.DELETE;
import static cwms.cda.api.Controllers.FAIL_IF_EXISTS;
import static cwms.cda.api.Controllers.GET_ALL;
import static cwms.cda.api.Controllers.GET_ONE;
import static cwms.cda.api.Controllers.METHOD;
import static cwms.cda.api.Controllers.NAME;
import static cwms.cda.api.Controllers.NAME_MASK;
import static cwms.cda.api.Controllers.OFFICE;
import static cwms.cda.api.Controllers.OFFICE_MASK;
import static cwms.cda.api.Controllers.RESULTS;
import static cwms.cda.api.Controllers.SIZE;
import static cwms.cda.api.Controllers.STAGE_UNIT;
import static cwms.cda.api.Controllers.STATION_UNIT;
import static cwms.cda.api.Controllers.STATUS_200;
import static cwms.cda.api.Controllers.STATUS_204;
import static cwms.cda.api.Controllers.STATUS_404;
import static cwms.cda.api.Controllers.STREAM_ID;
import static cwms.cda.api.Controllers.STREAM_ID_MASK;
import static cwms.cda.api.Controllers.requiredParam;
import cwms.cda.data.dao.StreamLocationDao;
import cwms.cda.data.dto.stream.StreamLocation;
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
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;

import javax.servlet.http.HttpServletResponse;
import java.util.List;

import static cwms.cda.data.dao.JooqDao.getDslContext;

public final class StreamLocationController implements CrudHandler {

    static final String TAG = "StreamLocations";

    private final MetricRegistry metrics;
    private final Histogram requestResultSize;

    public StreamLocationController(MetricRegistry metrics) {
        this.metrics = metrics;
        String className = this.getClass().getName();
        requestResultSize = this.metrics.histogram((name(className, RESULTS, SIZE)));
    }

    private Timer.Context markAndTime(String subject) {
        return Controllers.markAndTime(metrics, getClass().getName(), subject);
    }

    @OpenApi(
            queryParams = {
                    @OpenApiParam(name = OFFICE_MASK, description = "Office id for the reservoir project location " +
                            "associated with the stream locations."),
                    @OpenApiParam(name = STREAM_ID_MASK, description = "Specifies the stream-id of the " +
                            "stream that the returned stream locations belong to."),
                    @OpenApiParam(name = NAME_MASK, description = "Specifies the location-id of the " +
                            "stream location to be retrieved."),
                    @OpenApiParam(name = STATION_UNIT, description = "Specifies the unit of measure for the station. Default units are mi."),
                    @OpenApiParam(name = STAGE_UNIT, description = "Specifies the unit of measure for the stage. Default units are ft."),
                    @OpenApiParam(name = AREA_UNIT, description = "Specifies the unit of measure for the area. Default units are mi2.")
            },
            responses = {
                    @OpenApiResponse(status = STATUS_200, content = {
                            @OpenApiContent(isArray = true, type = Formats.JSONV1, from = StreamLocation.class),
                            @OpenApiContent(isArray = true, type = Formats.JSON, from = StreamLocation.class)
                    })
            },
            description = "Returns matching CWMS Stream Location Data for a Reservoir Project.",
            tags = {TAG}
    )
    @Override
    public void getAll(Context ctx) {
        String office = ctx.queryParam(OFFICE_MASK);
        String streamId = ctx.queryParam(STREAM_ID_MASK);
        String locationId = ctx.queryParam(NAME_MASK);
        try (Timer.Context ignored = markAndTime(GET_ALL)) {
            DSLContext dsl = getDslContext(ctx);
            StreamLocationDao dao = new StreamLocationDao(dsl);
            //db doesn't return default units, and null units causes a db exception, so defaulting to english if nothing is provided
            String stationUnits = ctx.queryParamAsClass(STATION_UNIT, String.class).getOrDefault("mi");
            String areaUnits = ctx.queryParamAsClass(AREA_UNIT, String.class).getOrDefault("mi2");
            String stageUnits = ctx.queryParamAsClass(STAGE_UNIT, String.class).getOrDefault("ft");
            List<StreamLocation> streamLocations = dao.retrieveStreamLocations(office, streamId, locationId, stationUnits, stageUnits, areaUnits);
            String formatHeader = ctx.header(Header.ACCEPT) != null ? ctx.header(Header.ACCEPT) :
                    Formats.JSONV1;
            ContentType contentType = Formats.parseHeader(formatHeader, StreamLocation.class);
            ctx.contentType(contentType.toString());
            String serialized = Formats.format(contentType, streamLocations, StreamLocation.class);
            ctx.result(serialized);
            ctx.status(HttpServletResponse.SC_OK);
            requestResultSize.update(serialized.length());
        }
    }

    @OpenApi(
            pathParams = {
                    @OpenApiParam(name = NAME, required = true, description = "Specifies the location-id of "
                            + "the stream location to be retrieved."),
            },
            queryParams = {
                    @OpenApiParam(name = OFFICE, required = true, description = "Specifies the owning office of "
                            + "the stream location to be retrieved."),
                    @OpenApiParam(name = STREAM_ID, required = true, description = "Specifies the stream-id of the "
                            + "stream location to be retrieved."),
                    @OpenApiParam(name = STATION_UNIT, description = "Specifies the unit of measure for the station. Default units are mi."),
                    @OpenApiParam(name = STAGE_UNIT, description = "Specifies the unit of measure for the stage. Default units are ft."),
                    @OpenApiParam(name = AREA_UNIT, description = "Specifies the unit of measure for the area. Default units are mi2.")
            },
            responses = {
                    @OpenApiResponse(status = STATUS_200,
                            content = {
                                    @OpenApiContent(isArray = true, type = Formats.JSONV1, from = StreamLocation.class),
                                    @OpenApiContent(isArray = true, type = Formats.JSON, from = StreamLocation.class)
                            })
            },
            description = "Returns CWMS Stream Location Data",
            tags = {TAG}
    )
    @Override
    public void getOne(@NotNull Context ctx, @NotNull String locationId) {
        String office = requiredParam(ctx, OFFICE);
        String streamId = requiredParam(ctx, STREAM_ID);
        try (Timer.Context ignored = markAndTime(GET_ONE)) {
            DSLContext dsl = getDslContext(ctx);
            StreamLocationDao dao = new StreamLocationDao(dsl);
            //db doesn't return default units, and null units causes a db exception, so defaulting to english if nothing is provided
            String stationUnits = ctx.queryParamAsClass(STATION_UNIT, String.class).getOrDefault("mi");
            String areaUnits = ctx.queryParamAsClass(AREA_UNIT, String.class).getOrDefault("mi2");
            String stageUnits = ctx.queryParamAsClass(STAGE_UNIT, String.class).getOrDefault("ft");
            StreamLocation streamLocation = dao.retrieveStreamLocation(office, streamId, locationId, stationUnits, stageUnits, areaUnits);
            String header = ctx.header(Header.ACCEPT);
            String formatHeader = header != null ? header : Formats.JSONV1;
            ContentType contentType = Formats.parseHeader(formatHeader, StreamLocation.class);
            ctx.contentType(contentType.toString());
            String serialized = Formats.format(contentType, streamLocation);
            ctx.result(serialized);
            ctx.status(HttpServletResponse.SC_OK);
            requestResultSize.update(serialized.length());
        }
    }

    @OpenApi(
            requestBody = @OpenApiRequestBody(
                    content = {
                            @OpenApiContent(from = StreamLocation.class, type = Formats.JSONV1)
                    },
                    required = true),
            queryParams = {
                    @OpenApiParam(name = FAIL_IF_EXISTS, type = Boolean.class,
                            description = "Create will fail if provided ID already exists. Default: true")
            },
            description = "Create CWMS Stream Location",
            method = HttpMethod.POST,
            tags = {TAG},
            responses = {
                    @OpenApiResponse(status = STATUS_204, description = "Stream Location successfully stored to CWMS.")
            }
    )
    @Override
    public void create(Context ctx) {
        try (Timer.Context ignored = markAndTime(CREATE)) {
            String acceptHeader = ctx.req.getContentType();
            String formatHeader = acceptHeader != null ? acceptHeader : Formats.JSONV1;
            ContentType contentType = Formats.parseHeader(formatHeader, StreamLocation.class);
            StreamLocation streamLocation = Formats.parseContent(contentType, ctx.body(), StreamLocation.class);
            boolean failIfExists = ctx.queryParamAsClass(FAIL_IF_EXISTS, Boolean.class).getOrDefault(true);
            DSLContext dsl = getDslContext(ctx);
            StreamLocationDao dao = new StreamLocationDao(dsl);
            dao.storeStreamLocation(streamLocation, failIfExists);
            ctx.status(HttpServletResponse.SC_CREATED).json("Created Stream Location");
        }
    }

    @OpenApi(
            requestBody = @OpenApiRequestBody(
                    content = {
                            @OpenApiContent(from = StreamLocation.class, type = Formats.JSONV1)
                    },
                    required = true),
            description = "Update CWMS Stream Location",
            method = HttpMethod.PATCH,
            tags = {TAG},
            responses = {
                    @OpenApiResponse(status = STATUS_204, description = "Stream Location successfully updated to CWMS.")
            }
    )
    @Override
    public void update(Context ctx, @NotNull String locationId) {
        try (Timer.Context ignored = markAndTime(METHOD + "update")) {
            String acceptHeader = ctx.req.getContentType();
            String formatHeader = acceptHeader != null ? acceptHeader : Formats.JSONV1;
            ContentType contentType = Formats.parseHeader(formatHeader, StreamLocation.class);
            StreamLocation streamLocation = Formats.parseContent(contentType, ctx.body(), StreamLocation.class);
            DSLContext dsl = getDslContext(ctx);
            StreamLocationDao dao = new StreamLocationDao(dsl);
            dao.updateStreamLocation(streamLocation);
            ctx.status(HttpServletResponse.SC_NO_CONTENT).json("Updated Stream Location");
        }
    }

    @OpenApi(
            pathParams = {
                    @OpenApiParam(name = NAME, required = true, description = "Specifies the location-id of "
                            + "the stream location to be deleted."),
            },
            queryParams = {
                    @OpenApiParam(name = OFFICE, required = true, description = "Specifies the owning office of "
                            + "the stream location to be deleted."),
                    @OpenApiParam(name = STREAM_ID, required = true, description = "Specifies the stream-id of the "
                            + "stream location to be deleted.")
            },
            description = "Delete CWMS Stream Location",
            method = HttpMethod.DELETE,
            tags = {TAG},
            responses = {
                    @OpenApiResponse(status = STATUS_204, description = "Stream Location successfully deleted from CWMS."),
                    @OpenApiResponse(status = STATUS_404, description = "Stream Location not found.")
            }
    )
    @Override
    public void delete(@NotNull Context ctx, @NotNull String locationId) {
        String officeId = requiredParam(ctx, OFFICE);
        String streamId = requiredParam(ctx, STREAM_ID);
        try (Timer.Context ignored = markAndTime(DELETE)) {
            DSLContext dsl = getDslContext(ctx);
            StreamLocationDao dao = new StreamLocationDao(dsl);
            dao.deleteStreamLocation(officeId, streamId, locationId);
            ctx.status(HttpServletResponse.SC_NO_CONTENT).json("Deleted Stream Location");
        }
    }
}
