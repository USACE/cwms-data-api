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
import static cwms.cda.api.Controllers.CONFIGURATION_ID_MASK;
import static cwms.cda.api.Controllers.CREATE;
import static cwms.cda.api.Controllers.DELETE;
import static cwms.cda.api.Controllers.FAIL_IF_EXISTS;
import static cwms.cda.api.Controllers.GET_ALL;
import static cwms.cda.api.Controllers.GET_ONE;
import static cwms.cda.api.Controllers.NAME;
import static cwms.cda.api.Controllers.OFFICE;
import static cwms.cda.api.Controllers.OFFICE_MASK;
import static cwms.cda.api.Controllers.REACH_ID_MASK;
import static cwms.cda.api.Controllers.RESULTS;
import static cwms.cda.api.Controllers.SIZE;
import static cwms.cda.api.Controllers.STATION_UNIT;
import static cwms.cda.api.Controllers.STATUS_200;
import static cwms.cda.api.Controllers.STATUS_204;
import static cwms.cda.api.Controllers.STREAM_ID;
import static cwms.cda.api.Controllers.STREAM_ID_MASK;
import static cwms.cda.api.Controllers.UPDATE;
import static cwms.cda.api.Controllers.requiredParam;
import cwms.cda.data.dao.StreamLocationDao;
import cwms.cda.data.dao.StreamReachDao;
import cwms.cda.data.dto.stream.StreamReach;
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

public final class StreamReachController implements CrudHandler {

    static final String TAG = "StreamReaches";

    private final MetricRegistry metrics;
    private final Histogram requestResultSize;

    public StreamReachController(MetricRegistry metrics) {
        this.metrics = metrics;
        String className = this.getClass().getName();
        requestResultSize = this.metrics.histogram(name(className, RESULTS, SIZE));
    }

    private Timer.Context markAndTime(String subject) {
        return Controllers.markAndTime(metrics, getClass().getName(), subject);
    }

    @OpenApi(
            queryParams = {
                    @OpenApiParam(name = OFFICE_MASK, description = "Office id for the reservoir project location " +
                            "associated with the stream reaches."),
                    @OpenApiParam(name = STREAM_ID_MASK, description = "Specifies the stream-id mask for the stream reaches."),
                    @OpenApiParam(name = REACH_ID_MASK, description = "Specifies the reach-id mask for the stream reaches."),
                    @OpenApiParam(name = CONFIGURATION_ID_MASK, description = "Specifies the configuration-id mask for the stream reaches."),
                    @OpenApiParam(name = STATION_UNIT, description = "Specifies the unit of measure for the station. " +
                            "Defaults to mi.")
            },
            responses = {
                    @OpenApiResponse(status = STATUS_200, content = {
                            @OpenApiContent(isArray = true, type = Formats.JSONV1, from = StreamReach.class),
                            @OpenApiContent(isArray = true, type = Formats.JSON, from = StreamReach.class)
                    })
            },
            description = "Returns matching CWMS Stream Reach Data for a Reservoir Project.",
            tags = {TAG}
    )
    @Override
    public void getAll(@NotNull Context ctx) {
        String office = ctx.queryParam(OFFICE_MASK);
        String streamIdMask = ctx.queryParam(STREAM_ID_MASK);
        String reachIdMask = ctx.queryParam(REACH_ID_MASK);
        String configurationIdMask = ctx.queryParam(CONFIGURATION_ID_MASK);
        try (Timer.Context ignored = markAndTime(GET_ALL)) {
            DSLContext dsl = getDslContext(ctx);
            StreamReachDao dao = new StreamReachDao(dsl);
            String stationUnits = ctx.queryParamAsClass(STATION_UNIT, String.class).getOrDefault("mi");
            List<StreamReach> streamReaches = dao.retrieveStreamReaches(office, streamIdMask, reachIdMask, configurationIdMask, stationUnits);
            String formatHeader = ctx.header(Header.ACCEPT);
            ContentType contentType = Formats.parseHeader(formatHeader, StreamReach.class);
            ctx.contentType(contentType.toString());
            String serialized = Formats.format(contentType, streamReaches, StreamReach.class);
            ctx.result(serialized);
            ctx.status(HttpServletResponse.SC_OK);
            requestResultSize.update(serialized.length());
        }
    }

    @OpenApi(
            pathParams = {
                    @OpenApiParam(name = NAME, required = true, description = "Specifies the reach-id of "
                            + "the stream reach to be retrieved."),
            },
            queryParams = {
                    @OpenApiParam(name = OFFICE, required = true, description = "Specifies the owning office of "
                            + "the stream reach to be retrieved."),
                    @OpenApiParam(name = STREAM_ID, required = true, description = "Specifies the stream-id of "
                            + "the stream reach to be retrieved."),
                    @OpenApiParam(name = STATION_UNIT, description = "Specifies the unit of measure for the station. " +
                            "Defaults to mi.")
            },
            responses = {
                    @OpenApiResponse(status = STATUS_200,
                            content = {
                                    @OpenApiContent(isArray = true, type = Formats.JSONV1, from = StreamReach.class),
                                    @OpenApiContent(isArray = true, type = Formats.JSON, from = StreamReach.class)
                            })
            },
            description = "Returns CWMS Stream Reach Data",
            tags = {TAG}
    )
    @Override
    public void getOne(@NotNull Context ctx, @NotNull String reachId) {
        String office = requiredParam(ctx, OFFICE);
        String streamId = requiredParam(ctx, STREAM_ID);
        try (Timer.Context ignored = markAndTime(GET_ONE)) {
            DSLContext dsl = getDslContext(ctx);
            StreamReachDao dao = new StreamReachDao(dsl);
            String stationUnits = ctx.queryParamAsClass(STATION_UNIT, String.class).getOrDefault("mi");
            StreamReach streamReach = dao.retrieveStreamReach(office, streamId, reachId, stationUnits);
            String header = ctx.header(Header.ACCEPT);
            String formatHeader = header != null ? header : Formats.JSONV1;
            ContentType contentType = Formats.parseHeader(formatHeader, StreamReach.class);
            ctx.contentType(contentType.toString());
            String serialized = Formats.format(contentType, streamReach);
            ctx.result(serialized);
            ctx.status(HttpServletResponse.SC_OK);
            requestResultSize.update(serialized.length());
        }
    }

    @OpenApi(
            requestBody = @OpenApiRequestBody(
                    content = {
                            @OpenApiContent(from = StreamReach.class, type = Formats.JSONV1)
                    },
                    required = true),
            queryParams = {
                    @OpenApiParam(name = FAIL_IF_EXISTS, type = Boolean.class,
                            description = "Create will fail if provided ID already exists. Default: true")
            },
            description = "Create CWMS Stream Reach",
            method = HttpMethod.POST,
            tags = {TAG},
            responses = {
                    @OpenApiResponse(status = STATUS_204, description = "Stream Reach successfully stored to CWMS.")
            }
    )
    @Override
    public void create(@NotNull Context ctx) {
        try (Timer.Context ignored = markAndTime(CREATE)) {
            String formatHeader = ctx.req.getContentType();
            ContentType contentType = Formats.parseHeader(formatHeader, StreamReach.class);
            StreamReach streamReach = Formats.parseContent(contentType, ctx.body(), StreamReach.class);
            boolean failIfExists = ctx.queryParamAsClass(FAIL_IF_EXISTS, Boolean.class).getOrDefault(true);
            DSLContext dsl = getDslContext(ctx);
            StreamReachDao dao = new StreamReachDao(dsl);
            dao.storeStreamReach(streamReach, failIfExists);
            ctx.status(HttpServletResponse.SC_CREATED).json("Created Stream Reach");
        }
    }

    @OpenApi(
            pathParams = {
                    @OpenApiParam(name = NAME, description = "Specifies the reach-id of "
                            + "the stream reach to be renamed."),
            },
            queryParams = {
                    @OpenApiParam(name = OFFICE, required = true, description = "Specifies the owning office of "
                            + "the stream reach to be renamed."),
                    @OpenApiParam(name = NAME, required = true, description = "Specifies the new reach-id. ")
            },
            description = "Rename CWMS Stream Reach",
            method = HttpMethod.PATCH,
            tags = {TAG},
            responses = {
                    @OpenApiResponse(status = STATUS_204, description = "Stream Reach successfully renamed in CWMS.")
            }
    )
    @Override
    public void update(@NotNull Context ctx, @NotNull String reachId) {
        try (Timer.Context ignored = markAndTime(UPDATE)) {
            String office = requiredParam(ctx, OFFICE);
            String newReachId = requiredParam(ctx, NAME);
            DSLContext dsl = getDslContext(ctx);
            StreamReachDao dao = new StreamReachDao(dsl);
            dao.renameStreamReach(office, reachId, newReachId);
            ctx.status(HttpServletResponse.SC_OK).json("Renamed Stream Reach");
        }
    }

    @OpenApi(
            pathParams = {
                    @OpenApiParam(name = NAME, required = true, description = "Specifies the reach-id of "
                            + "the stream reach to be deleted."),
            },
            queryParams = {
                    @OpenApiParam(name = OFFICE, required = true, description = "Specifies the owning office of "
                            + "the stream reach to be deleted.")
            },
            description = "Delete CWMS Stream Reach",
            method = HttpMethod.DELETE,
            tags = {TAG},
            responses = {
                    @OpenApiResponse(status = STATUS_204, description = "Stream Reach successfully deleted from CWMS.")
            }
    )
    @Override
    public void delete(@NotNull Context ctx, @NotNull String reachId) {
        try (Timer.Context ignored = markAndTime(DELETE)) {
            String office = requiredParam(ctx, OFFICE);
            DSLContext dsl = getDslContext(ctx);
            StreamReachDao dao = new StreamReachDao(dsl);
            dao.deleteStreamReach(office, reachId);
            ctx.status(HttpServletResponse.SC_OK).json("Deleted Stream Reach");
        }
    }
}
