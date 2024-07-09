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
import static cwms.cda.api.Controllers.DIVERTS_FROM_STREAM_ID_MASK;
import static cwms.cda.api.Controllers.FAIL_IF_EXISTS;
import static cwms.cda.api.Controllers.FLOWS_INTO_STREAM_ID_MASK;
import static cwms.cda.api.Controllers.GET_ALL;
import static cwms.cda.api.Controllers.GET_ONE;
import static cwms.cda.api.Controllers.METHOD;
import static cwms.cda.api.Controllers.NAME;
import static cwms.cda.api.Controllers.OFFICE;
import static cwms.cda.api.Controllers.OFFICE_MASK;
import static cwms.cda.api.Controllers.RESULTS;
import static cwms.cda.api.Controllers.SIZE;
import static cwms.cda.api.Controllers.STATION_UNIT;
import static cwms.cda.api.Controllers.STATUS_200;
import static cwms.cda.api.Controllers.STATUS_204;
import static cwms.cda.api.Controllers.STATUS_404;
import static cwms.cda.api.Controllers.UPDATE;
import static cwms.cda.api.Controllers.requiredParam;
import cwms.cda.data.dao.DeleteRule;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dao.StreamDao;
import cwms.cda.data.dto.stream.Stream;
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

public final class StreamController implements CrudHandler {

    static final String TAG = "Streams";

    private final MetricRegistry metrics;
    private final Histogram requestResultSize;

    public StreamController(MetricRegistry metrics) {
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
                            "associated with the streams."),
                    @OpenApiParam(name = DIVERTS_FROM_STREAM_ID_MASK, description = "Specifies the stream-id of the " +
                            "stream that the returned streams flow from."),
                    @OpenApiParam(name = FLOWS_INTO_STREAM_ID_MASK, description = "Specifies the stream-id of the " +
                            "stream that the returned streams flow into."),
                    @OpenApiParam(name = STATION_UNIT, description = "Specifies the unit of measure for the station. " +
                            "Defaults to mi.")
            },
            responses = {
                    @OpenApiResponse(status = STATUS_200, content = {
                            @OpenApiContent(isArray = true, type = Formats.JSONV1, from = Stream.class),
                            @OpenApiContent(isArray = true, type = Formats.JSON, from = Stream.class)
                    })
            },
            description = "Returns matching CWMS Stream Data for a Reservoir Project.",
            tags = {TAG}
    )
    @Override
    public void getAll(@NotNull Context ctx) {
        String office = ctx.queryParam(OFFICE_MASK);
        String divertsFromStream = ctx.queryParam(DIVERTS_FROM_STREAM_ID_MASK);
        String flowsIntoStream = ctx.queryParam(FLOWS_INTO_STREAM_ID_MASK);
        try (Timer.Context ignored = markAndTime(GET_ALL)) {
            DSLContext dsl = getDslContext(ctx);
            StreamDao dao = new StreamDao(dsl);
            String stationUnits = ctx.queryParam(STATION_UNIT) == null ? "mi" : ctx.queryParam(STATION_UNIT);
            List<Stream> streams = dao.retrieveStreams(office, divertsFromStream, flowsIntoStream, stationUnits);
            String formatHeader = ctx.header(Header.ACCEPT) != null ? ctx.header(Header.ACCEPT) :
                    Formats.JSONV1;
            ContentType contentType = Formats.parseHeader(formatHeader, Stream.class);
            ctx.contentType(contentType.toString());
            String serialized = Formats.format(contentType, streams, Stream.class);
            ctx.result(serialized);
            ctx.status(HttpServletResponse.SC_OK);
            requestResultSize.update(serialized.length());
        }
    }

    @OpenApi(
            pathParams = {
                    @OpenApiParam(name = NAME, required = true, description = "Specifies the stream-id of "
                            + "the stream to be retrieved."),
            },
            queryParams = {
                    @OpenApiParam(name = OFFICE, required = true, description = "Specifies the owning office of "
                            + "the stream to be retrieved."),
                    @OpenApiParam(name = STATION_UNIT, description = "Specifies the unit of measure for the station. " +
                            "Defaults to mi.")
            },
            responses = {
                    @OpenApiResponse(status = STATUS_200,
                            content = {
                                    @OpenApiContent(isArray = true, type = Formats.JSONV1, from = Stream.class),
                                    @OpenApiContent(isArray = true, type = Formats.JSON, from = Stream.class)
                            })
            },
            description = "Returns CWMS Stream Data",
            tags = {TAG}
    )
    @Override
    public void getOne(@NotNull Context ctx, @NotNull String streamId) {
        String office = requiredParam(ctx, OFFICE);
        try (Timer.Context ignored = markAndTime(GET_ONE)) {
            DSLContext dsl = getDslContext(ctx);
            StreamDao dao = new StreamDao(dsl);
            String stationUnits = ctx.queryParam(STATION_UNIT) == null ? "mi" : ctx.queryParam(STATION_UNIT);
            Stream stream = dao.retrieveStream(office, streamId, stationUnits);
            String header = ctx.header(Header.ACCEPT);
            String formatHeader = header != null ? header : Formats.JSONV1;
            ContentType contentType = Formats.parseHeader(formatHeader, Stream.class);
            ctx.contentType(contentType.toString());
            String serialized = Formats.format(contentType, stream);
            ctx.result(serialized);
            ctx.status(HttpServletResponse.SC_OK);
            requestResultSize.update(serialized.length());
        }
    }

    @OpenApi(
            requestBody = @OpenApiRequestBody(
                    content = {
                            @OpenApiContent(from = Stream.class, type = Formats.JSONV1)
                    },
                    required = true),
            queryParams = {
                    @OpenApiParam(name = FAIL_IF_EXISTS, type = Boolean.class,
                            description = "Create will fail if provided ID already exists. Default: true")
            },
            description = "Create CWMS Stream",
            method = HttpMethod.POST,
            tags = {TAG},
            responses = {
                    @OpenApiResponse(status = STATUS_204, description = "Stream successfully stored to CWMS.")
            }
    )
    @Override
    public void create(Context ctx) {
        try (Timer.Context ignored = markAndTime(CREATE)) {
            String acceptHeader = ctx.req.getContentType();
            String formatHeader = acceptHeader != null ? acceptHeader : Formats.JSONV1;
            ContentType contentType = Formats.parseHeader(formatHeader, Stream.class);
            Stream stream = Formats.parseContent(contentType, ctx.body(), Stream.class);
            boolean failIfExists = ctx.queryParamAsClass(FAIL_IF_EXISTS, Boolean.class).getOrDefault(true);
            DSLContext dsl = getDslContext(ctx);
            StreamDao dao = new StreamDao(dsl);
            dao.storeStream(stream, failIfExists);
            ctx.status(HttpServletResponse.SC_CREATED).json("Created Stream");
        }

    }

    @OpenApi(
            pathParams = {
                    @OpenApiParam(name = NAME, description = "Specifies the stream-id of "
                            + "the stream to be renamed."),
            },
            queryParams = {
                    @OpenApiParam(name = OFFICE, required = true, description = "Specifies the owning office of "
                            + "the stream to be renamed."),
                    @OpenApiParam(name = NAME, required = true, description = "Specifies the new stream-id. ")
            },
            description = "Rename CWMS Stream",
            method = HttpMethod.PATCH,
            tags = {TAG},
            responses = {
                    @OpenApiResponse(status = STATUS_204, description = "Stream successfully renamed in CWMS.")
            }
    )
    @Override
    public void update(@NotNull Context ctx, @NotNull String streamId) {
        try (Timer.Context ignored = markAndTime(UPDATE)) {
            String office = requiredParam(ctx, OFFICE);
            String newStreamId = requiredParam(ctx, NAME);
            DSLContext dsl = getDslContext(ctx);
            StreamDao dao = new StreamDao(dsl);
            dao.renameStream(office, streamId, newStreamId);
            ctx.status(HttpServletResponse.SC_OK).json("Renamed Stream");
        }
    }

    @OpenApi(
            pathParams = {
                    @OpenApiParam(name = NAME, description = "Specifies the stream-id of "
                            + "the stream to be deleted."),
            },
            queryParams = {
                    @OpenApiParam(name = OFFICE, required = true, description = "Specifies the owning office of "
                            + "the stream to be deleted."),
                    @OpenApiParam(name = METHOD, description = "Specifies the delete method used. " +
                            "Defaults to \"DELETE_KEY\"",
                            type = JooqDao.DeleteMethod.class)
            },
            description = "Delete CWMS Stream",
            method = HttpMethod.DELETE,
            tags = {TAG},
            responses = {
                    @OpenApiResponse(status = STATUS_204, description = "Stream successfully deleted from CWMS."),
                    @OpenApiResponse(status = STATUS_404, description = "Based on the combination of "
                            + "inputs provided the stream was not found.")
            }
    )
    @Override
    public void delete(@NotNull Context ctx, @NotNull String streamId) {
        String office = requiredParam(ctx, OFFICE);
        try (Timer.Context ignored = markAndTime(DELETE)) {
            DSLContext dsl = getDslContext(ctx);
            StreamDao dao = new StreamDao(dsl);
            JooqDao.DeleteMethod deleteMethod = ctx.queryParamAsClass(METHOD, JooqDao.DeleteMethod.class)
                    .getOrDefault(JooqDao.DeleteMethod.DELETE_KEY);
            DeleteRule deleteRule;
            switch (deleteMethod) {
                case DELETE_DATA:
                    deleteRule = DeleteRule.DELETE_DATA;
                    break;
                case DELETE_ALL:
                    deleteRule = DeleteRule.DELETE_ALL;
                    break;
                case DELETE_KEY:
                    deleteRule = DeleteRule.DELETE_KEY;
                    break;
                default:
                    throw new IllegalArgumentException("Delete Method provided does not match accepted rule constants: "
                            + deleteMethod);
            }
            dao.deleteStream(office, streamId, deleteRule);
            ctx.status(HttpServletResponse.SC_NO_CONTENT).json(streamId + " Deleted");
        }
    }
}