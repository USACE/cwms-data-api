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

package cwms.cda.api.location.kind;

import static com.codahale.metrics.MetricRegistry.name;
import static cwms.cda.api.Controllers.CREATE;
import static cwms.cda.api.Controllers.DELETE;
import static cwms.cda.api.Controllers.FAIL_IF_EXISTS;
import static cwms.cda.api.Controllers.GET_ALL;
import static cwms.cda.api.Controllers.GET_ONE;
import static cwms.cda.api.Controllers.METHOD;
import static cwms.cda.api.Controllers.NAME;
import static cwms.cda.api.Controllers.OFFICE;
import static cwms.cda.api.Controllers.PROJECT_ID;
import static cwms.cda.api.Controllers.RESULTS;
import static cwms.cda.api.Controllers.SIZE;
import static cwms.cda.api.Controllers.STATUS_200;
import static cwms.cda.api.Controllers.STATUS_204;
import static cwms.cda.api.Controllers.STATUS_404;
import static cwms.cda.api.Controllers.UNIT;
import static cwms.cda.api.Controllers.UPDATE;
import static cwms.cda.api.Controllers.requiredParam;
import static cwms.cda.data.dao.JooqDao.getDslContext;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.cda.api.Controllers;
import cwms.cda.api.enums.UnitSystem;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dao.location.kind.LockDao;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.location.kind.Lock;
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

public final class LockController implements CrudHandler {
    static final String TAG = "Locks";
    private final MetricRegistry metrics;

    private final Histogram requestResultSize;

    public LockController(MetricRegistry metrics) {
        this.metrics = metrics;
        String className = this.getClass().getName();

        requestResultSize = this.metrics.histogram((name(className, RESULTS, SIZE)));
    }

    private Timer.Context markAndTime(String subject) {
        return Controllers.markAndTime(metrics, getClass().getName(), subject);
    }

    @OpenApi(
        queryParams = {
            @OpenApiParam(name = OFFICE, description = "Office id for the reservoir project location "
                + "associated with the locks."),
            @OpenApiParam(name = PROJECT_ID, required = true, description = "Specifies the project-id of the "
                + "Locks whose data is to be included in the response."),
            @OpenApiParam(name = UNIT, description = "Specifies the unit system to be used in the response. "
                + "Valid values are: \n* `SI` - Metric units. \n* `EN` - Imperial units. \nDefaults to SI.")
        },
        responses = {
            @OpenApiResponse(status = STATUS_200, content = {
                @OpenApiContent(isArray = true, type = Formats.JSONV1, from = Lock.class),
                @OpenApiContent(isArray = true, type = Formats.JSON, from = Lock.class)
            })
        },
        description = "Returns matching CWMS Lock Data for a Reservoir Project.",
        tags = {TAG}
    )
    @Override
    public void getAll(Context ctx) {
        try (Timer.Context ignored = markAndTime(GET_ALL)) {
            String office = ctx.queryParam(OFFICE);
            String projectId = ctx.queryParam(PROJECT_ID);
            UnitSystem unitSystem = ctx.queryParamAsClass(UNIT, UnitSystem.class).getOrDefault(UnitSystem.SI);
            CwmsId project = CwmsId.buildCwmsId(office, projectId);
            DSLContext dsl = getDslContext(ctx);
            LockDao dao = new LockDao(dsl);
            List<CwmsId> locks = dao.retrieveLockIds(project, unitSystem);
            String formatHeader = ctx.header(Header.ACCEPT) != null ? ctx.header(Header.ACCEPT) :
                Formats.JSONV1;
            ContentType contentType = Formats.parseHeader(formatHeader, Lock.class);
            ctx.contentType(contentType.toString());
            String serialized = Formats.format(contentType, locks, Lock.class);
            ctx.result(serialized);
            ctx.status(HttpServletResponse.SC_OK);
            requestResultSize.update(serialized.length());
        }
    }

    @OpenApi(
        pathParams = {
            @OpenApiParam(name = NAME, required = true, description = "Specifies the name of "
                + "the lock to be retrieved."),
        },
        queryParams = {
            @OpenApiParam(name = OFFICE, required = true, description = "Specifies the owning office of "
                + "the lock to be retrieved."),
            @OpenApiParam(name = UNIT, description = "Specifies the unit system to be used in the response. "
                + "Valid values are: \n* `SI` - Metric units. \n* `EN` - Imperial units. \nDefaults to SI.")
        },
        responses = {
            @OpenApiResponse(status = STATUS_200,
                content = {
                    @OpenApiContent(isArray = true, type = Formats.JSONV1, from = Lock.class),
                    @OpenApiContent(isArray = true, type = Formats.JSON, from = Lock.class)
                })
        },
        description = "Returns CWMS Lock Data",
        tags = {TAG}
    )
    @Override
    public void getOne(@NotNull Context ctx, @NotNull String name) {
        String office = requiredParam(ctx, OFFICE);
        UnitSystem unitSystem = ctx.queryParamAsClass(UNIT, UnitSystem.class).getOrDefault(UnitSystem.SI);
        try (Timer.Context ignored = markAndTime(GET_ONE)) {
            DSLContext dsl = getDslContext(ctx);
            LockDao dao = new LockDao(dsl);
            Lock lock = dao.retrieveLock(CwmsId.buildCwmsId(office, name), unitSystem);
            String header = ctx.header(Header.ACCEPT);
            String formatHeader = header != null ? header : Formats.JSONV1;
            ContentType contentType = Formats.parseHeader(formatHeader, Lock.class);
            ctx.contentType(contentType.toString());
            String serialized = Formats.format(contentType, lock);
            ctx.result(serialized);
            ctx.status(HttpServletResponse.SC_OK);
            requestResultSize.update(serialized.length());
        }
    }

    @OpenApi(
        requestBody = @OpenApiRequestBody(
            content = {
                @OpenApiContent(from = Lock.class, type = Formats.JSONV1)
            },
            required = true),
        queryParams = {
            @OpenApiParam(name = FAIL_IF_EXISTS, type = Boolean.class,
                description = "Create will fail if provided ID already exists. Default: true")
        },
        description = "Create CWMS Lock",
        method = HttpMethod.POST,
        tags = {TAG},
        responses = {
            @OpenApiResponse(status = STATUS_204, description = "Lock successfully stored to CWMS.")
        }
    )
    @Override
    public void create(Context ctx) {
        try (Timer.Context ignored = markAndTime(CREATE)) {
            String acceptHeader = ctx.req.getContentType();
            String formatHeader = acceptHeader != null ? acceptHeader : Formats.JSONV1;
            ContentType contentType = Formats.parseHeader(formatHeader, Lock.class);
            Lock lock = Formats.parseContent(contentType, ctx.body(), Lock.class);
            boolean failIfExists = ctx.queryParamAsClass(FAIL_IF_EXISTS, Boolean.class).getOrDefault(true);
            DSLContext dsl = getDslContext(ctx);
            LockDao dao = new LockDao(dsl);
            dao.storeLock(lock, failIfExists);
            ctx.status(HttpServletResponse.SC_CREATED).json("Created Lock");
        }
    }

    @OpenApi(
        pathParams = {
            @OpenApiParam(name = NAME, description = "Specifies the name of "
                + "the lock to be renamed."),
        },
        queryParams = {
            @OpenApiParam(name = OFFICE, required = true, description = "Specifies the owning office of "
                + "the lock to be renamed."),
            @OpenApiParam(name = NAME, required = true, description = "Specifies the new lock name.")
        },
        description = "Rename CWMS Lock",
        method = HttpMethod.PATCH,
        tags = {TAG},
        responses = {
            @OpenApiResponse(status = STATUS_204, description = "Lock successfully renamed in CWMS.")
        }
    )
    @Override
    public void update(@NotNull Context ctx, @NotNull String name) {
        try (Timer.Context ignored = markAndTime(UPDATE)) {
            String office = requiredParam(ctx, OFFICE);
            String newName = requiredParam(ctx, NAME);
            DSLContext dsl = getDslContext(ctx);
            LockDao dao = new LockDao(dsl);
            dao.renameLock(CwmsId.buildCwmsId(office, name), newName);
            ctx.status(HttpServletResponse.SC_OK).json("Renamed Lock");
        }
    }

    @OpenApi(
        pathParams = {
            @OpenApiParam(name = NAME, description = "Specifies the name of "
                + "the lock to be deleted."),
        },
        queryParams = {
            @OpenApiParam(name = OFFICE, required = true, description = "Specifies the owning office of "
                + "the lock to be deleted."),
            @OpenApiParam(name = METHOD, description = "Specifies the delete method used. "
                + "Defaults to \"DELETE_KEY\"", type = JooqDao.DeleteMethod.class)
        },
        description = "Delete CWMS Lock",
        method = HttpMethod.DELETE,
        tags = {TAG},
        responses = {
            @OpenApiResponse(status = STATUS_204, description = "Lock successfully deleted from CWMS."),
            @OpenApiResponse(status = STATUS_404, description = "Based on the combination of "
                + "inputs provided the lock was not found.")
        }
    )
    @Override
    public void delete(@NotNull Context ctx, @NotNull String name) {
        try (Timer.Context ignored = markAndTime(DELETE)) {
            String office = requiredParam(ctx, OFFICE);
            JooqDao.DeleteMethod deleteMethod = ctx.queryParamAsClass(METHOD, JooqDao.DeleteMethod.class)
                .getOrDefault(JooqDao.DeleteMethod.DELETE_KEY);
            DSLContext dsl = getDslContext(ctx);
            LockDao dao = new LockDao(dsl);
            dao.deleteLock(CwmsId.buildCwmsId(office, name), deleteMethod.getRule());
            ctx.status(HttpServletResponse.SC_NO_CONTENT).json(name + " Deleted");
        }
    }
}