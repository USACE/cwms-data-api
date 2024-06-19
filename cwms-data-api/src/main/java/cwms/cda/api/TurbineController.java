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
import com.codahale.metrics.Timer;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dao.location.kind.TurbineDao;
import cwms.cda.data.dto.location.kind.Turbine;
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

import static com.codahale.metrics.MetricRegistry.name;
import static cwms.cda.api.Controllers.*;
import static cwms.cda.data.dao.JooqDao.getDslContext;

public final class TurbineController implements CrudHandler {

    static final String TAG = "Turbines";
    private final MetricRegistry metrics;

    private final Histogram requestResultSize;


    public TurbineController(MetricRegistry metrics) {
        this.metrics = metrics;
        String className = this.getClass().getName();

        requestResultSize = this.metrics.histogram((name(className, RESULTS, SIZE)));
    }

    private Timer.Context markAndTime(String subject) {
        return Controllers.markAndTime(metrics, getClass().getName(), subject);
    }

    @OpenApi(
            queryParams = {
                    @OpenApiParam(name = OFFICE, description = "Office id for the reservoir project location " +
                            "associated with the turbines."),
                    @OpenApiParam(name = PROJECT_ID, required = true, description = "Specifies the project-id of the " +
                            "Turbines whose data is to be included in the response."),
            },
            responses = {
                    @OpenApiResponse(status = STATUS_200, content = {
                            @OpenApiContent(isArray = true, type = Formats.JSONV1, from = Turbine.class)
                    })
            },
            description = "Returns matching CWMS Turbine Data for a Reservoir Project.",
            tags = {TAG}
    )
    @Override
    public void getAll(Context ctx) {
        String office = ctx.queryParam(OFFICE);
        String projectId = ctx.queryParam(PROJECT_ID);
        try (Timer.Context ignored = markAndTime(GET_ALL)) {
            DSLContext dsl = getDslContext(ctx);
            TurbineDao dao = new TurbineDao(dsl);
            List<Turbine> turbines = dao.retrieveTurbines(projectId, office);
            String formatHeader = ctx.header(Header.ACCEPT) != null ? ctx.header(Header.ACCEPT) :
                    Formats.JSONV1;
            ContentType contentType = Formats.parseHeader(formatHeader, Turbine.class);
            ctx.contentType(contentType.toString());
            String serialized = Formats.format(contentType, turbines, Turbine.class);
            ctx.result(serialized);
            ctx.status(HttpServletResponse.SC_OK);
            requestResultSize.update(serialized.length());
        }
    }

    @OpenApi(
            pathParams = {
                    @OpenApiParam(name = NAME, required = true, description = "Specifies the name of "
                            + "the turbine to be retrieved."),
            },
            queryParams = {
                    @OpenApiParam(name = OFFICE, required = true, description = "Specifies the owning office of "
                            + "the turbine to be retrieved.")
            },
            responses = {
                    @OpenApiResponse(status = STATUS_200,
                            content = {
                                    @OpenApiContent(type = Formats.JSONV1, from = Turbine.class)
                            })
            },
            description = "Returns CWMS Turbine Data",
            tags = {TAG}
    )
    @Override
    public void getOne(@NotNull Context ctx, @NotNull String name) {
        String office = requiredParam(ctx, OFFICE);
        try (Timer.Context ignored = markAndTime(GET_ONE)) {
            DSLContext dsl = getDslContext(ctx);
            TurbineDao dao = new TurbineDao(dsl);
            Turbine turbine = dao.retrieveTurbine(name, office);
            String header = ctx.header(Header.ACCEPT);
            String formatHeader = header != null ? header : Formats.JSONV1;
            ContentType contentType = Formats.parseHeader(formatHeader, Turbine.class);
            ctx.contentType(contentType.toString());
            String serialized = Formats.format(contentType, turbine);
            ctx.result(serialized);
            ctx.status(HttpServletResponse.SC_OK);
            requestResultSize.update(serialized.length());
        }
    }

    @OpenApi(
            requestBody = @OpenApiRequestBody(
                    content = {
                            @OpenApiContent(from = Turbine.class, type = Formats.JSONV1)
                    },
                    required = true),
            queryParams = {
                    @OpenApiParam(name = FAIL_IF_EXISTS, type = Boolean.class,
                            description = "Create will fail if provided ID already exists. Default: true")
            },
            description = "Create CWMS Turbine",
            method = HttpMethod.POST,
            tags = {TAG},
            responses = {
                    @OpenApiResponse(status = STATUS_204, description = "Turbine successfully stored to CWMS.")
            }
    )
    @Override
    public void create(Context ctx) {
        try (Timer.Context ignored = markAndTime(CREATE)) {
            String acceptHeader = ctx.req.getContentType();
            String formatHeader = acceptHeader != null ? acceptHeader : Formats.JSONV1;
            ContentType contentType = Formats.parseHeader(formatHeader, Turbine.class);
            Turbine turbine = Formats.parseContent(contentType, ctx.body(), Turbine.class);
            turbine.validate();
            boolean failIfExists = ctx.queryParamAsClass(FAIL_IF_EXISTS, Boolean.class).getOrDefault(true);
            DSLContext dsl = getDslContext(ctx);
            TurbineDao dao = new TurbineDao(dsl);
            dao.storeTurbine(turbine, failIfExists);
            ctx.status(HttpServletResponse.SC_CREATED).json("Created Turbine");
        }

    }

    @OpenApi(
            pathParams = {
                    @OpenApiParam(name = NAME, description = "Specifies the name of "
                            + "the turbine to be deleted."),
            },
            queryParams = {
                    @OpenApiParam(name = OFFICE, required = true, description = "Specifies the owning office of "
                            + "the turbine to be deleted."),
                    @OpenApiParam(name = NAME, required = true, description = "Specifies the new turbine name. ")
            },
            description = "Rename CWMS Turbine",
            method = HttpMethod.PATCH,
            tags = {TAG},
            responses = {
                    @OpenApiResponse(status = STATUS_204, description = "Turbine successfully stored to CWMS.")
            }
    )
    @Override
    public void update(@NotNull Context ctx, @NotNull String name) {
        try (Timer.Context ignored = markAndTime(UPDATE)) {
            String office = requiredParam(ctx, OFFICE);
            String newName = requiredParam(ctx, NAME);
            DSLContext dsl = getDslContext(ctx);
            TurbineDao dao = new TurbineDao(dsl);
            dao.renameTurbine(office, name, newName);
            ctx.status(HttpServletResponse.SC_OK).json("Renamed Turbine");
        }
    }

    @OpenApi(
            pathParams = {
                    @OpenApiParam(name = NAME, description = "Specifies the name of "
                            + "the turbine to be deleted."),
            },
            queryParams = {
                    @OpenApiParam(name = OFFICE, required = true, description = "Specifies the owning office of "
                            + "the turbine to be deleted."),
                    @OpenApiParam(name = METHOD, description = "Specifies the delete method used. " +
                            "Defaults to \"DELETE_KEY\"",
                            type = JooqDao.DeleteMethod.class)
            },
            description = "Delete CWMS Turbine",
            method = HttpMethod.DELETE,
            tags = {TAG},
            responses = {
                    @OpenApiResponse(status = STATUS_204, description = "Turbine successfully deleted from CWMS."),
                    @OpenApiResponse(status = STATUS_404, description = "Based on the combination of "
                            + "inputs provided the turbine was not found.")
            }
    )
    @Override
    public void delete(@NotNull Context ctx, @NotNull String name) {
        String office = requiredParam(ctx, OFFICE);
        JooqDao.DeleteMethod deleteMethod = ctx.queryParamAsClass(METHOD, JooqDao.DeleteMethod.class)
                .getOrDefault(JooqDao.DeleteMethod.DELETE_KEY);
        try (Timer.Context ignored = markAndTime(DELETE)) {
            DSLContext dsl = getDslContext(ctx);
            TurbineDao dao = new TurbineDao(dsl);
            dao.deleteTurbine(name, office, deleteMethod.getRule());
            ctx.status(HttpServletResponse.SC_NO_CONTENT).json(name + " Deleted");
        }
    }
}