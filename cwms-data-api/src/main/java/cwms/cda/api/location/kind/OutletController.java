/*
 * MIT License
 * Copyright (c) 2024 Hydrologic Engineering Center
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package cwms.cda.api.location.kind;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.cda.api.Controllers;
import cwms.cda.api.errors.CdaError;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dao.location.kind.OutletDao;
import cwms.cda.data.dto.location.kind.Outlet;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import io.javalin.apibuilder.CrudHandler;
import io.javalin.core.util.Header;
import io.javalin.http.Context;
import io.javalin.plugin.openapi.annotations.HttpMethod;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import javax.servlet.http.HttpServletResponse;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;
import static com.codahale.metrics.MetricRegistry.name;
import static cwms.cda.api.Controllers.*;
import static cwms.cda.data.dao.JooqDao.getDslContext;

public class OutletController implements CrudHandler {
    static final String TAG = "Outlets";

    private final MetricRegistry metrics;

    private final Histogram requestResultSize;

    public OutletController(MetricRegistry metrics) {
        this.metrics = metrics;
        String className = this.getClass().getName();

        requestResultSize = this.metrics.histogram((name(className, RESULTS, SIZE)));
    }

    private Timer.Context markAndTime(String subject) {
        return Controllers.markAndTime(metrics, getClass().getName(), subject);
    }
    @OpenApi(ignore = true)
    @Override
    public void create(@NotNull Context ctx) {
        //Implemented in OutletCreateController
        ctx.status(HttpServletResponse.SC_NOT_IMPLEMENTED).json(CdaError.notImplemented());
    }

    @OpenApi(ignore = true)
    @Override
    public void getAll(@NotNull Context ctx) {
        //Implemented in OutletGetAllController
        ctx.status(HttpServletResponse.SC_NOT_IMPLEMENTED).json(CdaError.notImplemented());
    }

    @OpenApi(
            pathParams = {
                    @OpenApiParam(name = NAME, required = true, description = "Specifies the location-id of the " +
                            "Outlet to be created."),
            },
            queryParams = {
                    @OpenApiParam(name = OFFICE, required = true, description = "Specifies the owning office of "
                            + "the outlet to be retrieved."),
            },
            responses = {
                    @OpenApiResponse(status = STATUS_200,
                            content = {
                                    @OpenApiContent(from = Outlet.class, type = Formats.JSONV1),
                                    @OpenApiContent(from = Outlet.class, type = Formats.JSON)
                            })
            },
            description = "Returns CWMS Outlet Data",
            tags = {TAG}
    )
    @Override
    public void getOne(@NotNull Context ctx, @NotNull String name) {
        String office = requiredParam(ctx, OFFICE);
        try (Timer.Context ignored = markAndTime(GET_ONE)) {
            DSLContext dsl = getDslContext(ctx);
            OutletDao dao = new OutletDao(dsl);
            Outlet outlet = dao.retrieveOutlet(office, name);
            String header = ctx.header(Header.ACCEPT);
            String formatHeader = header != null ? header : Formats.JSONV1;
            ContentType contentType = Formats.parseHeader(formatHeader, Outlet.class);
            ctx.contentType(contentType.toString());
            String serialized = Formats.format(contentType, outlet);
            ctx.result(serialized);
            ctx.status(HttpServletResponse.SC_OK);
            requestResultSize.update(serialized.length());
        }
    }

    @OpenApi(
            pathParams = {
                    @OpenApiParam(name = NAME, required = true, description = "Specifies the location-id of "
                            + "the outlet to be renamed."),
            },
            queryParams = {
                    @OpenApiParam(name = OFFICE, required = true, description = "Specifies the owning office of "
                            + "the outlet to be renamed."),
                    @OpenApiParam(name = NAME, required = true, description = "Specifies the new outlet location-id."),
            },
            description = "Rename CWMS Outlet",
            method = HttpMethod.PATCH,
            tags = {TAG},
            responses = {
                    @OpenApiResponse(status = STATUS_204, description = "CWMS Outlet successfully renamed.")
            }
    )
    @Override
    public void update(@NotNull Context ctx, @NotNull String name) {
        String office = requiredParam(ctx, OFFICE);
        String newOutletId = requiredParam(ctx, NAME);
        try (Timer.Context ignored = markAndTime(DELETE)) {
            DSLContext dsl = getDslContext(ctx);
            OutletDao dao = new OutletDao(dsl);
            dao.renameOutlet(office, name, newOutletId);
            ctx.status(HttpServletResponse.SC_NO_CONTENT).json(name + " successfully renamed to " + newOutletId);
        }
    }

    @OpenApi(
            pathParams = {
                    @OpenApiParam(name = NAME, description = "Specifies the location-id of the outlet to be" +
                            " deleted."),
            },
            queryParams = {
                    @OpenApiParam(name = OFFICE, required = true, description = "Specifies the owning office of "
                            + "the outlet to be deleted."),
                    @OpenApiParam(name = METHOD, description = "Specifies the delete method used. " +
                            "Defaults to \"DELETE_KEY\"",
                            type = JooqDao.DeleteMethod.class)
            },
            description = "Delete CWMS Outlet",
            method = HttpMethod.DELETE,
            tags = {TAG},
            responses = {
                    @OpenApiResponse(status = STATUS_204, description = "Outlet successfully deleted from CWMS."),
                    @OpenApiResponse(status = STATUS_404, description = "Based on the combination of "
                            + "inputs provided the outlet was not found.")
            }
    )
    @Override
    public void delete(@NotNull Context ctx, @NotNull String name) {
        String office = requiredParam(ctx, OFFICE);
        JooqDao.DeleteMethod deleteMethod = queryParamAsClass(ctx, JooqDao.DeleteMethod.class,
                                                              JooqDao.DeleteMethod.DELETE_KEY, METHOD);
        try (Timer.Context ignored = markAndTime(DELETE)) {
            DSLContext dsl = getDslContext(ctx);
            OutletDao dao = new OutletDao(dsl);
            dao.deleteOutlet(office, name, deleteMethod.getRule());
            ctx.status(HttpServletResponse.SC_NO_CONTENT).json(name + " Deleted");
        }
    }
}
