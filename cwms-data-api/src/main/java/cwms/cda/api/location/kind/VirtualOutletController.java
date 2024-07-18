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
import cwms.cda.data.dto.location.kind.VirtualOutlet;
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
import java.util.List;
import javax.servlet.http.HttpServletResponse;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;
import static com.codahale.metrics.MetricRegistry.name;
import static cwms.cda.api.Controllers.*;
import static cwms.cda.data.dao.JooqDao.getDslContext;

public class VirtualOutletController implements CrudHandler {

    private final MetricRegistry metrics;
    private final Histogram requestResultSize;

    public VirtualOutletController(MetricRegistry metrics) {
        this.metrics = metrics;
        String className = this.getClass().getName();

        requestResultSize = this.metrics.histogram(name(className, RESULTS, SIZE));
    }

    private Timer.Context markAndTime(String subject) {
        return Controllers.markAndTime(metrics, getClass().getName(), subject);
    }


    @OpenApi(ignore = true)
    @Override
    public void create(@NotNull Context ctx) {
        //Implemented in VirtualOutletCreateController
        ctx.status(HttpServletResponse.SC_NOT_IMPLEMENTED).json(CdaError.notImplemented());
    }

    @OpenApi(
            pathParams = {
                    @OpenApiParam(name = OFFICE, description = "Office id for the reservoir project location " +
                            "associated with the virtual outlets.  Defaults to the user session id."),
                    @OpenApiParam(name = PROJECT_ID, required = true, description = "Specifies the project-id of the " +
                            "virtual outlets whose data is to be included in the response."),
            },
            responses = {
                    @OpenApiResponse(status = STATUS_200, content = {
                            @OpenApiContent(from = VirtualOutlet.class, isArray = true, type = Formats.JSONV1),
                            @OpenApiContent(from = VirtualOutlet.class, isArray = true, type = Formats.JSON)
                    })
            },
            description = "Returns matching CWMS Virtual Outlet Data for a Reservoir Project.",
            tags = {OutletController.TAG}
    )
    @Override
    public void getAll(@NotNull Context ctx) {
        String office = ctx.pathParam(OFFICE);
        String projectId = ctx.pathParam(PROJECT_ID);
        try (Timer.Context ignored = markAndTime(GET_ALL)) {
            DSLContext dsl = getDslContext(ctx);
            OutletDao dao = new OutletDao(dsl);
            List<VirtualOutlet> outlets = dao.retrieveVirtualOutletsForProject(office, projectId);
            String formatHeader = ctx.header(Header.ACCEPT) != null ? ctx.header(Header.ACCEPT) :
                    Formats.JSONV1;
            ContentType contentType = Formats.parseHeader(formatHeader, VirtualOutlet.class);
            ctx.contentType(contentType.toString());
            String serialized = Formats.format(contentType, outlets, VirtualOutlet.class);
            ctx.result(serialized);
            ctx.status(HttpServletResponse.SC_OK);
            requestResultSize.update(serialized.length());
        }
    }

    @OpenApi(
            pathParams = {
                    @OpenApiParam(name = OFFICE, required = true, description = "Specifies the owning office of "
                            + "the virtual outlet to be retrieved."),
                    @OpenApiParam(name = PROJECT_ID, required = true, description = "Specifies the project-id of the " +
                            "virtual outlets whose data is to be included in the response."),
                    @OpenApiParam(name = NAME, required = true, description = "Specifies the location-id of the " +
                            "virtual outlet to be created."),
            },
            responses = {
                    @OpenApiResponse(status = STATUS_200,
                            content = {
                                    @OpenApiContent(from = VirtualOutlet.class, type = Formats.JSONV1),
                                    @OpenApiContent(from = VirtualOutlet.class, type = Formats.JSON)
                            })
            },
            description = "Returns CWMS Virtual Outlet Data",
            tags = {OutletController.TAG}
    )
    @Override
    public void getOne(@NotNull Context ctx, @NotNull String name) {
        String office = ctx.pathParam(OFFICE);
        String projectId = ctx.pathParam(PROJECT_ID);
        try (Timer.Context ignored = markAndTime(GET_ALL)) {
            DSLContext dsl = getDslContext(ctx);
            OutletDao dao = new OutletDao(dsl);
            VirtualOutlet outlet = dao.retrieveVirtualOutlet(office, projectId, name);
            String formatHeader = ctx.header(Header.ACCEPT) != null ? ctx.header(Header.ACCEPT) :
                    Formats.JSONV1;
            ContentType contentType = Formats.parseHeader(formatHeader, VirtualOutlet.class);
            ctx.contentType(contentType.toString());
            String serialized = Formats.format(contentType, outlet);
            ctx.result(serialized);
            ctx.status(HttpServletResponse.SC_OK);
            requestResultSize.update(serialized.length());
        }
    }

    @OpenApi(ignore = true)
    @Override
    public void update(@NotNull Context ctx, @NotNull String s) {
        ctx.status(HttpServletResponse.SC_NOT_IMPLEMENTED).json(CdaError.notImplemented());
    }

    @OpenApi(
            pathParams = {
                    @OpenApiParam(name = OFFICE, required = true, description = "Specifies the owning office of "
                            + "the virtual outlet to be deleted."),
                    @OpenApiParam(name = PROJECT_ID, required = true, description = "Specifies the project id of "
                            + "the virtual outlet to be deleted."),
                    @OpenApiParam(name = NAME, description = "Specifies the location id of "
                            + "the virtual outlet to be deleted"),
            },
            queryParams = {
                    @OpenApiParam(name = METHOD, description = "Specifies the delete method used. " +
                            "Defaults to \"DELETE_KEY\"",
                            type = JooqDao.DeleteMethod.class)
            },
            description = "Delete CWMS Virtual Outlet",
            method = HttpMethod.DELETE,
            tags = {OutletController.TAG},
            responses = {
                    @OpenApiResponse(status = STATUS_204, description = "Virtual Outlet successfully deleted from CWMS."),
                    @OpenApiResponse(status = STATUS_404, description = "Based on the combination of "
                            + "inputs provided the virtual outlet was not found.")
            }
    )
    @Override
    public void delete(@NotNull Context ctx, @NotNull String name) {
        String office = ctx.pathParam(OFFICE);
        String projectId = ctx.pathParam(PROJECT_ID);
        JooqDao.DeleteMethod deleteMethod = ctx.queryParamAsClass(METHOD, JooqDao.DeleteMethod.class)
                                               .getOrDefault(JooqDao.DeleteMethod.DELETE_KEY);
        try (Timer.Context ignored = markAndTime(DELETE)) {
            DSLContext dsl = getDslContext(ctx);
            OutletDao dao = new OutletDao(dsl);
            dao.deleteVirtualOutlet(office, projectId, name, deleteMethod.getRule());
            ctx.status(HttpServletResponse.SC_NO_CONTENT).json(name + " Deleted");
        }
    }
}
