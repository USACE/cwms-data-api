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

import static cwms.cda.api.Controllers.DELETE;
import static cwms.cda.api.Controllers.GET_ALL;
import static cwms.cda.api.Controllers.METHOD;
import static cwms.cda.api.Controllers.NAME;
import static cwms.cda.api.Controllers.OFFICE;
import static cwms.cda.api.Controllers.PROJECT_ID;
import static cwms.cda.api.Controllers.STATUS_200;
import static cwms.cda.api.Controllers.STATUS_404;
import static cwms.cda.data.dao.JooqDao.getDslContext;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.flogger.FluentLogger;
import cwms.cda.api.BaseCrudHandler;
import cwms.cda.api.errors.CdaError;
import cwms.cda.api.errors.ExceptionTraceSupport;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dao.location.kind.OutletDao;
import cwms.cda.data.dto.StatusResponse;
import cwms.cda.data.dto.location.kind.VirtualOutlet;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import io.javalin.core.util.Header;
import io.javalin.http.Context;
import io.javalin.plugin.openapi.annotations.HttpMethod;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import java.io.IOException;
import java.util.List;
import javax.servlet.http.HttpServletResponse;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;

public class VirtualOutletController extends BaseCrudHandler {
    private static final FluentLogger LOGGER = FluentLogger.forEnclosingClass();

    public VirtualOutletController(MetricRegistry metrics) {
        super(metrics);
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
            String formatHeader = ctx.header(Header.ACCEPT);
            ContentType contentType = Formats.parseHeader(formatHeader, VirtualOutlet.class);
            ctx.contentType(contentType.toString());
            String serialized = Formats.format(contentType, outlets, VirtualOutlet.class);
            ctx.status(HttpServletResponse.SC_OK);
            updateResultSize(serialized);

            byte[] bytes = serialized.getBytes();
            ctx.header(Header.CONTENT_LENGTH, String.valueOf(bytes.length));
            ctx.res.getOutputStream().write(bytes);
        } catch (IOException ex) {
            CdaError error = ExceptionTraceSupport.buildError(ctx,
                "Failed to process request to retrieve Virtual Outlets", ex);
            LOGGER.atSevere().withCause(ex).log("Failed to process request to retrieve Virtual Outlets");
            ctx.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR).json(error);
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
            String formatHeader = ctx.header(Header.ACCEPT);
            ContentType contentType = Formats.parseHeader(formatHeader, VirtualOutlet.class);
            ctx.contentType(contentType.toString());
            String serialized = Formats.format(contentType, outlet);
            ctx.status(HttpServletResponse.SC_OK);
            updateResultSize(serialized);

            byte[] bytes = serialized.getBytes();
            ctx.header(Header.CONTENT_LENGTH, String.valueOf(bytes.length));
            ctx.res.getOutputStream().write(bytes);
        } catch (IOException ex) {
            CdaError error = ExceptionTraceSupport.buildError(ctx,
                "Failed to process request to retrieve Virtual Outlet", ex);
            LOGGER.atSevere().withCause(ex).log("Failed to process request to retrieve Virtual Outlet");
            ctx.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR).json(error);
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
                    @OpenApiResponse(status = STATUS_200, description = "Virtual Outlet successfully deleted from CWMS."),
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
            StatusResponse re = new StatusResponse(office, "Virtual Outlet successfully deleted from CWMS.", name);
            ctx.status(HttpServletResponse.SC_OK).json(re);
        }
    }
}
