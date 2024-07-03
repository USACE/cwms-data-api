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

package cwms.cda.api;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.cda.api.errors.CdaError;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dao.location.kind.OutletDao;
import cwms.cda.data.dto.location.kind.VirtualOutletRecord;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import io.javalin.apibuilder.CrudHandler;
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
import static com.codahale.metrics.MetricRegistry.name;
import static cwms.cda.api.Controllers.*;
import static cwms.cda.api.Controllers.STATUS_204;
import static cwms.cda.data.dao.JooqDao.getDslContext;

public class VirtualOutletController implements CrudHandler {

    private final MetricRegistry metrics;

    public VirtualOutletController(MetricRegistry metrics) {
        this.metrics = metrics;
    }

    private Timer.Context markAndTime(String subject) {
        return Controllers.markAndTime(metrics, getClass().getName(), subject);
    }

    @OpenApi(
            requestBody = @OpenApiRequestBody(
                    content = {
                            @OpenApiContent(isArray = true, from = VirtualOutletRecord.class, type = Formats.JSONV1),
                            @OpenApiContent(isArray = true, from = VirtualOutletRecord.class, type = Formats.JSON)
                    },
                    required = true),
            queryParams = {
                    @OpenApiParam(name = PROJECT_ID, required = true, description = "Specifies the project id of "
                            + "the compound outlet to be created."),
                    @OpenApiParam(name = LOCATION_ID, required = true, description = "Specifies the location-id of the " +
                            "Compound Outlet to be created."),
                    @OpenApiParam(name = OFFICE, required = true, description = "Specifies the owning office of "
                            + "the compound outlet to be deleted."),
                    @OpenApiParam(name = FAIL_IF_EXISTS, type = Boolean.class,
                            description = "Create will fail if provided ID already exists. Default: true"),
            },
            description = "Create CWMS Compound Outlet",
            method = HttpMethod.POST,
            tags = {OutletController.TAG},
            responses = {
                    @OpenApiResponse(status = STATUS_204, description = "Compound Outlet successfully stored to CWMS.")
            }
    )
    @Override
    public void create(@NotNull Context ctx) {
        try (Timer.Context ignored = markAndTime(CREATE)) {
            String acceptHeader = ctx.req.getContentType();
            String formatHeader = acceptHeader != null ? acceptHeader : Formats.JSONV1;
            ContentType contentType = Formats.parseHeader(formatHeader, VirtualOutletRecord.class);
            List<VirtualOutletRecord> virtualOutletRecords = Formats.parseContentList(contentType, ctx.body(), VirtualOutletRecord.class);
            boolean failIfExists = ctx.queryParamAsClass(FAIL_IF_EXISTS, Boolean.class).getOrDefault(true);
            String projectId = requiredParam(ctx, PROJECT_ID);
            String compoundOutletId = requiredParam(ctx, LOCATION_ID);
            String officeId = requiredParam(ctx, OFFICE);
            DSLContext dsl = getDslContext(ctx);
            OutletDao dao = new OutletDao(dsl);
            dao.storeVirtualOutlet(projectId, compoundOutletId, officeId, virtualOutletRecords, failIfExists);
            ctx.status(HttpServletResponse.SC_CREATED).json("Created Outlet");
        }
    }

    @OpenApi(ignore = true)
    @Override
    public void getAll(@NotNull Context context) {
        context.status(HttpServletResponse.SC_NOT_IMPLEMENTED).json(CdaError.notImplemented());
    }

    @OpenApi(ignore = true)
    @Override
    public void getOne(@NotNull Context context, @NotNull String s) {
        context.status(HttpServletResponse.SC_NOT_IMPLEMENTED).json(CdaError.notImplemented());
    }

    @OpenApi(ignore = true)
    @Override
    public void update(@NotNull Context context, @NotNull String s) {
        context.status(HttpServletResponse.SC_NOT_IMPLEMENTED).json(CdaError.notImplemented());
    }

    @OpenApi(
            pathParams = {
                    @OpenApiParam(name = NAME, description = "Specifies the location id of "
                            + "the compound outlet to be deleted"),
            },
            queryParams = {
                    @OpenApiParam(name = PROJECT_ID, required = true, description = "Specifies the project id of "
                            + "the compound outlet to be deleted."),
                    @OpenApiParam(name = OFFICE, required = true, description = "Specifies the owning office of "
                            + "the compound outlet to be deleted."),
                    @OpenApiParam(name = METHOD, description = "Specifies the delete method used. " +
                            "Defaults to \"DELETE_KEY\"",
                            type = JooqDao.DeleteMethod.class)
            },
            description = "Delete CWMS Compound Outlet",
            method = HttpMethod.DELETE,
            tags = {OutletController.TAG},
            responses = {
                    @OpenApiResponse(status = STATUS_204, description = "Compound Outlet successfully deleted from CWMS."),
                    @OpenApiResponse(status = STATUS_404, description = "Based on the combination of "
                            + "inputs provided the compound outlet was not found.")
            }
    )
    @Override
    public void delete(@NotNull Context ctx, @NotNull String name) {
        String office = requiredParam(ctx, OFFICE);
        String locationId = requiredParam(ctx, NAME);
        JooqDao.DeleteMethod deleteMethod = ctx.queryParamAsClass(METHOD, JooqDao.DeleteMethod.class)
                                               .getOrDefault(JooqDao.DeleteMethod.DELETE_KEY);
        try (Timer.Context ignored = markAndTime(DELETE)) {
            DSLContext dsl = getDslContext(ctx);
            OutletDao dao = new OutletDao(dsl);
            dao.deleteVirtualOutlet(name, locationId, office, deleteMethod.getRule());
            ctx.status(HttpServletResponse.SC_NO_CONTENT).json(name + " Deleted");
        }
    }
}
