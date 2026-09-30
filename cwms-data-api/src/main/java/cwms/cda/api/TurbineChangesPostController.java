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

import static cwms.cda.api.Controllers.CREATE;
import static cwms.cda.api.Controllers.NAME;
import static cwms.cda.api.Controllers.OFFICE;
import static cwms.cda.api.Controllers.OVERRIDE_PROTECTION;
import static cwms.cda.api.Controllers.STATUS_201;
import static cwms.cda.api.Controllers.STATUS_404;
import static cwms.cda.data.dao.JooqDao.getDslContext;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.cda.data.dao.location.kind.TurbineDao;
import cwms.cda.data.dto.StatusResponse;
import cwms.cda.data.dto.location.kind.TurbineChange;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import io.javalin.http.Context;
import io.javalin.openapi.OpenApi;
import io.javalin.openapi.HttpMethod;
import io.javalin.openapi.OpenApiContent;
import io.javalin.openapi.OpenApiParam;
import io.javalin.openapi.OpenApiRequestBody;
import io.javalin.openapi.OpenApiResponse;
import java.util.List;
import javax.servlet.http.HttpServletResponse;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;

public final class TurbineChangesPostController extends BaseHandler {


    public TurbineChangesPostController(MetricRegistry metrics) {
        super(metrics);
    }

    @OpenApi(
        pathParams = {
            @OpenApiParam(name = OFFICE, description = "Office id for the reservoir project location " +
                "associated with the turbine changes."),
            @OpenApiParam(name = NAME, required = true, description = "Specifies the name of project of the " +
                "Turbine changes whose data is to stored."),
        },
        requestBody = @OpenApiRequestBody(
            content = {
                @OpenApiContent(from = TurbineChange.class, type = Formats.JSONV1),
                @OpenApiContent(from = TurbineChange.class, type = Formats.JSON)
            },
            required = true),
        queryParams = {
            @OpenApiParam(name = OVERRIDE_PROTECTION, type = Boolean.class, description = "A flag "
                + "('True'/'False') specifying whether to delete protected data. "
                + "Default is False")
        },
        description = "Create CWMS Turbine Changes",
        methods = {HttpMethod.POST},
        tags = {TurbineController.TAG},
        path = "/",
        responses = {
            @OpenApiResponse(status = STATUS_201, description = "Turbine successfully stored to CWMS."),
            @OpenApiResponse(status = STATUS_404, description = "Project Id or Turbine location Ids not found.")
        }
    )
    @Override
    public void handle(@NotNull Context ctx) throws Exception {
        logUnusedPathParameter(ctx, NAME, "Body contains required information.");
        logUnusedPathParameter(ctx, OFFICE, "Body contains required information.");

        try (Timer.Context ignored = markAndTime(CREATE)) {
            String formatHeader = ctx.contentType();
            ContentType contentType = Formats.parseHeader(formatHeader, TurbineChange.class);
            List<TurbineChange> turbine = Formats.parseContentList(contentType, ctx.body(), TurbineChange.class);
            boolean overrideProtection = ctx.queryParamAsClass(OVERRIDE_PROTECTION, Boolean.class)
                .getOrDefault(false);
            DSLContext dsl = getDslContext(ctx);
            TurbineDao dao = new TurbineDao(dsl);
            dao.storeOperationalChanges(turbine, overrideProtection);
            StatusResponse re = new StatusResponse(turbine.get(0).getProjectId().getOfficeId(), "Created Turbine Changes");
            ctx.status(HttpServletResponse.SC_CREATED).json(re);
        }
    }
}