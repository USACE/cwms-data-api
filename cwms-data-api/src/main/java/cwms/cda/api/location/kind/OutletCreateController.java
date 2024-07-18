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

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.cda.data.dao.location.kind.OutletDao;
import cwms.cda.data.dto.location.kind.Outlet;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import io.javalin.http.Context;
import io.javalin.plugin.openapi.annotations.HttpMethod;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiRequestBody;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import javax.servlet.http.HttpServletResponse;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;
import static cwms.cda.api.Controllers.*;
import static cwms.cda.data.dao.JooqDao.getDslContext;

public class OutletCreateController extends BaseOutletHandler {

    public OutletCreateController(MetricRegistry metrics) {
        super(metrics);
    }

    @OpenApi(
            requestBody = @OpenApiRequestBody(
                    content = {
                            @OpenApiContent(from = Outlet.class, type = Formats.JSONV1),
                            @OpenApiContent(from = Outlet.class, type = Formats.JSON)
                    },
                    required = true),
            queryParams = {
                    @OpenApiParam(name = FAIL_IF_EXISTS, type = Boolean.class,
                            description = "Create will fail if provided ID already exists. Default: true")
            },
            description = "Create CWMS Outlet",
            method = HttpMethod.POST,
            tags = {OutletController.TAG},
            responses = {
                    @OpenApiResponse(status = STATUS_204, description = "Outlet successfully stored to CWMS.")
            }
    )
    @Override
    public void handle(@NotNull Context ctx) throws Exception {
        try (Timer.Context ignored = markAndTime(CREATE)) {
            String acceptHeader = ctx.req.getContentType();
            String formatHeader = acceptHeader != null ? acceptHeader : Formats.JSONV1;
            ContentType contentType = Formats.parseHeader(formatHeader, Outlet.class);
            Outlet outlet = Formats.parseContent(contentType, ctx.body(), Outlet.class);
            outlet.validate();
            boolean failIfExists = ctx.queryParamAsClass(FAIL_IF_EXISTS, Boolean.class).getOrDefault(true);
            DSLContext dsl = getDslContext(ctx);
            OutletDao dao = new OutletDao(dsl);
            dao.storeOutlet(outlet, failIfExists);
            ctx.status(HttpServletResponse.SC_CREATED).json("Created Outlet");
        }
    }
}
