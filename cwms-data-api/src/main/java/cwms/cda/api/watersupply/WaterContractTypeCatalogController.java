/*
 *
 * MIT License
 *
 * Copyright (c) 2024 Hydrologic Engineering Center
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
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
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE
 * SOFTWARE.
 */

package cwms.cda.api.watersupply;

import static cwms.cda.api.Controllers.GET_ALL;
import static cwms.cda.api.Controllers.OFFICE;
import static cwms.cda.api.Controllers.PROJECT_ID;
import static cwms.cda.api.Controllers.WATER_USER;
import static cwms.cda.data.dao.JooqDao.getDslContext;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.cda.data.dao.watersupply.WaterContractDao;
import cwms.cda.data.dto.LookupType;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import io.javalin.core.util.Header;
import io.javalin.http.Context;
import io.javalin.http.Handler;
import io.javalin.plugin.openapi.annotations.HttpMethod;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import java.util.List;
import javax.servlet.http.HttpServletResponse;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;


public final class WaterContractTypeCatalogController extends WaterSupplyControllerBase implements Handler {

    public WaterContractTypeCatalogController(MetricRegistry metrics) {
        waterMetrics(metrics);
    }

    @OpenApi(
        pathParams = {
            @OpenApiParam(name = OFFICE, description = "The office Id the contract is associated with.",
                    required = true),
            @OpenApiParam(name = PROJECT_ID, description = "The project Id the contract is associated with.",
                    required = true),
            @OpenApiParam(name = WATER_USER, description = "The water user the contract is associated with.",
                    required = true)
        },
        responses = {
            @OpenApiResponse(status = "200", content = {
                @OpenApiContent(from = LookupType.class, type = Formats.JSONV1),
                @OpenApiContent(from = LookupType.class, type = Formats.JSON)
            }),
            @OpenApiResponse(status = "404", description = "The provided combination of parameters"
                    + " did not find any contracts."),
            @OpenApiResponse(status = "501", description = "Requested format is not implemented.")
        },
        description = "Get all water contract types",
        path = "/projects/{office}/{project-id}/water-user/{water-user}/contracts/{contract-id}/types",
        method = HttpMethod.GET,
        tags = {TAG}
    )

    @Override
    public void handle(@NotNull Context ctx) {
        try (Timer.Context ignored = markAndTime(GET_ALL)) {
            String officeId = ctx.pathParam(OFFICE);
            DSLContext dsl = getDslContext(ctx);
            String result;
            String formatHeader = ctx.header(Header.ACCEPT) != null ? ctx.header(Header.ACCEPT) : Formats.JSONV1;
            ContentType contentType = Formats.parseHeader(formatHeader, LookupType.class);
            ctx.contentType(contentType.toString());
            WaterContractDao dao = getContractDao(dsl);
            List<LookupType> typeList = dao.getAllWaterContractTypes(officeId);
            result = Formats.format(contentType, typeList, LookupType.class);
            ctx.result(result);
            ctx.status(HttpServletResponse.SC_OK);
        }
    }

}
