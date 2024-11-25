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

import static cwms.cda.api.Controllers.DELETE;
import static cwms.cda.api.Controllers.OFFICE;
import static cwms.cda.data.dao.JooqDao.getDslContext;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.cda.data.dao.watersupply.WaterContractDao;
import cwms.cda.data.dto.LookupType;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import io.javalin.http.Context;
import io.javalin.http.Handler;
import io.javalin.plugin.openapi.annotations.HttpMethod;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import javax.servlet.http.HttpServletResponse;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;


public final class WaterContractTypeDeleteController extends WaterSupplyControllerBase implements Handler {

    private static final String DISPLAY_VALUE = "display-value";

    public WaterContractTypeDeleteController(MetricRegistry metrics) {
        waterMetrics(metrics);
    }

    @OpenApi(
        pathParams = {
            @OpenApiParam(name = OFFICE, required = true, description = "The office associated with "
                + "the contract type to delete"),
            @OpenApiParam(name = DISPLAY_VALUE, required = true, description = "The location associated with "
                + "the contract type to delete"),
        },
        description = "Delete a water contract type",
        method = HttpMethod.DELETE,
        path = "/projects/{office}/contract-types/{display-value}",
        tags = {TAG}
    )

    @Override
    public void handle(@NotNull Context ctx) {
        try (Timer.Context ignored = markAndTime(DELETE)) {
            DSLContext dsl = getDslContext(ctx);
            String office = ctx.pathParam(OFFICE);
            String displayValue = ctx.pathParam(DISPLAY_VALUE);
            String formatHeader = ctx.req.getContentType();
            ContentType contentType = Formats.parseHeader(formatHeader, LookupType.class);
            ctx.contentType(contentType.toString());
            WaterContractDao dao = new WaterContractDao(dsl);
            dao.deleteWaterContractType(office, displayValue);
            ctx.status(HttpServletResponse.SC_NO_CONTENT).json("Contract type successfully deleted from CWMS.");
        }
    }
}
