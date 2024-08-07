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

import static cwms.cda.api.Controllers.NAME;
import static cwms.cda.api.Controllers.OFFICE;
import static cwms.cda.api.Controllers.PROJECT_ID;
import static cwms.cda.api.Controllers.STATUS_204;
import static cwms.cda.api.Controllers.STATUS_501;
import static cwms.cda.api.Controllers.UPDATE;
import static cwms.cda.api.Controllers.WATER_USER;
import static cwms.cda.api.Controllers.requiredParam;
import static cwms.cda.data.dao.JooqDao.getDslContext;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.cda.data.dao.watersupply.WaterContractDao;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.watersupply.WaterUser;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import io.javalin.core.util.Header;
import io.javalin.http.Context;
import io.javalin.http.Handler;
import io.javalin.plugin.openapi.annotations.HttpMethod;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiRequestBody;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import javax.servlet.http.HttpServletResponse;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;


public final class WaterUserUpdateController extends WaterSupplyControllerBase implements Handler {

    public WaterUserUpdateController(MetricRegistry metrics) {
        waterMetrics(metrics);
    }

    @OpenApi(
        requestBody = @OpenApiRequestBody(
            content = {
                @OpenApiContent(from = WaterUser.class, type = Formats.JSONV1)
            },
            required = true),
        queryParams = {
            @OpenApiParam(name = NAME, description = "Specifies the"
                    + " new name of the water user entity.", required = true),
        },
        pathParams = {
            @OpenApiParam(name = OFFICE, description = "The office Id the contract is associated with.",
                    required = true),
            @OpenApiParam(name = PROJECT_ID, description = "The project Id the contract is associated with.",
                    required = true),
            @OpenApiParam(name = WATER_USER, description = "The water user the contract is associated with.",
                    required = true)
        },
        responses = {
            @OpenApiResponse(status = STATUS_204, description = "Water user successfully updated in CWMS."),
            @OpenApiResponse(status = STATUS_501, description = "Requested format is not implemented")
        },
        description = "Updates a water user in CWMS.",
        method = HttpMethod.PATCH,
        path = "/projects/{office}/{project-id}/water-user/{water-user}",
        tags = {TAG}
    )
    @Override
    public void handle(@NotNull Context ctx) {
        try (Timer.Context ignored = markAndTime(UPDATE)) {
            DSLContext dsl = getDslContext(ctx);
            String formatHeader = ctx.header(Header.ACCEPT) != null ? ctx.header(Header.ACCEPT) : Formats.JSONV1;
            ContentType contentType = Formats.parseHeader(formatHeader, WaterUser.class);
            ctx.contentType(contentType.toString());
            String newName = requiredParam(ctx, NAME);
            String office = ctx.pathParam(OFFICE);
            String oldName = ctx.pathParam(WATER_USER);
            String locationId = ctx.pathParam(PROJECT_ID);
            CwmsId location = CwmsId.buildCwmsId(office, locationId);
            WaterContractDao contractDao = getContractDao(dsl);
            contractDao.renameWaterUser(oldName, newName, location);
            ctx.status(HttpServletResponse.SC_NO_CONTENT).json("Water user renamed successfully.");
        }

    }
}
