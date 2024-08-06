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

import static cwms.cda.api.Controllers.CONTRACT_NAME;
import static cwms.cda.api.Controllers.DELETE;
import static cwms.cda.api.Controllers.METHOD;
import static cwms.cda.api.Controllers.OFFICE;
import static cwms.cda.api.Controllers.PROJECT_ID;
import static cwms.cda.api.Controllers.WATER_USER;
import static cwms.cda.data.dao.JooqDao.getDslContext;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.cda.data.dao.watersupply.WaterContractDao;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.watersupply.WaterUserContract;
import io.javalin.http.Context;
import io.javalin.http.Handler;
import io.javalin.plugin.openapi.annotations.HttpMethod;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import javax.servlet.http.HttpServletResponse;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;


public final class WaterContractDeleteController extends WaterSupplyControllerBase implements Handler {
    public WaterContractDeleteController(MetricRegistry metrics) {
        waterMetrics(metrics);
    }

    @OpenApi(
        queryParams = {
            @OpenApiParam(name = METHOD, description = "Specifies the delete method used."),
        },
        pathParams = {
            @OpenApiParam(name = CONTRACT_NAME, description = "The name of the contract to be deleted."),
            @OpenApiParam(name = OFFICE, description = "The office Id the contract is associated with.",
                    required = true),
            @OpenApiParam(name = PROJECT_ID, description = "The project Id the contract is associated with.",
                    required = true),
            @OpenApiParam(name = WATER_USER, description = "The water user the contract is associated with.",
                    required = true)
        },
        responses = {
            @OpenApiResponse(status = "404", description = "The provided combination of parameters"
                    + " did not find any contracts."),
            @OpenApiResponse(status = "501", description = "Requested format is not implemented.")
        },
        description = "Delete a specified water contract",
        path = "/projects/{office}/{project-id}/water-users/{water-user}/contracts/{name}",
        method = HttpMethod.DELETE,
        tags = {TAG}
    )

    @Override
    public void handle(@NotNull Context ctx) {
        try (Timer.Context ignored = markAndTime(DELETE)) {
            DSLContext dsl = getDslContext(ctx);
            String contractName = ctx.pathParam(CONTRACT_NAME);
            String deleteMethod = ctx.queryParam(METHOD);
            String locationId = ctx.pathParam(PROJECT_ID);
            String entityName = ctx.pathParam(WATER_USER);
            String office = ctx.pathParam(OFFICE);
            WaterContractDao contractDao = getContractDao(dsl);
            CwmsId projectLocation = buildCwmsId(office, locationId);
            WaterUserContract contract = contractDao.getWaterContract(contractName, projectLocation, entityName);
            contractDao.deleteWaterContract(contract, deleteMethod);
            ctx.status(HttpServletResponse.SC_NO_CONTENT).json(contractName + " deleted successfully");
        }
    }
}
