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

import static cwms.cda.api.Controllers.*;
import static cwms.cda.api.Controllers.DELETE_MODE;
import static cwms.cda.data.dao.JooqDao.getDslContext;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.cda.api.Controllers;
import cwms.cda.data.dao.watersupply.WaterContractDao;
import cwms.cda.data.dto.CwmsId;
import io.javalin.http.Context;
import io.javalin.http.Handler;
import io.javalin.plugin.openapi.annotations.HttpMethod;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import javax.servlet.http.HttpServletResponse;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;


public class WaterUserDeleteController implements Handler {
    public static final String TAG = "Water Contracts";
    private final MetricRegistry metrics;

    private Timer.Context markAndTime(String subject) {
        return Controllers.markAndTime(metrics, getClass().getName(), subject);
    }

    public WaterUserDeleteController(MetricRegistry metrics) {
        this.metrics = metrics;
    }

    @NotNull
    protected WaterContractDao getContractDao(DSLContext dsl) {
        return new WaterContractDao(dsl);
    }

    @OpenApi(
        queryParams = {
            @OpenApiParam(name = DELETE_MODE, description = "Specifies the delete method used."),
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
            @OpenApiResponse(status = STATUS_204, description = "Water user successfully deleted from CWMS."),
            @OpenApiResponse(status = STATUS_501, description = "Requested format is not implemented")
        },
        description = "Deletes a water user from CWMS.",
        method = HttpMethod.DELETE,
        path = "/projects/{office}/{project-id}/water-user/{water-user}",
        tags = {TAG}
    )
    @Override
    public void handle(@NotNull Context ctx) {
        try (Timer.Context ignored = markAndTime(DELETE)) {
            DSLContext dsl = getDslContext(ctx);
            String office = ctx.pathParam(OFFICE);
            String locationId = ctx.pathParam(PROJECT_ID);
            String deleteMode = ctx.queryParam(DELETE_MODE);
            String entityName = ctx.pathParam(WATER_USER);
            CwmsId location = new CwmsId.Builder().withName(locationId).withOfficeId(office).build();
            WaterContractDao contractDao = getContractDao(dsl);
            contractDao.deleteWaterUser(location, entityName, deleteMode);
            ctx.status(HttpServletResponse.SC_NO_CONTENT).json("Water user deleted successfully.");
        }
    }
}
