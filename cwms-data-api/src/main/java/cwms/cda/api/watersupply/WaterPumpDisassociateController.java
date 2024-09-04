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
import static cwms.cda.api.Controllers.NAME;
import static cwms.cda.api.Controllers.OFFICE;
import static cwms.cda.api.Controllers.PROJECT_ID;
import static cwms.cda.api.Controllers.WATER_USER;
import static cwms.cda.api.Controllers.requiredParamAs;
import static cwms.cda.data.dao.JooqDao.getDslContext;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.cda.data.dao.watersupply.WaterContractDao;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.watersupply.PumpType;
import cwms.cda.data.dto.watersupply.WaterUserContract;
import io.javalin.core.validation.JavalinValidation;
import io.javalin.http.Context;
import io.javalin.http.Handler;
import io.javalin.plugin.openapi.annotations.HttpMethod;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import javax.servlet.http.HttpServletResponse;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;


public final class WaterPumpDisassociateController extends WaterSupplyControllerBase implements Handler {
    private static final String PUMP_TYPE = "pump-type";

    public WaterPumpDisassociateController(MetricRegistry metrics) {
        waterMetrics(metrics);
    }

    @OpenApi(
        queryParams = {
            @OpenApiParam(name = PUMP_TYPE, required = true, type = PumpType.class,
                    description = "The type of pump to be disassociated from the contract."
                            + " Expected values: IN, OUT, OUT BELOW"),
            @OpenApiParam(name = METHOD, type = boolean.class,
                    description = "Whether to delete the associated accounting data. Defaults to FALSE.")
        },
        pathParams = {
            @OpenApiParam(name = NAME, description = "The name of the pump to be "
                    + "disassociated from the specified contract.", required = true),
            @OpenApiParam(name = OFFICE, description = "The office the project is associated with.", required = true),
            @OpenApiParam(name = PROJECT_ID, description = "The name of the project.", required = true),
            @OpenApiParam(name = CONTRACT_NAME, description = "The name of the contract the pump is associated with.",
                    required = true),
            @OpenApiParam(name = WATER_USER, description = "The name of the water user the contract "
                    + "is associated with.", required = true)
        },
        description = "Disassociate a pump from a contract",
        path = "/projects/{office}/{project-id}/water-user/{water-user}/contracts/{contract-name}/pumps/{name}",
        method = HttpMethod.DELETE,
        tags = {TAG}
    )

    @Override
    public void handle(@NotNull Context ctx) {
        try (Timer.Context ignored = markAndTime(DELETE)) {
            DSLContext dsl = getDslContext(ctx);
            boolean deleteAccounting = ctx.queryParamAsClass(METHOD, boolean.class).getOrDefault(false);
            String officeId = ctx.pathParam(OFFICE);
            String pumpName = ctx.pathParam(NAME);
            String projectName = ctx.pathParam(PROJECT_ID);
            String entityName = ctx.pathParam(WATER_USER);
            JavalinValidation.register(PumpType.class, PumpType::valueOf);
            PumpType pumpType = requiredParamAs(ctx, PUMP_TYPE, PumpType.class);
            String contractName = ctx.pathParam(CONTRACT_NAME);
            WaterContractDao contractDao = getContractDao(dsl);
            CwmsId projectLocation = CwmsId.buildCwmsId(officeId, projectName);
            WaterUserContract contract = contractDao.getWaterContract(contractName, projectLocation, entityName);
            contractDao.removePumpFromContract(contract, pumpName, pumpType, deleteAccounting);
            ctx.status(HttpServletResponse.SC_NO_CONTENT);
        }
    }
}
