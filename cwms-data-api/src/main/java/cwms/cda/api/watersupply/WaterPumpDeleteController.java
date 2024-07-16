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
import static cwms.cda.data.dao.JooqDao.getDslContext;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.cda.api.Controllers;
import cwms.cda.api.errors.CdaError;
import cwms.cda.data.dao.watersupply.WaterContractDao;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.watersupply.WaterUserContract;
import io.javalin.http.Context;
import io.javalin.http.Handler;
import io.javalin.plugin.openapi.annotations.HttpMethod;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.servlet.http.HttpServletResponse;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;


public class WaterPumpDeleteController implements Handler {
    private static final Logger LOGGER = Logger.getLogger(WaterPumpDeleteController.class.getName());
    public static final String TAG = "Water Contracts";
    private static final String CONTRACT_ID = "contract-id";
    private static final String PUMP_TYPE = "pump-type";
    private final MetricRegistry metrics;

    private Timer.Context markAndTime(String subject) {
        return Controllers.markAndTime(metrics, getClass().getName(), subject);
    }

    public WaterPumpDeleteController(MetricRegistry metrics) {
        this.metrics = metrics;
    }

    @NotNull
    protected WaterContractDao getContractDao(DSLContext dsl) {
        return new WaterContractDao(dsl);
    }

    @OpenApi(
        queryParams = {
            @OpenApiParam(name = PUMP_TYPE, required = true,
                    description = "The type of pump to be removed from the contract."
                            + " Expected values: IN, OUT, OUT BELOW"),
            @OpenApiParam(name = DELETE, type = boolean.class, required = true,
                    description = "Whether to delete the associated accounting data.")
        },
        pathParams = {
            @OpenApiParam(name = NAME, description = "The name of the pump to be "
                    + "removed from the specified contract.", required = true),
            @OpenApiParam(name = OFFICE, description = "The office the project is associated with.", required = true),
            @OpenApiParam(name = PROJECT_ID, description = "The name of the project.", required = true),
            @OpenApiParam(name = CONTRACT_ID, description = "The name of the contract the pump is associated with.",
                    required = true),
            @OpenApiParam(name = WATER_USER, description = "The name of the water user the contract "
                    + "is associated with.", required = true)
        },
        responses = {
            @OpenApiResponse(status = "404", description = "The provided combination of parameters"
                    + " did not find any contracts."),
            @OpenApiResponse(status = "501", description = "Requested format is not implemented.")
        },
        description = "Delete a pump from a contract",
        path = "/projects/{office}/{project-id}/water-user/{water-user}/contracts/{contract-id}/pumps/{name}",
        method = HttpMethod.DELETE,
        tags = {TAG}
    )

    @Override
    public void handle(@NotNull Context ctx) {
        try (Timer.Context ignored = markAndTime(DELETE)) {
            DSLContext dsl = getDslContext(ctx);
            boolean deleteAccounting = Boolean.parseBoolean(ctx.queryParam(DELETE));
            String officeId = ctx.pathParam(OFFICE);
            String projectName = ctx.pathParam(PROJECT_ID);
            String entityName = ctx.pathParam(WATER_USER);
            String pumpType = ctx.queryParam(PUMP_TYPE);
            String contractName = ctx.pathParam(CONTRACT_ID);
            assert pumpType != null;
            WaterContractDao contractDao = getContractDao(dsl);
            CwmsId projectLocation = new CwmsId.Builder().withName(projectName).withOfficeId(officeId).build();
            List<WaterUserContract> contract = contractDao.getAllWaterContracts(projectLocation, entityName);

            if (contract.isEmpty()) {
                CdaError error = new CdaError("No contract found for the provided name.");
                LOGGER.log(Level.SEVERE, "No matching contract found.");
                ctx.status(HttpServletResponse.SC_NOT_FOUND).json(error);
                return;
            }

            for (WaterUserContract waterUserContract : contract) {
                if (waterUserContract.getContractId().getName().equals(contractName)) {
                    switch (pumpType) {
                        case "IN":
                            contractDao.removePumpFromContract(waterUserContract,
                                    waterUserContract.getPumpInLocation().getPumpId().getName(),
                                    pumpType, deleteAccounting);
                            ctx.status(HttpServletResponse.SC_NO_CONTENT);
                            return;
                        case "OUT":
                            contractDao.removePumpFromContract(waterUserContract,
                                    waterUserContract.getPumpOutLocation().getPumpId().getName(),
                                    pumpType, deleteAccounting);
                            ctx.status(HttpServletResponse.SC_NO_CONTENT);
                            return;
                        case "BELOW":
                            contractDao.removePumpFromContract(waterUserContract,
                                    waterUserContract.getPumpOutBelowLocation().getPumpId().getName(),
                                    pumpType, deleteAccounting);
                            ctx.status(HttpServletResponse.SC_NO_CONTENT);
                            return;
                        default:
                            CdaError error = new CdaError("Invalid pump type provided.");
                            LOGGER.log(Level.SEVERE, "Invalid pump type provided.");
                            ctx.status(HttpServletResponse.SC_NOT_FOUND).json(error);
                            return;
                    }

                }
            }
            CdaError error = new CdaError("No contract found for the provided name.");
            LOGGER.log(Level.SEVERE, "No matching contract found.");
            ctx.status(HttpServletResponse.SC_NOT_FOUND).json(error);
        }
    }



}
