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

package cwms.cda.api;

import cwms.cda.api.errors.CdaError;
import cwms.cda.data.dao.watersupply.WaterContractDao;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.watersupply.WaterUserContract;
import cwms.cda.data.dto.watersupply.WaterUserContractRef;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import io.javalin.apibuilder.CrudHandler;
import io.javalin.core.util.Header;
import io.javalin.http.Context;
import io.javalin.plugin.openapi.annotations.HttpMethod;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;

import javax.servlet.http.HttpServletResponse;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import static cwms.cda.api.Controllers.*;
import static cwms.cda.data.dao.JooqDao.getDslContext;

public class WaterContractController implements CrudHandler {
    private final Logger LOGGER = Logger.getLogger(WaterContractController.class.getName());
    public static final String TAG = "Water Contracts";
    private static final String WATER_USER = "water-user";

    @NotNull
    protected WaterContractDao getContractDao(DSLContext dsl) {
        return new WaterContractDao(dsl);
    }

    @OpenApi(
            queryParams = {

                    @OpenApiParam(name = NAME, description = "Specifies the name of the contract.", required = true),
                    @OpenApiParam(name = LOCATION_ID, description =
                            "Specifies the parent location id of the contract.", required = true),
            },
            pathParams = {
                    @OpenApiParam(name = OFFICE, description = "Specifies the"
                            + " office that the contract is associated with.", required = true),
                    @OpenApiParam(name = PROJECT_ID, description = "Specifies the project id of the contract.", required = true),
                    @OpenApiParam(name = WATER_USER, description = "Specifies the water user of the contract.", required = true),
            },
            responses = {
                @OpenApiResponse(status = STATUS_200,
                    content = {
                            @OpenApiContent(from = WaterUserContract.class, type = Formats.JSONV1),
                            @OpenApiContent(from = WaterUserContract.class, type = Formats.JSON)
                    }),
                @OpenApiResponse(status = "404", description = "The provided combination of parameters"
                        + " did not find any contracts."),
                @OpenApiResponse(status = "501", description = "Requested format is not implemented.")
            },
            description = "Return all water contracts",
            path = "/projects/{office}/{project-id}/water-users/{water-user}/contracts/{office}",
            method = HttpMethod.GET,
            tags = {TAG}
    )
    @Override
    public void getAll(@NotNull Context ctx) {
        final String office = ctx.queryParam("office");
        final String name = ctx.queryParam("name");
        final String locationId = ctx.queryParam("locationId");
        DSLContext dsl = getDslContext(ctx);
        String result;
        CwmsId projectLocation = new CwmsId.Builder().withOfficeId(office).withName(locationId).build();
        String formatHeader = ctx.header(Header.ACCEPT) != null ? ctx.header(Header.ACCEPT) : Formats.JSONV1;
        ContentType contentType = Formats.parseHeader(formatHeader, WaterUserContract.class);
        ctx.contentType(contentType.toString());
        WaterContractDao contractDao = getContractDao(dsl);
        List<WaterUserContract> contracts = contractDao.getAllWaterContracts(projectLocation, name);

        if (contracts.isEmpty()) {
            CdaError error = new CdaError("No contracts found for the provided parameters.");
            LOGGER.log(Level.SEVERE, "Error retrieving all contracts");
            ctx.status(HttpServletResponse.SC_NOT_FOUND).json(error);
            return;
        }

        result = Formats.format(contentType, contracts, WaterUserContract.class);
        ctx.result(result);
        ctx.status(HttpServletResponse.SC_OK);
    }


    @OpenApi(
        queryParams = {
            @OpenApiParam(name = OFFICE, description = "Specifies the"
                    + " office that the contract is associated with.", required = true),
            @OpenApiParam(name = NAME, description = "Specifies the name of the contract.", required = true),
            @OpenApiParam(name = LOCATION_ID, description =
                    "Specifies the parent location id of the contract.", required = true),
        },
        pathParams = {
            @OpenApiParam(name = OFFICE, description = "The office Id the contract is associated with.", required = true),
            @OpenApiParam(name = PROJECT_ID, description = "The project Id the contract is associated with.", required = true),
            @OpenApiParam(name = WATER_USER, description = "The water user the contract is associated with.", required = true)
        },
        responses = {
            @OpenApiResponse(status = STATUS_200,
                content = {
                    @OpenApiContent(from = WaterUserContract.class, type = Formats.JSONV1),
                    @OpenApiContent(from = WaterUserContract.class, type = Formats.JSON)
                }),
            @OpenApiResponse(status = "404", description = "The provided combination of parameters"
                    + " did not find any contracts."),
            @OpenApiResponse(status = "501", description = "Requested format is not implemented.")
        },
        description = "Return a specified water contract",
        path = "/projects/{office}/{project-id}/water-users/{water-user}/contracts/{office}/{project-id}",
        method = HttpMethod.GET,
        tags = {TAG}
    )

    @Override
    public void getOne(@NotNull Context ctx, @NotNull String contractName){
        final String office = ctx.queryParam("office");
        final String name = ctx.queryParam("name");
        final String locationId = ctx.queryParam("locationId");
        DSLContext dsl = getDslContext(ctx);
        String result;
        CwmsId projectLocation = new CwmsId.Builder().withOfficeId(office).withName(locationId).build();
        String formatHeader = ctx.header(Header.ACCEPT) != null ? ctx.header(Header.ACCEPT) : Formats.JSONV1;
        ContentType contentType = Formats.parseHeader(formatHeader, WaterUserContract.class);
        ctx.contentType(contentType.toString());
        WaterContractDao contractDao = getContractDao(dsl);
        List<WaterUserContract> contracts = contractDao.getAllWaterContracts(projectLocation, name);

        if (contracts.isEmpty()) {
            CdaError error = new CdaError("No contracts found for the provided parameters.");
            LOGGER.log(Level.SEVERE, "Error retrieving contracts");
            ctx.status(HttpServletResponse.SC_NOT_FOUND).json(error);
            return;
        }

        for (WaterUserContract contract : contracts) {
            if (contract.getContractId().getName().equals(contractName)) {
                contracts.clear();
                contracts.add(contract);
                result = Formats.format(contentType, contracts, WaterUserContract.class);
                ctx.result(result);
                ctx.status(HttpServletResponse.SC_OK);
                return;
            }
        }
        CdaError error = new CdaError("No contract found for the provided name.");
        LOGGER.log(Level.SEVERE, "Error retrieving contract");
        ctx.status(HttpServletResponse.SC_NOT_FOUND).json(error);
    }


    @OpenApi(
        responses = {
            @OpenApiResponse(status = "204", description = "Basin successfully stored to CWMS."),
            @OpenApiResponse(status = "501", description = "Requested format is not implemented.")
        },
        pathParams = {
            @OpenApiParam(name = OFFICE, description = "The office Id the contract is associated with.", required = true),
            @OpenApiParam(name = PROJECT_ID, description = "The project Id the contract is associated with.", required = true),
            @OpenApiParam(name = WATER_USER, description = "The water user the contract is associated with.", required = true)
        },
        description = "Create a new water contract",
        method = HttpMethod.POST,
        path = "/projects/{office}/{project-id}/water-users/{water-user}/contracts",
        tags = {TAG}
    )

    @Override
    public void create(@NotNull Context ctx){
        DSLContext dsl = getDslContext(ctx);
        String formatHeader = ctx.header(Header.ACCEPT) != null ? ctx.header(Header.ACCEPT) : Formats.JSONV1;
        ContentType contentType = Formats.parseHeader(formatHeader, WaterUserContract.class);
        ctx.contentType(contentType.toString());
        WaterUserContract waterContract = Formats.parseContent(contentType, ctx.body(), WaterUserContract.class);

        String newContractName = waterContract.getContractId().getName();
        WaterContractDao contractDao = getContractDao(dsl);
        contractDao.storeWaterContract(waterContract, true, false);
        ctx.status(HttpServletResponse.SC_CREATED).json(newContractName + " created successfully");
    }

    @OpenApi(
        queryParams = {
            @OpenApiParam(name = NAME, description = "Specifies the new name of the contract.", required = true)
        },
        pathParams = {
            @OpenApiParam(name = "contractName", description = "Specifies the name of the contract to be renamed.", required = true),
            @OpenApiParam(name = OFFICE, description = "The office Id the contract is associated with.", required = true),
            @OpenApiParam(name = PROJECT_ID, description = "The project Id the contract is associated with.", required = true),
            @OpenApiParam(name = WATER_USER, description = "The water user the contract is associated with.", required = true)
        },
        responses = {
            @OpenApiResponse(status = "404", description = "The provided combination of "
                    + "parameters did not find a contract"),
            @OpenApiResponse(status = "501", description = "Requested format is not implemented.")
        },
        description = "Renames a water contract",
        method = HttpMethod.PATCH,
        path = "/projects/{office}/{project-id}/water-users/{water-user}/contracts/{office}/{project-id}",
        tags = {TAG}
    )

    @Override
    public void update(@NotNull Context ctx, @NotNull String contractName){
        DSLContext dsl = getDslContext(ctx);
        String formatHeader = ctx.header(Header.ACCEPT) != null ? ctx.header(Header.ACCEPT) : Formats.JSONV1;
        ContentType contentType = Formats.parseHeader(formatHeader, WaterUserContract.class);
        ctx.contentType(contentType.toString());
        String newName = ctx.queryParam(NAME);
        WaterUserContract waterContract = Formats.parseContent(contentType, ctx.body(), WaterUserContract.class);
        WaterContractDao contractDao = getContractDao(dsl);
        WaterUserContractRef ref = new WaterUserContractRef(waterContract.getWaterUser(),
                waterContract.getContractId().getName());
        contractDao.renameWaterContract(ref, contractName, newName);
        ctx.status(HttpServletResponse.SC_OK).json("Contract renamed successfully");
    }


    @OpenApi(
        queryParams = {
            @OpenApiParam(name = OFFICE, description = "Specifies the"
                    + " office that the contract is associated with.", required = true),
            @OpenApiParam(name = LOCATION_ID, description =
                    "Specifies the parent location for the contract.", required = true),
            @OpenApiParam(name = DELETE_MODE, description = "Specifies the delete method used."),
        },
        pathParams = {
            @OpenApiParam(name = NAME, description = "The name of the contract to be deleted."),
            @OpenApiParam(name = OFFICE, description = "The office Id the contract is associated with.", required = true),
            @OpenApiParam(name = PROJECT_ID, description = "The project Id the contract is associated with.", required = true),
            @OpenApiParam(name = WATER_USER, description = "The water user the contract is associated with.", required = true)
        },
        responses = {
            @OpenApiResponse(status = "404", description = "The provided combination of parameters"
                    + " did not find any contracts."),
            @OpenApiResponse(status = "501", description = "Requested format is not implemented.")
        },
        description = "Delete a specified water contract",
        path = "/projects/{office}/{project-id}/water-users/{water-user}/contracts/{office}/{project-id}",
        method = HttpMethod.DELETE,
        tags = {TAG}
    )

    @Override
    public void delete(@NotNull Context ctx, @NotNull String contractName){

        DSLContext dsl = getDslContext(ctx);
        String deleteMethod = ctx.queryParam(DELETE_MODE);
        String locationId = ctx.queryParam(LOCATION_ID);
        String office = ctx.queryParam(OFFICE);
        WaterContractDao contractDao = getContractDao(dsl);
        CwmsId projectLocation = new CwmsId.Builder().withOfficeId(office).withName(locationId).build();

        List<WaterUserContract> retContracts = contractDao.getAllWaterContracts(projectLocation, contractName);

        if (retContracts.isEmpty()) {
            CdaError error = new CdaError("No contract found for the provided parameters.");
            LOGGER.log(Level.SEVERE, "Error retrieving contracts");
            ctx.status(HttpServletResponse.SC_NOT_FOUND).json(error);
            return;
        }

        for (WaterUserContract contract : retContracts) {
            if (contract.getContractId().getName().equals(contractName)) {
                contractDao.deleteWaterContract(contract, deleteMethod);
                ctx.status(HttpServletResponse.SC_NO_CONTENT).json(contractName + " deleted successfully");
                return;
            }
        }
        CdaError error = new CdaError("No contract found for the provided name.");
        LOGGER.log(Level.SEVERE, "No matching contract found for deletion.");
        ctx.status(HttpServletResponse.SC_NOT_FOUND).json(error);
    }
}
