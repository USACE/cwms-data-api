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
import cwms.cda.data.dto.watersupply.WaterUser;
import cwms.cda.data.dto.watersupply.WaterUserContract;
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
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import static cwms.cda.api.Controllers.*;
import static cwms.cda.data.dao.JooqDao.getDslContext;

public class WaterUserController implements CrudHandler {
    private static final Logger LOGGER = Logger.getLogger(WaterUserController.class.getName());
    public static final String TAG = "Water Contracts";


    @NotNull
    protected WaterContractDao getContractDao(DSLContext dsl) {
        return new WaterContractDao(dsl);
    }

    @OpenApi(
        queryParams = {
            @OpenApiParam(name = OFFICE, description = "Specifies the"
                    + " office that the contract is associated with.", required = true),
            @OpenApiParam(name = LOCATION_ID, description = "Specifies the parent location id of the contract.", required = true)
        },
        pathParams = {
                @OpenApiParam(name = OFFICE, description = "The office Id the contract is associated with.", required = true),
                @OpenApiParam(name = PROJECT_ID, description = "The project Id the contract is associated with.", required = true)
        },
        responses = {
            @OpenApiResponse(status = STATUS_200,
                content = {
                    @OpenApiContent(type = Formats.JSONV1, from = WaterUserContract.class)
                }
            )
        },
        description = "Gets all water users.",
        method = HttpMethod.GET,
        path = "/projects/{office}/{project-id}/water-user",
        tags = {TAG}
    )

    @Override
    public void getAll(@NotNull Context ctx) {
        DSLContext dsl = getDslContext(ctx);
        String office = ctx.queryParam(OFFICE);
        String locationId = ctx.queryParam(LOCATION_ID);
        CwmsId projectLocation = new CwmsId.Builder().withOfficeId(office).withName(locationId).build();
        String result;
        String formatHeader = ctx.header(Header.ACCEPT) != null ? ctx.header(Header.ACCEPT) : Formats.JSONV1;
        ContentType contentType = Formats.parseHeader(formatHeader, WaterUserContract.class);
        ctx.contentType(contentType.toString());
        WaterContractDao contractDao = getContractDao(dsl);
        List<WaterUser> users = contractDao.getAllWaterUsers(projectLocation);

        if (users.isEmpty()) {
            CdaError error = new CdaError("No water users found for the provided parameters.");
            LOGGER.log(Level.SEVERE, "Error retrieving all water users.");
            ctx.status(HttpServletResponse.SC_NOT_FOUND).json(error);
            return;
        }

        result = Formats.format(contentType, users, WaterUserContract.class);
        ctx.result(result);
        ctx.status(HttpServletResponse.SC_OK);
    }

    @OpenApi(
        queryParams = {
            @OpenApiParam(name = OFFICE, description = "Specifies the"
                    + " office that the contract is associated with.", required = true),
            @OpenApiParam(name = LOCATION_ID, description =
                    "Specifies the parent location id of the contract.", required = true)
        },
        pathParams = {
            @OpenApiParam(name = OFFICE, description = "The office Id the contract is associated with.", required = true),
            @OpenApiParam(name = PROJECT_ID, description = "The project Id the contract is associated with.", required = true)
        },
        responses = {
            @OpenApiResponse(status = STATUS_200,
                content = {
                    @OpenApiContent(type = Formats.JSONV1, from = WaterUserContract.class)
                }
            )
        },
        description = "Gets a specified water user.",
        method = HttpMethod.GET,
        path = "/projects/{office}/{project-id}/water-user",
        tags = {TAG}
    )

    @Override
    public void getOne(@NotNull Context ctx, @NotNull String entityName) {
        String location = ctx.queryParam(LOCATION_ID);
        CwmsId projectLocation = new CwmsId.Builder().withOfficeId(ctx.queryParam(OFFICE)).withName(location).build();
        DSLContext dsl = getDslContext(ctx);
        String result;
        String formatHeader = ctx.header(Header.ACCEPT) != null ? ctx.header(Header.ACCEPT) : Formats.JSONV1;
        ContentType contentType = Formats.parseHeader(formatHeader, WaterUserContract.class);
        ctx.contentType(contentType.toString());
        WaterContractDao contractDao = getContractDao(dsl);
        WaterUser user = contractDao.getWaterUser(projectLocation, entityName);

        if (user == null) {
            CdaError error = new CdaError("No water user found for the provided parameters.");
            LOGGER.log(Level.SEVERE, "Error retrieving water user.");
            ctx.status(HttpServletResponse.SC_NOT_FOUND).json(error);
            return;
        }
        List<WaterUser> params = new ArrayList<>();
        params.add(user);
        result = Formats.format(contentType, params, WaterUserContract.class);
        ctx.result(result);
        ctx.status(HttpServletResponse.SC_OK);
    }

    @OpenApi(
        responses = {
            @OpenApiResponse(status = STATUS_204, description = "Water user successfully stored to CWMS."),
            @OpenApiResponse(status = STATUS_501, description = "Requested format is not implemented")
        },
        pathParams = {
            @OpenApiParam(name = OFFICE, description = "The office Id the contract is associated with.", required = true),
            @OpenApiParam(name = PROJECT_ID, description = "The project Id the contract is associated with.", required = true)
        },
        description = "Stores a water user to CWMS.",
        method = HttpMethod.POST,
        path = "/projects/{office}/{project-id}/water-user",
        tags = {TAG}
    )
    @Override
    public void create(@NotNull Context ctx) {
        DSLContext dsl = getDslContext(ctx);
        String formatHeader = ctx.header(Header.ACCEPT) != null ? ctx.header(Header.ACCEPT) : Formats.JSONV1;
        ContentType contentType = Formats.parseHeader(formatHeader, WaterUserContract.class);
        ctx.contentType(contentType.toString());
        WaterUser user = Formats.parseContent(contentType, ctx.body(), WaterUser.class);
        WaterContractDao contractDao = getContractDao(dsl);
        contractDao.storeWaterUser(user, true);
        ctx.status(HttpServletResponse.SC_CREATED).json(user.getEntityName() + " user created successfully.");
    }

    @OpenApi(
        queryParams = {
            @OpenApiParam(name = NAME, description = "Specifies the"
                    + " new name of the water user entity.", required = true),
            @OpenApiParam(name = OFFICE, description = "Specifies the"
                    + " office that the contract is associated with.", required = true),
            @OpenApiParam(name = LOCATION_ID, description =
                    "Specifies the parent location id of the contract.", required = true)
        },
        pathParams = {
                @OpenApiParam(name = OFFICE, description = "The office Id the contract is associated with.", required = true),
                @OpenApiParam(name = PROJECT_ID, description = "The project Id the contract is associated with.", required = true),
                @OpenApiParam(name = WATER_USER, description = "The water user the contract is associated with.", required = true)
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
    public void update(@NotNull Context ctx, @NotNull String oldName) {
        DSLContext dsl = getDslContext(ctx);
        String formatHeader = ctx.header(Header.ACCEPT) != null ? ctx.header(Header.ACCEPT) : Formats.JSONV1;
        ContentType contentType = Formats.parseHeader(formatHeader, WaterUserContract.class);
        ctx.contentType(contentType.toString());
        String newName = ctx.queryParam(NAME);
        String office = ctx.queryParam(OFFICE);
        String locationId = ctx.queryParam(LOCATION_ID);
        CwmsId location = new CwmsId.Builder().withName(locationId).withOfficeId(office).build();
        WaterContractDao contractDao = getContractDao(dsl);
        contractDao.renameWaterUser(oldName, newName, location);
        ctx.status(HttpServletResponse.SC_OK).json("Water user renamed successfully.");

    }

    @OpenApi(
        queryParams = {
            @OpenApiParam(name = OFFICE, description = "Specifies the"
                    + " office that the contract is associated with.", required = true),
            @OpenApiParam(name = LOCATION_ID, description =
                    "Specifies the parent location id of the contract.", required = true),
            @OpenApiParam(name = DELETE_MODE, description = "Specifies the delete method used."),
        },
        pathParams = {
                @OpenApiParam(name = OFFICE, description = "The office Id the contract is associated with.", required = true),
                @OpenApiParam(name = PROJECT_ID, description = "The project Id the contract is associated with.", required = true),
                @OpenApiParam(name = WATER_USER, description = "The water user the contract is associated with.", required = true)
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
    public void delete(@NotNull Context ctx, @NotNull String entityName) {
        DSLContext dsl = getDslContext(ctx);
        String office = ctx.queryParam(OFFICE);
        String locationId = ctx.queryParam(LOCATION_ID);
        String deleteMode = ctx.queryParam(DELETE_MODE);
        CwmsId location = new CwmsId.Builder().withName(locationId).withOfficeId(office).build();
        WaterContractDao contractDao = getContractDao(dsl);
        contractDao.deleteWaterUser(location, entityName, deleteMode);
        ctx.status(HttpServletResponse.SC_OK).json("Water user deleted successfully.");
    }
}
