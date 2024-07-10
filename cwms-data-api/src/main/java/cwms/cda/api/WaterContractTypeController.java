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
import cwms.cda.data.dto.LookupType;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import io.javalin.apibuilder.CrudHandler;
import io.javalin.core.util.Header;
import io.javalin.http.Context;
import io.javalin.http.HttpCode;
import io.javalin.plugin.openapi.annotations.HttpMethod;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;
import usace.cwms.db.dao.ifc.watersupply.WaterUserContractType;

import javax.servlet.http.HttpServletResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import static cwms.cda.api.Controllers.*;
import static cwms.cda.data.dao.JooqDao.getDslContext;

public class WaterContractTypeController implements CrudHandler {
    private static final Logger LOGGER = Logger.getLogger(WaterContractTypeController.class.getName());
    public static final String TAG = "Water Contracts";


    @NotNull
    protected WaterContractDao getContractDao(DSLContext dsl) {
        return new WaterContractDao(dsl);
    }

    @OpenApi (
        queryParams = {
                @OpenApiParam(name = OFFICE, required = true,
                        description = "The office Id the contract is associated with.")
        },
        pathParams = {
                @OpenApiParam(name = OFFICE, description = "The office Id the contract is associated with.", required = true),
                @OpenApiParam(name = PROJECT_ID, description = "The project Id the contract is associated with.", required = true),
                @OpenApiParam(name = WATER_USER, description = "The water user the contract is associated with.", required = true)
        },
        responses = {
            @OpenApiResponse(status = "200", content = {
                    @OpenApiContent(from = WaterUserContractType.class, type = Formats.JSONV1),
                    @OpenApiContent(from = WaterUserContractType.class, type = Formats.JSON)
            }),
            @OpenApiResponse(status = "404", description = "The provided combination of parameters"
                    + " did not find any contracts."),
            @OpenApiResponse(status = "501", description = "Requested format is not implemented.")
        },
        description = "Get all water contract types",
        path = "/projects/{office}/{project-id}/water-users/{water-user}/contracts/{office}/{project-id}/types",
        method = HttpMethod.GET,
        tags = {TAG}
    )

    @Override
    public void getAll(@NotNull Context ctx){
        String officeId = ctx.queryParam(OFFICE);
        DSLContext dsl = getDslContext(ctx);
        String result;
        String formatHeader = ctx.header(Header.ACCEPT) != null ? ctx.header(Header.ACCEPT) : Formats.JSONV1;
        ContentType contentType = Formats.parseHeader(formatHeader, LookupType.class);
        ctx.contentType(contentType.toString());
        WaterContractDao dao = getContractDao(dsl);
        List<LookupType> typeList = dao.getAllWaterContractTypes(officeId);

        if (typeList.isEmpty()) {
            CdaError error = new CdaError("No contract types found for office: " + officeId);
            LOGGER.log(Level.SEVERE, "Error retrieving contract types");
            ctx.status(HttpServletResponse.SC_NOT_FOUND).json(error);
            return;
        }

        result = Formats.format(contentType, typeList, LookupType.class);
        ctx.result(result);
        ctx.status(HttpServletResponse.SC_OK);
    }

    @OpenApi(
        pathParams = {
            @OpenApiParam(name = OFFICE, description = "The office Id the contract is associated with.", required = true),
            @OpenApiParam(name = PROJECT_ID, description = "The project Id the contract is associated with.", required = true),
            @OpenApiParam(name = WATER_USER, description = "The water user the contract is associated with.", required = true)
        },
        responses = {
            @OpenApiResponse(status = "204", description = "Contract type successfully stored to CWMS."),
            @OpenApiResponse(status = "501", description = "Requested format is not implemented.")
        },
        description = "Create a new water contract type",
        method = HttpMethod.POST,
        path = "/projects/{office}/{project-id}/water-users/{water-user}/contracts/{office}/{project-id}/types",
        tags = {TAG}
    )

    @Override
    public void create(@NotNull Context ctx){
        DSLContext dsl = getDslContext(ctx);
        String formatHeader = ctx.header(Header.ACCEPT) != null ? ctx.header(Header.ACCEPT) : Formats.JSONV1;
        ContentType contentType = Formats.parseHeader(formatHeader, LookupType.class);
        ctx.contentType(contentType.toString());
        LookupType contractType = Formats.parseContent(contentType, ctx.body(), LookupType.class);
        List<LookupType> types = new ArrayList<>();
        types.add(contractType);
        WaterContractDao contractDao = getContractDao(dsl);
        contractDao.storeWaterContractTypes(types, true);
        ctx.status(HttpServletResponse.SC_CREATED).json("Contract type successfully stored to CWMS.");
    }

    @OpenApi(ignore = true)
    @Override
    public void getOne(@NotNull Context ctx, @NotNull String id){
        ctx.status(HttpCode.NOT_IMPLEMENTED).json(CdaError.notImplemented());
    }

    @OpenApi(ignore = true)
    @Override
    public void update(@NotNull Context ctx, @NotNull String oldName){
        ctx.status(HttpCode.NOT_IMPLEMENTED).json(CdaError.notImplemented());
    }

    @OpenApi(ignore = true)
    @Override
    public void delete(@NotNull Context ctx, @NotNull String id){
        ctx.status(HttpCode.NOT_IMPLEMENTED).json(CdaError.notImplemented());
    }


}
