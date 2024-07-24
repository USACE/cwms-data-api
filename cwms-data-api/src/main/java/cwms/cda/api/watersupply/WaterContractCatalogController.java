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
import cwms.cda.data.dto.watersupply.WaterUser;
import cwms.cda.data.dto.watersupply.WaterUserContract;
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
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.servlet.http.HttpServletResponse;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;


public class WaterContractCatalogController implements Handler {
    private final Logger LOGGER = Logger.getLogger(WaterContractCatalogController.class.getName());
    public static final String TAG = "Water Contracts";
    private static final String WATER_USER = "water-user";
    private final MetricRegistry metrics;

    private Timer.Context markAndTime(String subject) {
        return Controllers.markAndTime(metrics, getClass().getName(), subject);
    }

    public WaterContractCatalogController(MetricRegistry metrics) {
        this.metrics = metrics;
    }

    @NotNull
    protected WaterContractDao getContractDao(DSLContext dsl) {
        return new WaterContractDao(dsl);
    }

    @OpenApi(
        pathParams = {
            @OpenApiParam(name = OFFICE, description = "Specifies the"
                    + " office that the contract is associated with.", required = true),
            @OpenApiParam(name = PROJECT_ID, description = "Specifies the project id of the contract.",
                    required = true),
            @OpenApiParam(name = WATER_USER, description = "Specifies the water user of the contract.",
                    required = true),
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
        path = "/projects/{office}/{project-id}/water-users/{water-user}/contracts",
        method = HttpMethod.GET,
        tags = {TAG}
    )
    @Override
    public void handle(@NotNull Context ctx) {
        try (Timer.Context ignored = markAndTime(GET_ALL)) {
            final String office = ctx.pathParam(OFFICE);
            final String locationId = ctx.pathParam(PROJECT_ID);
            final String waterUserEntity = ctx.pathParam(WATER_USER);
            DSLContext dsl = getDslContext(ctx);
            String result;
            CwmsId projectLocation = new CwmsId.Builder().withOfficeId(office).withName(locationId).build();
            String formatHeader = ctx.header(Header.ACCEPT) != null ? ctx.header(Header.ACCEPT) : Formats.JSONV1;
            ContentType contentType = Formats.parseHeader(formatHeader, WaterUserContract.class);
            ctx.contentType(contentType.toString());
            WaterContractDao contractDao = getContractDao(dsl);

            WaterUser waterUser = contractDao.getWaterUser(projectLocation, waterUserEntity);

            if (waterUser == null) {
                CdaError error = new CdaError("No water user found for the provided parameters.");
                LOGGER.log(Level.SEVERE, "Error retrieving water user contracts - no water user found.");
                ctx.status(HttpServletResponse.SC_NOT_FOUND).json(error);
                return;
            }

            List<WaterUserContract> contracts = contractDao.getAllWaterContracts(projectLocation, waterUserEntity);
            result = Formats.format(contentType, contracts, WaterUserContract.class);
            ctx.result(result);
            ctx.status(HttpServletResponse.SC_OK);
        }
    }
}
