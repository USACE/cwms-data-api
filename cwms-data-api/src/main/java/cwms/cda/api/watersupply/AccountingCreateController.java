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
import static cwms.cda.api.Controllers.CREATE;
import static cwms.cda.api.Controllers.OFFICE;
import static cwms.cda.api.Controllers.STATUS_204;
import static cwms.cda.api.Controllers.STATUS_501;
import static cwms.cda.api.Controllers.WATER_USER;
import static cwms.cda.data.dao.JooqDao.getDslContext;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.cda.api.Controllers;
import cwms.cda.data.dao.LookupTypeDao;
import cwms.cda.data.dao.watersupply.WaterSupplyAccountingDao;
import cwms.cda.data.dto.LookupType;
import cwms.cda.data.dto.watersupply.PumpAccounting;
import cwms.cda.data.dto.watersupply.WaterSupplyAccounting;
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
import java.util.List;
import javax.servlet.http.HttpServletResponse;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;

public class AccountingCreateController implements Handler {
    private static final String TAG = "Pump Accounting";
    private final MetricRegistry metrics;

    private Timer.Context markAndTime(String subject) {
        return Controllers.markAndTime(metrics, getClass().getName(), subject);
    }

    public AccountingCreateController(MetricRegistry metrics) {
        this.metrics = metrics;
    }

    @NotNull
    protected WaterSupplyAccountingDao getWaterSupplyAccountingDao(DSLContext dsl) {
        return new WaterSupplyAccountingDao(dsl);
    }

    @OpenApi(
        requestBody = @OpenApiRequestBody(
            content = {
                @OpenApiContent(from = WaterSupplyAccounting.class, type = Formats.JSONV1)
            },
            required = true),
        pathParams = {
            @OpenApiParam(name = OFFICE, description = "The office ID the accounting is associated with.",
                    required = true),
            @OpenApiParam(name = WATER_USER, description = "The water user the accounting is associated with.",
                    required = true),
            @OpenApiParam(name = CONTRACT_NAME, description = "The name of the contract associated with the "
                    + "accounting.", required = true),
        },
        responses = {
            @OpenApiResponse(status = STATUS_204, description = "The pump accounting entry was created."),
            @OpenApiResponse(status = STATUS_501, description = "Requested format is not implemented")
        },
        description = "Create a new pump accounting entry associated with a water supply contract.",
        path = "/projects/{office}/water-user/{water-user}/contracts/{contract-name}/accounting",
        method = HttpMethod.POST,
        tags = {TAG}
    )

    @Override
    public void handle(@NotNull Context ctx) {
        try (Timer.Context ignored = markAndTime(CREATE)) {
            final String contractId = ctx.pathParam(CONTRACT_NAME);
            final String office = ctx.pathParam(OFFICE);
            DSLContext dsl = getDslContext(ctx);
            String formatHeader = ctx.header(Header.ACCEPT) != null ? ctx.header(Header.ACCEPT) : Formats.JSONV1;
            ContentType contentType = Formats.parseHeader(formatHeader, WaterSupplyAccounting.class);
            ctx.contentType(contentType.toString());
            WaterSupplyAccounting accounting = Formats.parseContent(contentType, ctx.body(),
                    WaterSupplyAccounting.class);
            WaterSupplyAccountingDao waterSupplyAccountingDao = getWaterSupplyAccountingDao(dsl);
            LookupTypeDao lookupTypeDao = new LookupTypeDao(dsl);
            List<LookupType> lookupList = lookupTypeDao
                    .retrieveLookupTypes("AT_PHYSICAL_TRANSFER_TYPE", "PHYS_TRANS_TYPE", office);

            for (PumpAccounting pumpAccounting : accounting.getPumpAccounting()) {
                if (!searchForTransferType(pumpAccounting, lookupList)) {
                    ctx.status(HttpServletResponse.SC_BAD_REQUEST).json("No matching transfer type found "
                            + "for an accounting entry.");
                    return;
                }
            }
            waterSupplyAccountingDao.storeAccounting(accounting);
            ctx.status(HttpServletResponse.SC_CREATED).json(contractId + " created successfully");
        }
    }

    private boolean searchForTransferType(PumpAccounting accounting, List<LookupType> lookupTypes) {
        for (LookupType lookupType : lookupTypes) {
            if (accounting.getTransferType().getActive() == lookupType.getActive()
                    && accounting.getTransferType().getOfficeId().equals(lookupType.getOfficeId())
                    && accounting.getTransferType().getTooltip().equals(lookupType.getTooltip())
                    && accounting.getTransferType().getDisplayValue().equals(lookupType.getDisplayValue())) {
                return true;
            }
        }
        return false;
    }
}
