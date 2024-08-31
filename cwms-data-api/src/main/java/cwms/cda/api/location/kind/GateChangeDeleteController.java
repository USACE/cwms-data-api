/*
 * MIT License
 * Copyright (c) 2024 Hydrologic Engineering Center
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package cwms.cda.api.location.kind;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.cda.api.BaseHandler;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dao.location.kind.OutletDao;
import cwms.cda.data.dto.CwmsId;
import io.javalin.http.Context;
import io.javalin.plugin.openapi.annotations.HttpMethod;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import java.sql.Timestamp;
import javax.servlet.http.HttpServletResponse;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;
import static cwms.cda.api.Controllers.*;
import static cwms.cda.api.Controllers.GET_ALL;

public class GateChangeDeleteController extends BaseHandler {

    public GateChangeDeleteController(MetricRegistry metrics) {
        super(metrics);
    }

    @OpenApi(
            pathParams = {
                    @OpenApiParam(name = OFFICE, required = true, description = "Office id for the reservoir project " +
                            "location associated with the Gate Changes."),
                    @OpenApiParam(name = PROJECT_ID, required = true, description = "Specifies the project-id of the " +
                            "Gate Changes whose data is to be included in the response."),
            },
            queryParams = {
                    @OpenApiParam(name = BEGIN, required = true, description = "The start of the time window"),
                    @OpenApiParam(name = END, required = true, description = "The end of the time window."),
                    @OpenApiParam(name = OVERRIDE_PROTECTION, type = Boolean.class, description = "A flag "
                            + "('True'/'False') specifying whether to delete protected data. "
                            + "Default is False")
            }, responses = {
            @OpenApiResponse(status = STATUS_204, description = "Gate changes successfully deleted from CWMS."),
            @OpenApiResponse(status = STATUS_404, description = "Based on the combination of "
                    + "inputs provided the project was not found.")},
            description = "Deletes matching CWMS gate change data for a Reservoir Project.",
            tags = {OutletController.TAG},
            method = HttpMethod.DELETE
    )
    @Override
    public void handle(@NotNull Context context) throws Exception {
        String office = context.pathParam(OFFICE);
        String id = context.pathParam(PROJECT_ID);
        CwmsId projectId = new CwmsId.Builder().withName(id).withOfficeId(office).build();
        Timestamp startTime = Timestamp.from(requiredZdt(context, BEGIN).toInstant());
        Timestamp endTime = Timestamp.from(requiredZdt(context, END).toInstant());
        boolean overrideProtection = context.queryParamAsClass(OVERRIDE_PROTECTION, Boolean.class).getOrDefault(false);
        try (Timer.Context ignored = markAndTime(GET_ALL)) {
            DSLContext dsl = JooqDao.getDslContext(context);
            OutletDao dao = new OutletDao(dsl);
            dao.deleteOperationalChanges(projectId, startTime.toInstant(), endTime.toInstant(), overrideProtection);
            context.res.setStatus(HttpServletResponse.SC_NO_CONTENT);
        }
    }
}
