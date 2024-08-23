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
import cwms.cda.api.BaseCrudHandler;
import cwms.cda.api.enums.UnitSystem;
import cwms.cda.api.errors.CdaError;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dao.location.kind.OutletDao;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.location.kind.GateChange;
import cwms.cda.data.dto.location.kind.Outlet;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import io.javalin.core.util.Header;
import io.javalin.http.Context;
import io.javalin.plugin.openapi.annotations.HttpMethod;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiRequestBody;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import java.sql.Timestamp;
import java.util.List;
import javax.servlet.http.HttpServletResponse;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;
import static cwms.cda.api.Controllers.*;

public class GateChangeController extends BaseCrudHandler {
    private static final int DEFAULT_PAGE_SIZE = 500;

    public GateChangeController(MetricRegistry metrics) {
        super(metrics);
    }

    @OpenApi(
            requestBody = @OpenApiRequestBody(
                    content = {
                            @OpenApiContent(from = GateChange.class, isArray = true, type = Formats.JSONV1),
                            @OpenApiContent(from = GateChange.class, isArray = true, type = Formats.JSON)
                    },
                    required = true),
            queryParams = {
                    @OpenApiParam(name = FAIL_IF_EXISTS, type = Boolean.class,
                            description = "Create will fail if provided Gate Changes already exist. Default: true")
            },
            description = "Create CWMS Gate Changes",
            method = HttpMethod.POST,
            tags = {OutletController.TAG},
            responses = {
                    @OpenApiResponse(status = STATUS_201, description = "Gate Changes successfully stored to CWMS.")
            }
    )
    @Override
    public void create(@NotNull Context context) {
        boolean failIfExists = context.queryParamAsClass(FAIL_IF_EXISTS, Boolean.class).getOrDefault(true);
        String formatHeader = context.header(Header.ACCEPT) != null ? context.header(Header.ACCEPT) : Formats.JSONV1;
        ContentType contentType = Formats.parseHeader(formatHeader, GateChange.class);
        List<GateChange> changes = Formats.parseContentList(contentType, context.body(), GateChange.class);

        try (Timer.Context ignored = markAndTime(CREATE)) {
            DSLContext dsl = JooqDao.getDslContext(context);
            OutletDao dao = new OutletDao(dsl);
            dao.storeOperationalChanges(changes, failIfExists);
            context.status(HttpServletResponse.SC_CREATED);
        }
    }

    @OpenApi(
            pathParams = {
                    @OpenApiParam(name = OFFICE, required = true, description = "Office id for the reservoir project location " +
                            "associated with the Gate Changes."),
                    @OpenApiParam(name = PROJECT_ID, required = true, description = "Specifies the project-id of the " +
                            "Gate Changes whose data is to be included in the response."),
            },
            queryParams = {
                    @OpenApiParam(name = BEGIN, required = true, description = "The start of the time window"),
                    @OpenApiParam(name = END, required = true, description = "The end of the time window."),
                    @OpenApiParam(name = START_TIME_INCLUSIVE, type = Boolean.class, description = "A flag "
                            + "specifying whether any data at the start time should be retrieved ('True') "
                            + "or only data <b><em>after</em></b> the start time ('False').  "
                            + "Default value is True"),
                    @OpenApiParam(name = END_TIME_INCLUSIVE, type = Boolean.class, description = "A flag "
                            + "('True'/'False') specifying whether any data at the end time should be "
                            + "retrieved ('True') or only data <b><em>before</em></b> the end time ('False'). "
                            + "Default value is False"),
                    @OpenApiParam(name = UNIT_SYSTEM, type = UnitSystem.class, description = "Unit System desired in response. "
                            + "Can be SI (International Scientific) or EN (Imperial.) If unspecified, "
                            + "defaults to EN."),
                    @OpenApiParam(name = PAGE_SIZE, type = Integer.class,
                            description = "the maximum number of turbine changes to retrieve, regardless of time window. " +
                                    "A positive integer is interpreted as the maximum number of changes from the " +
                                    "beginning of the time window. A negative integer is interpreted as the maximum number " +
                                    "from the end of the time window. " + "Default " + DEFAULT_PAGE_SIZE + "." +
                                    "A page cursor will not be returned by this DTO. Instead, the next page can be determined " +
                                    "by querying the next set of changes using the last returned change date and using " +
                                    "start-time-inclusive=false")
            },
            responses = {
                    @OpenApiResponse(status = STATUS_200, content = {
                            @OpenApiContent(from = GateChange.class, isArray = true, type = Formats.JSONV1)
                    })
            },
            description = "Returns matching CWMS Outlet Data for a Reservoir Project.",
            tags = {OutletController.TAG}
    )
    @Override
    public void getAll(@NotNull Context context) {
        String office = context.pathParam(OFFICE);
        String id = context.pathParam(PROJECT_ID);
        CwmsId projectId = new CwmsId.Builder().withName(id).withOfficeId(office).build();
        Timestamp startTime = Timestamp.from(requiredZdt(context, BEGIN).toInstant());
        Timestamp endTime = Timestamp.from(requiredZdt(context, END).toInstant());
        boolean startInclusive = context.queryParamAsClass(START_TIME_INCLUSIVE, Boolean.class).getOrDefault(true);
        boolean endInclusive = context.queryParamAsClass(END_TIME_INCLUSIVE, Boolean.class).getOrDefault(false);
        UnitSystem unitSystem = context.queryParamAsClass(UNIT_SYSTEM, UnitSystem.class).getOrDefault(UnitSystem.EN);
        int pageSize = context.queryParamAsClass(PAGE_SIZE, Integer.class).getOrDefault(DEFAULT_PAGE_SIZE);

        try (Timer.Context ignored = markAndTime(GET_ALL)) {
            DSLContext dsl = JooqDao.getDslContext(context);
            OutletDao dao = new OutletDao(dsl);
            List<GateChange> changes = dao.retrieveOperationalChanges(projectId, startTime.toInstant(),
                                                                      endTime.toInstant(), startInclusive, endInclusive,
                                                                      unitSystem, pageSize);
            String formatHeader = context.header(Header.ACCEPT) != null ? context.header(Header.ACCEPT) : Formats.JSONV1;
            ContentType contentType = Formats.parseHeader(formatHeader, GateChange.class);
            String serialized = Formats.format(contentType, changes, GateChange.class);
            context.result(serialized);
            context.status(HttpServletResponse.SC_OK);
            updateResultSize(serialized);
        }
    }

    @OpenApi(ignore = true)
    @Override
    public void getOne(@NotNull Context context, @NotNull String s) {
        context.status(HttpServletResponse.SC_NOT_IMPLEMENTED).json(CdaError.notImplemented());
    }

    @OpenApi(ignore = true)
    @Override
    public void update(@NotNull Context context, @NotNull String s) {
        context.status(HttpServletResponse.SC_NOT_IMPLEMENTED).json(CdaError.notImplemented());
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
                    @OpenApiResponse(status = STATUS_204, description = "Turbine successfully deleted from CWMS."),
                    @OpenApiResponse(status = STATUS_404, description = "Based on the combination of "
                            + "inputs provided the project was not found.")},
            description = "Delete CWMS Gate Change Data for a Reservoir Project.",
            tags = {OutletController.TAG},
            method = HttpMethod.DELETE
    )
    @Override
    public void delete(@NotNull Context context, @NotNull String s) {
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
        }
    }
}
