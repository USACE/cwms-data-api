/*
 * MIT License
 *
 * Copyright (c) 2024 Hydrologic Engineering Center
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
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
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package cwms.cda.api;

import static com.codahale.metrics.MetricRegistry.name;
import static cwms.cda.api.Controllers.BEGIN;
import static cwms.cda.api.Controllers.CREATE;
import static cwms.cda.api.Controllers.DELETE;
import static cwms.cda.api.Controllers.END;
import static cwms.cda.api.Controllers.END_TIME_INCLUSIVE;
import static cwms.cda.api.Controllers.GET_ALL;
import static cwms.cda.api.Controllers.NAME;
import static cwms.cda.api.Controllers.OFFICE;
import static cwms.cda.api.Controllers.OVERRIDE_PROTECTION;
import static cwms.cda.api.Controllers.PAGE_SIZE;
import static cwms.cda.api.Controllers.PROJECT_ID;
import static cwms.cda.api.Controllers.RESULTS;
import static cwms.cda.api.Controllers.SIZE;
import static cwms.cda.api.Controllers.START_TIME_INCLUSIVE;
import static cwms.cda.api.Controllers.STATUS_200;
import static cwms.cda.api.Controllers.STATUS_204;
import static cwms.cda.api.Controllers.STATUS_404;
import static cwms.cda.api.Controllers.UNIT_SYSTEM;
import static cwms.cda.api.Controllers.requiredInstant;
import static cwms.cda.api.Controllers.requiredParam;
import static cwms.cda.data.dao.JooqDao.getDslContext;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.cda.api.enums.UnitSystem;
import cwms.cda.api.errors.CdaError;
import cwms.cda.api.errors.RequiredQueryParameterException;
import cwms.cda.data.dao.location.kind.TurbineDao;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.location.kind.TurbineChange;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import io.javalin.apibuilder.CrudHandler;
import io.javalin.core.util.Header;
import io.javalin.http.Context;
import io.javalin.http.Handler;
import io.javalin.plugin.openapi.annotations.HttpMethod;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiRequestBody;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import java.time.Instant;
import java.util.List;
import javax.servlet.http.HttpServletResponse;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;

public final class TurbineChangesGetController implements Handler {
    private static final int DEFAULT_PAGE_SIZE = 500;
    private final MetricRegistry metrics;

    private final Histogram requestResultSize;


    public TurbineChangesGetController(MetricRegistry metrics) {
        this.metrics = metrics;
        String className = this.getClass().getName();

        requestResultSize = this.metrics.histogram((name(className, RESULTS, SIZE)));
    }

    private Timer.Context markAndTime(String subject) {
        return Controllers.markAndTime(metrics, getClass().getName(), subject);
    }

    @OpenApi(
        pathParams = {
            @OpenApiParam(name = OFFICE, description = "Office id for the reservoir project location " +
                "associated with the turbine changes."),
            @OpenApiParam(name = NAME, required = true, description = "Specifies the name of project of the " +
                "Turbine changes whose data is to be included in the response."),
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
                @OpenApiContent(isArray = true, type = Formats.JSONV1, from = TurbineChange.class)
            })
        },
        description = "Returns matching CWMS Turbine Change Data for a Reservoir Project.",
        tags = {TurbineController.TAG}
    )
    public void handle(@NotNull Context ctx) throws Exception {
        String projectId = ctx.pathParam(NAME);
        String office = ctx.pathParam(OFFICE);
        Instant begin = requiredInstant(ctx, BEGIN);
        Instant end = requiredInstant(ctx, END);
        boolean startTimeInclusive = ctx.queryParamAsClass(START_TIME_INCLUSIVE, Boolean.class)
            .getOrDefault(true);
        boolean endTimeInclusive = ctx.queryParamAsClass(END_TIME_INCLUSIVE, Boolean.class)
            .getOrDefault(false);
        UnitSystem unitSystem = ctx.queryParamAsClass(UNIT_SYSTEM, UnitSystem.class)
            .getOrDefault(UnitSystem.EN);
        int rowLimit = ctx.queryParamAsClass(PAGE_SIZE, Integer.class)
            .getOrDefault(DEFAULT_PAGE_SIZE);
        try (Timer.Context ignored = markAndTime(GET_ALL)) {
            CwmsId cwmsId = new CwmsId.Builder()
                .withName(projectId)
                .withOfficeId(office)
                .build();
            DSLContext dsl = getDslContext(ctx);
            TurbineDao dao = new TurbineDao(dsl);
            List<TurbineChange> turbineChanges = dao.retrieveOperationalChanges(cwmsId,
                begin, end, startTimeInclusive, endTimeInclusive, unitSystem.getValue(), rowLimit);
            String formatHeader = ctx.header(Header.ACCEPT);
            ContentType contentType = Formats.parseHeader(formatHeader, TurbineChange.class);
            ctx.contentType(contentType.toString());
            String serialized = Formats.format(contentType, turbineChanges, TurbineChange.class);
            ctx.result(serialized);
            ctx.status(HttpServletResponse.SC_OK);
            requestResultSize.update(serialized.length());
        }
    }
}