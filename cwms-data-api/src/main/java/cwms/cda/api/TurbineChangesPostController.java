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

public final class TurbineChangesPostController implements Handler {
    private static final int DEFAULT_PAGE_SIZE = 500;
    private final MetricRegistry metrics;

    private final Histogram requestResultSize;


    public TurbineChangesPostController(MetricRegistry metrics) {
        this.metrics = metrics;
        String className = this.getClass().getName();

        requestResultSize = this.metrics.histogram((name(className, RESULTS, SIZE)));
    }

    private Timer.Context markAndTime(String subject) {
        return Controllers.markAndTime(metrics, getClass().getName(), subject);
    }

    @OpenApi(
        requestBody = @OpenApiRequestBody(
            content = {
                @OpenApiContent(from = TurbineChange.class, type = Formats.JSONV1),
                @OpenApiContent(from = TurbineChange.class, type = Formats.JSON)
            },
            required = true),
        queryParams = {
            @OpenApiParam(name = OVERRIDE_PROTECTION, type = Boolean.class, description = "A flag "
                + "('True'/'False') specifying whether to delete protected data. "
                + "Default is False")
        },
        description = "Create CWMS Turbine Changes",
        method = HttpMethod.POST,
        tags = {TurbineController.TAG},
        responses = {
            @OpenApiResponse(status = STATUS_204, description = "Turbine successfully stored to CWMS."),
            @OpenApiResponse(status = STATUS_404, description = "Project Id or Turbine location Ids not found.")
        }
    )
    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    public void handle(@NotNull Context ctx) throws Exception {
        try (Timer.Context ignored = markAndTime(CREATE)) {
            String acceptHeader = ctx.req.getContentType();
            String formatHeader = acceptHeader != null ? acceptHeader : Formats.JSONV1;
            ContentType contentType = Formats.parseHeader(formatHeader, TurbineChange.class);
            List<TurbineChange> turbine = Formats.parseContentList(contentType, ctx.body(), TurbineChange.class);
            boolean overrideProtection = ctx.queryParamAsClass(OVERRIDE_PROTECTION, Boolean.class)
                .getOrDefault(false);
            DSLContext dsl = getDslContext(ctx);
            TurbineDao dao = new TurbineDao(dsl);
            dao.storeOperationalChanges(turbine, overrideProtection);
            ctx.status(HttpServletResponse.SC_CREATED).json("Created Turbine Changes");
        }
    }
}