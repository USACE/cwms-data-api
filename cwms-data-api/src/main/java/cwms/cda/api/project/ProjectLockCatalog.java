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

package cwms.cda.api.project;

import static com.codahale.metrics.MetricRegistry.name;
import static cwms.cda.api.Controllers.APPLICATION_MASK;
import static cwms.cda.api.Controllers.GET_ALL;
import static cwms.cda.api.Controllers.OFFICE_MASK;
import static cwms.cda.api.Controllers.PROJECT_MASK;
import static cwms.cda.api.Controllers.STATUS_200;
import static cwms.cda.api.Controllers.TIME_ZONE_ID;
import static cwms.cda.api.Controllers.requiredParam;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.cda.api.Controllers;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dao.project.ProjectLockDao;
import cwms.cda.data.dto.project.ProjectLock;
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
import java.util.TimeZone;
import javax.servlet.http.HttpServletResponse;
import org.jetbrains.annotations.NotNull;

public class ProjectLockCatalog implements Handler {
    private final MetricRegistry metrics;
    private final Histogram requestResultSize;

    private Timer.Context markAndTime(String subject) {
        return Controllers.markAndTime(metrics, getClass().getName(), subject);
    }

    public ProjectLockCatalog(MetricRegistry metrics) {
        this.metrics = metrics;
        requestResultSize = this.metrics.histogram((name(ProjectLockCatalog.class, Controllers.RESULTS,
                Controllers.SIZE)));
    }


    @OpenApi(
            description = "Get a list of project locks",
            queryParams = {
                @OpenApiParam(name = PROJECT_MASK, description =
                        "Specifies the "
                                + "project mask to be used to filter the locks. "
                                + "Defaults to '*'"),
                @OpenApiParam(name = APPLICATION_MASK, description =
                        "Specifies the "
                                + "application mask to be used to filter the locks. "
                                + "Defaults to '*'"),
                @OpenApiParam(name = OFFICE_MASK, required = true, description = "Specifies"
                        + " the "
                        + "office mask to be used to filter the locks. "
                        + "Supports '*' but is typically a single office."),
                @OpenApiParam(name = TIME_ZONE_ID, description =
                        "Specifies the "
                                + "time zone id to be used to filter the locks. Defaults to "
                                + "'UTC'"),
            },
            responses = {
                @OpenApiResponse(status = STATUS_200, content = {
                    @OpenApiContent(type = Formats.JSON, from = ProjectLock.class)}
                )},
            tags = {"Project Locks"},
            path = "/project-locks/",
            method = HttpMethod.GET
    )
    @Override
    public void handle(@NotNull Context ctx) throws Exception {
        try (Timer.Context timer = markAndTime(GET_ALL)) {
            ProjectLockDao lockDao = new ProjectLockDao(JooqDao.getDslContext(ctx));
            String projMask = ctx.queryParamAsClass(PROJECT_MASK, String.class).getOrDefault("*");
            String appMask = ctx.queryParamAsClass(APPLICATION_MASK, String.class).getOrDefault(
                    "*");
            String officeMask = requiredParam(ctx, OFFICE_MASK); // They should have to limit the
            // office.
            String timeZoneId = ctx.queryParamAsClass(TIME_ZONE_ID, String.class).getOrDefault(
                    "UTC");
            TimeZone tz = TimeZone.getTimeZone(timeZoneId);
            List<ProjectLock> locks = lockDao.catLocks(projMask, appMask, tz, officeMask);
            String formatHeader = ctx.header(Header.ACCEPT);
            ContentType contentType = Formats.parseHeader(formatHeader, ProjectLock.class);
            String result = Formats.format(contentType, locks, ProjectLock.class);
            ctx.result(result).contentType(contentType.toString());
            requestResultSize.update(result.length());
            ctx.status(HttpServletResponse.SC_OK);
        }
    }


}
