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
import static cwms.cda.api.Controllers.APPLICATION_ID;
import static cwms.cda.api.Controllers.GET_ONE;
import static cwms.cda.api.Controllers.NAME;
import static cwms.cda.api.Controllers.OFFICE;
import static cwms.cda.api.Controllers.STATUS_200;
import static cwms.cda.api.Controllers.STATUS_404;
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
import javax.servlet.http.HttpServletResponse;
import org.jetbrains.annotations.NotNull;

public class ProjectLockGetOne implements Handler {
    public static final String TAGS = "Project Locks";

    private final MetricRegistry metrics;
    private final Histogram requestResultSize;

    private Timer.Context markAndTime(String subject) {
        return Controllers.markAndTime(metrics, getClass().getName(), subject);
    }

    public ProjectLockGetOne(MetricRegistry metrics) {
        this.metrics = metrics;
        requestResultSize = this.metrics.histogram((
                name(ProjectLockGetOne.class, Controllers.RESULTS, Controllers.SIZE)));
    }

    @OpenApi(
            description = "Return a lock if the specified project is locked. Otherwise 404",
            queryParams = {
                @OpenApiParam(name = OFFICE, required = true,
                        description = "The office id."),
                @OpenApiParam(name = APPLICATION_ID, required = true,
                        description = "The application-id"),
            },
            pathParams = {
                @OpenApiParam(name = NAME, required = true,
                        description = "The id of the project."),
            },
            responses = {
                @OpenApiResponse(status = STATUS_200, content = {
                    @OpenApiContent(type = Formats.JSON, from = ProjectLock.class)}
                ),
                @OpenApiResponse(status = STATUS_404, description = "No matching Lock was found.")
            },
            tags = {TAGS},
            method = HttpMethod.GET
    )
    @Override
    public void handle(@NotNull Context ctx) throws Exception {
        String prjId = ctx.pathParam(NAME);

        String office = requiredParam(ctx, OFFICE);
        String appId = requiredParam(ctx, APPLICATION_ID);

        try (final Timer.Context ignored = markAndTime(GET_ONE)) {
            ProjectLockDao lockDao = new ProjectLockDao(JooqDao.getDslContext(ctx));
            ProjectLock lock = lockDao.retrieveLock(office, prjId, appId);
            if(lock != null) {
                String acceptHeader = ctx.header(Header.ACCEPT);
                ContentType acceptType = Formats.parseHeader(acceptHeader, ProjectLock.class);
                String result = Formats.format(acceptType, lock);
                ctx.result(result);
                ctx.contentType(acceptType.toString());
                requestResultSize.update(result.length());
                ctx.status(HttpServletResponse.SC_OK);
            } else {
                ctx.status(HttpServletResponse.SC_NOT_FOUND);
            }
        }
    }

}
