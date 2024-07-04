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
import static cwms.cda.api.Controllers.GET_ALL;
import static cwms.cda.api.Controllers.LOCATION_KIND_LIKE;
import static cwms.cda.api.Controllers.OFFICE;
import static cwms.cda.api.Controllers.PROJECT_LIKE;
import static cwms.cda.api.Controllers.STATUS_200;
import static cwms.cda.api.Controllers.requiredParam;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.cda.api.Controllers;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dao.project.ProjectChildLocationDao;
import cwms.cda.data.dto.project.ProjectChildLocations;
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
import javax.servlet.http.HttpServletResponse;
import org.jetbrains.annotations.NotNull;


public class ProjectChildLocationHandler implements Handler {
    public static final String TAGS = "Projects";
    public static final String PATH = "/projects/child-locations/";
    private final MetricRegistry metrics;
    private final Histogram requestResultSize;

    private Timer.Context markAndTime(String subject) {
        return Controllers.markAndTime(metrics, getClass().getName(), subject);
    }

    public ProjectChildLocationHandler(MetricRegistry metrics) {
        this.metrics = metrics;
        requestResultSize = this.metrics.histogram((name(ProjectChildLocationHandler.class, Controllers.RESULTS,
                Controllers.SIZE)));
    }


    @OpenApi(
            description = "Get a list of project child locations",
            queryParams = {
                @OpenApiParam(name = OFFICE, required = true),
                @OpenApiParam(name = PROJECT_LIKE,  description = "Posix <a href=\"regexp.html\">regular expression</a> matching "
                        + "against the project_id."),
                @OpenApiParam(name = LOCATION_KIND_LIKE, description = "Posix <a "
                        + "href=\"regexp.html\">regular expression</a> matching against the "
                        + "location kind.  The pattern will be matched against "
                        + "the valid location-kinds for Project child locations:"
                        + "{\"EMBANKMENT\", \"TURBINE\", \"OUTLET\", \"LOCK\", \"GATE\"}. "
                        + "Multiple kinds can be matched by using Regular Expression "
                        + "OR clauses. For example: \"(TURBINE|OUTLET)\"")
            },
            responses = {
                @OpenApiResponse(status = STATUS_200, content = {
                    @OpenApiContent(type = Formats.JSON, from = ProjectChildLocations.class, isArray = true)})
            },
            tags = {TAGS},
            path = PATH,
            method = HttpMethod.GET
    )
    @Override
    public void handle(@NotNull Context ctx) throws Exception {
        String office = requiredParam(ctx, OFFICE);
        try (Timer.Context ignored = markAndTime(GET_ALL)) {
            ProjectChildLocationDao lockDao = new ProjectChildLocationDao(JooqDao.getDslContext(ctx));
            String projLike = ctx.queryParamAsClass(PROJECT_LIKE, String.class).getOrDefault(null);
            String kindLike = ctx.queryParamAsClass(LOCATION_KIND_LIKE, String.class).getOrDefault(null);

            List<ProjectChildLocations> childLocations = lockDao.retrieveProjectChildLocations(office, projLike, kindLike);
            String formatHeader = ctx.header(Header.ACCEPT);
            ContentType contentType = Formats.parseHeader(formatHeader, ProjectChildLocations.class);
            String result = Formats.format(contentType, childLocations, ProjectChildLocations.class);
            ctx.result(result).contentType(contentType.toString());
            requestResultSize.update(result.length());
            ctx.status(HttpServletResponse.SC_OK);
        }
    }


}
