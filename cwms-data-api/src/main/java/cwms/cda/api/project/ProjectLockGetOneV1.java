/*
 * MIT License
 *
 * Copyright (c) 2026 Hydrologic Engineering Center
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

import static cwms.cda.api.Controllers.APPLICATION_ID;
import static cwms.cda.api.Controllers.NAME;
import static cwms.cda.api.Controllers.OFFICE;
import static cwms.cda.api.Controllers.STATUS_200;
import static cwms.cda.api.Controllers.STATUS_404;
import static cwms.cda.api.Controllers.requiredParam;

import com.codahale.metrics.MetricRegistry;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dao.project.ProjectLockDao;
import cwms.cda.data.dao.project.ProjectLockDaoV1;
import cwms.cda.data.dto.project.ProjectLockV1;
import cwms.cda.formatters.Formats;
import io.javalin.http.Context;
import io.javalin.plugin.openapi.annotations.HttpMethod;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import io.javalin.plugin.openapi.annotations.OpenApiSecurity;
import org.jetbrains.annotations.NotNull;

public final class ProjectLockGetOneV1 extends ProjectLockGetOne<ProjectLockV1> {

    public ProjectLockGetOneV1(MetricRegistry metrics) {
        super(metrics, ProjectLockGetOne.class);
    }

    @Override
    protected ProjectLockDao<ProjectLockV1> getDao(Context ctx) {
        return new ProjectLockDaoV1(JooqDao.getDslContext(ctx));
    }

    @Override
    protected Class<ProjectLockV1> lockClass() {
        return ProjectLockV1.class;
    }

    @Override
    protected String getOffice(Context ctx) {
        return requiredParam(ctx, OFFICE);
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
                    @OpenApiContent(type = Formats.JSON, from = ProjectLockV1.class)}
                ),
                @OpenApiResponse(status = STATUS_404, description = "No matching Lock was found.")
            },
            security = {
                @OpenApiSecurity(name = "gets overridden allows lock icon.")
            },
            tags = {TAGS},
            method = HttpMethod.GET
    )
    @Override
    public void handle(@NotNull Context ctx) throws Exception {
        super.handle(ctx);
    }

}
