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

import static cwms.cda.api.Controllers.APPLICATION_MASK;
import static cwms.cda.api.Controllers.OFFICE;
import static cwms.cda.api.Controllers.PROJECT_MASK;
import static cwms.cda.api.Controllers.STATUS_200;

import com.codahale.metrics.MetricRegistry;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dao.project.ProjectLockDao;
import cwms.cda.data.dao.project.ProjectLockDaoV2;
import cwms.cda.data.dto.project.ProjectLockV2;
import cwms.cda.formatters.Formats;
import io.javalin.http.Context;
import io.javalin.plugin.openapi.annotations.HttpMethod;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import io.javalin.plugin.openapi.annotations.OpenApiSecurity;
import org.jetbrains.annotations.NotNull;

public final class ProjectLockCatalogV2 extends ProjectLockCatalog<ProjectLockV2> {
    public static final String PATH = "/v2/project-locks/{office}/";

    public ProjectLockCatalogV2(MetricRegistry metrics) {
        super(metrics, ProjectLockCatalogV2.class);
    }

    @Override
    protected ProjectLockDao<ProjectLockV2> getDao(Context ctx) {
        return new ProjectLockDaoV2(JooqDao.getDslContext(ctx));
    }

    @Override
    protected Class<ProjectLockV2> lockClass() {
        return ProjectLockV2.class;
    }

    @Override
    protected String getOffice(Context ctx) {
        return ctx.pathParam(OFFICE);
    }

    @OpenApi(
            description = "Get a list of project locks for the given office",
            pathParams = {
                @OpenApiParam(name = OFFICE, required = true, description =
                        "Specifies the office whose locks are returned."),
            },
            queryParams = {
                @OpenApiParam(name = PROJECT_MASK, description =
                        "Specifies the "
                                + "project mask to be used to filter the locks. "
                                + "Defaults to '*'"),
                @OpenApiParam(name = APPLICATION_MASK, description =
                        "Specifies the "
                                + "application mask to be used to filter the locks. "
                                + "Defaults to '*'"),
            },
            responses = {
                @OpenApiResponse(status = STATUS_200, content = {
                    @OpenApiContent(type = Formats.JSON, from = ProjectLockV2.class)}
                )},
                security = {
                @OpenApiSecurity(name = "gets overridden allows lock icon.")
            },
            tags = {TAGS},
            path = PATH,
            method = HttpMethod.GET
    )
    @Override
    public void handle(@NotNull Context ctx) throws Exception {
        super.handle(ctx);
    }

}
