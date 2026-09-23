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

import static cwms.cda.api.Controllers.LOCK_ID;

import com.codahale.metrics.MetricRegistry;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dao.project.ProjectLockDao;
import cwms.cda.data.dao.project.ProjectLockDaoV1;
import cwms.cda.data.dto.project.ProjectLockV1;
import io.javalin.http.Context;
import io.javalin.plugin.openapi.annotations.HttpMethod;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import org.jetbrains.annotations.NotNull;

public final class ProjectLockRevokeDenyV1 extends ProjectLockRevokeDeny<ProjectLockV1> {
    public static final String PATH = "/project-locks/deny";

    public ProjectLockRevokeDenyV1(MetricRegistry metrics) {
        super(metrics);
    }

    @Override
    protected ProjectLockDao<ProjectLockV1> getDao(Context ctx) {
        return new ProjectLockDaoV1(JooqDao.getDslContext(ctx));
    }

    @Override
    protected String getOffice(Context ctx) {
        return null;
    }

    @OpenApi(
            description = "Deny a Lock revoke request.",
            queryParams = {
                @OpenApiParam(name = LOCK_ID, required = true,
                        description = "The id of the lock."),
            },
            method = HttpMethod.POST,
            tags = {TAGS},
            path = PATH
    )
    @Override
    public void handle(@NotNull Context ctx) throws Exception {
        super.handle(ctx);
    }

}
