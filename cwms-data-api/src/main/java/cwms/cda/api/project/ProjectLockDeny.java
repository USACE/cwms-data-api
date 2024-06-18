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

import static cwms.cda.api.Controllers.DELETE;
import static cwms.cda.api.Controllers.LOCK_ID;
import static cwms.cda.api.Controllers.requiredParam;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.cda.api.Controllers;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dao.project.ProjectLockDao;
import io.javalin.http.Context;
import io.javalin.http.Handler;
import io.javalin.plugin.openapi.annotations.HttpMethod;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import javax.servlet.http.HttpServletResponse;
import org.jetbrains.annotations.NotNull;

public class ProjectLockDeny implements Handler {
    private final MetricRegistry metrics;

    private Timer.Context markAndTime(String subject) {
        return Controllers.markAndTime(metrics, getClass().getName(), subject);
    }

    public ProjectLockDeny(MetricRegistry metrics) {
        this.metrics = metrics;

    }

    @OpenApi(
            description = "Deny a Lock revoke request.",
            queryParams = {
                @OpenApiParam(name = LOCK_ID, required = true,
                        description = "The id of the lock."),
            },
            method = HttpMethod.POST,
            tags = {"Project Locks"},
            path = "/project-locks/"
    )
    @Override
    public void handle(@NotNull Context ctx) throws Exception {

        String lockId = requiredParam(ctx, LOCK_ID);

        try (final Timer.Context ignored = markAndTime(DELETE)) {
            ProjectLockDao lockDao = new ProjectLockDao(JooqDao.getDslContext(ctx));
            lockDao.denyLockRevocation(lockId);
        }
        ctx.status(HttpServletResponse.SC_OK);
    }

}
