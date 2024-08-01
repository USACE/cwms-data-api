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

import static cwms.cda.api.Controllers.APPLICATION_ID;
import static cwms.cda.api.Controllers.OFFICE;
import static cwms.cda.api.Controllers.PROJECT_MASK;
import static cwms.cda.api.Controllers.USER_ID;
import static cwms.cda.api.Controllers.requiredParam;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.cda.api.Controllers;
import cwms.cda.api.errors.RequiredQueryParameterException;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dao.project.ProjectLockDao;
import io.javalin.http.Context;
import io.javalin.http.Handler;
import io.javalin.plugin.openapi.annotations.HttpMethod;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import javax.servlet.http.HttpServletResponse;
import org.jetbrains.annotations.NotNull;

public class UpdateLockRevokerRights implements Handler {
    public static final String PATH = "/project-lock-rights/update";
    public static final String TAGS = "Project Lock Revoker Rights";
    private final MetricRegistry metrics;

    private Timer.Context markAndTime(String subject) {
        return Controllers.markAndTime(metrics, getClass().getName(), subject);
    }

    public UpdateLockRevokerRights(MetricRegistry metrics) {
        this.metrics = metrics;

    }

    @OpenApi(
            description = "Update Lock Revoker Rights.",
            queryParams = {
                @OpenApiParam(name = OFFICE, required = true, description =
                        "Specifies the session office."),
                @OpenApiParam(name = PROJECT_MASK, description =
                        "Specifies the project mask to be used Defaults to '*'"),
                @OpenApiParam(name = APPLICATION_ID, required = true, description =
                        "Specifies the application id."),
                @OpenApiParam(name = USER_ID, required = true,
                        description = "Specifies the user."),
                @OpenApiParam(name = Controllers.ALLOW, required = true, type = Boolean.class, description =
                        "True to add the user to the allow list, False to add to the deny list")
            },
            method = HttpMethod.POST,
            tags = {TAGS},
            path = PATH
    )
    @Override
    public void handle(@NotNull Context ctx) throws Exception {
        String office = requiredParam(ctx, OFFICE);
        String userId = requiredParam(ctx, USER_ID);
        String projMask = ctx.queryParamAsClass(PROJECT_MASK, String.class).getOrDefault("*");
        String appId = requiredParam(ctx, APPLICATION_ID);
        Boolean allow = ctx.queryParamAsClass(Controllers.ALLOW, Boolean.class)
                .getOrThrow(e -> new RequiredQueryParameterException(Controllers.ALLOW));

        try (final Timer.Context ignored = markAndTime("updateRights")) {
            ProjectLockDao lockDao = new ProjectLockDao(JooqDao.getDslContext(ctx));
            lockDao.updateLockRevokerRights(office, projMask, appId, userId, allow);
        }
        ctx.status(HttpServletResponse.SC_OK);
    }

}
