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

import static cwms.cda.api.Controllers.REVOKE_EXISTING;
import static cwms.cda.api.Controllers.REVOKE_TIMEOUT;
import static cwms.cda.api.Controllers.STATUS_200;

import com.codahale.metrics.MetricRegistry;
import cwms.cda.data.dao.AuthDao;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dao.project.ProjectLockDao;
import cwms.cda.data.dao.project.ProjectLockDaoV1;
import cwms.cda.data.dto.project.ProjectLockId;
import cwms.cda.data.dto.project.ProjectLockV1;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import cwms.cda.security.DataApiPrincipal;
import io.javalin.http.Context;
import io.javalin.plugin.openapi.annotations.HttpMethod;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiRequestBody;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import org.jetbrains.annotations.NotNull;

public final class ProjectLockRequestV1 extends ProjectLockRequest<ProjectLockV1> {
    public static final String PATH = "/project-locks/";

    public ProjectLockRequestV1(MetricRegistry metrics) {
        super(metrics, ProjectLockRequest.class);
    }

    @Override
    protected ProjectLockDao<ProjectLockV1> getDao(Context ctx) {
        return new ProjectLockDaoV1(JooqDao.getDslContext(ctx));
    }

    @Override
    protected ProjectLockV1 parseAndValidateLock(@NotNull Context ctx) {
        String formatHeader = ctx.req.getContentType();
        ContentType contentType = Formats.parseHeader(formatHeader, ProjectLockV1.class);
        ProjectLockV1 lock = Formats.parseContent(contentType, ctx.bodyAsInputStream(), ProjectLockV1.class);
        if (lock.getSessionUser() == null) {
            Object principal = ctx.attribute(AuthDao.DATA_API_PRINCIPAL);
            if (principal == null || !(principal instanceof DataApiPrincipal)) {
                throw new IllegalArgumentException(
                    "Session user was not provided and user principal is not registered");
            }
            lock = new ProjectLockV1.Builder(lock)
                .withSessionUser(((DataApiPrincipal) principal).getName())
                .build();
        }
        return lock;
    }

    @OpenApi(
            description = "Requests the creation of a new Reservoir Project Lock",
            requestBody = @OpenApiRequestBody(
                    description = "Users must provide a Lock object specifying the officeId, "
                            + "projectId and applicationId. Other fields will be ignored.",
                    content = {
                        @OpenApiContent(from = ProjectLockV1.class, type = Formats.JSON),
                    },
                    required = true),
            queryParams = {
                @OpenApiParam(name = REVOKE_EXISTING, type = Boolean.class,
                        description = "If an existing lock is found should a revoke be "
                                + "attempted? Default: " + REVOKE_DEFAULT),
                @OpenApiParam(name = REVOKE_TIMEOUT, type = Integer.class,
                        description = "time in seconds to wait for existing lock to be "
                                + "revoked. Default: " + DEFAULT_TIMEOUT),
            },
            responses = {
                @OpenApiResponse(status = STATUS_200, content = {
                    @OpenApiContent(type = Formats.JSON, from = ProjectLockId.class)}
                )},
            method = HttpMethod.POST,
            path = PATH,
            tags = {TAGS}
    )
    @Override
    public void handle(@NotNull Context ctx) throws Exception {
        super.handle(ctx);
    }

}
