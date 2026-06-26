/*
 * MIT License
 *
 * Copyright (c) 2023 Hydrologic Engineering Center
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
import static cwms.cda.api.Controllers.CREATE;
import static cwms.cda.api.Controllers.FAIL_IF_EXISTS;
import static cwms.cda.api.Controllers.GET_ALL;
import static cwms.cda.api.Controllers.OFFICE;
import static cwms.cda.api.Controllers.RESULTS;
import static cwms.cda.api.Controllers.SIZE;
import static cwms.cda.api.Controllers.SPECIFIED_LEVEL_ID;
import static cwms.cda.api.Controllers.STATUS_200;
import static cwms.cda.api.Controllers.TEMPLATE_ID_MASK;
import static cwms.cda.api.Controllers.UPDATE;
import static cwms.cda.api.Controllers.requiredParam;
import static cwms.cda.data.dao.JooqDao.getDslContext;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.flogger.FluentLogger;
import cwms.cda.api.errors.CdaError;
import cwms.cda.api.errors.ExceptionTraceSupport;
import cwms.cda.data.dao.SpecifiedLevelDao;
import cwms.cda.data.dto.SpecifiedLevel;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import io.javalin.apibuilder.CrudHandler;
import io.javalin.core.util.Header;
import io.javalin.http.Context;
import io.javalin.plugin.openapi.annotations.HttpMethod;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiRequestBody;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import java.io.IOException;
import java.util.List;
import javax.servlet.http.HttpServletResponse;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;

public class SpecifiedLevelController implements CrudHandler {
    private static final FluentLogger LOGGER = FluentLogger.forEnclosingClass();
    private static final String TAG = "Levels";
    private final MetricRegistry metrics;

    private final Histogram requestResultSize;

    public SpecifiedLevelController(MetricRegistry metrics) {
        this.metrics = metrics;
        String className = this.getClass().getName();
        requestResultSize = this.metrics.histogram((name(className, RESULTS, SIZE)));
    }

    @NotNull
    protected SpecifiedLevelDao getDao(DSLContext dsl) {
        return new SpecifiedLevelDao(dsl);
    }


    private Timer.Context markAndTime(String subject) {
        return Controllers.markAndTime(metrics, getClass().getName(), subject);
    }


    @OpenApi(
        queryParams = {
            @OpenApiParam(name = OFFICE, description = "Specifies the owning office of "
                + "the Specified Levels whose data is to be included in the response."
                + " If this field is not specified, matching rating information from "
                + "all offices shall be returned."),
            @OpenApiParam(name = TEMPLATE_ID_MASK, description = "Mask that specifies "
                + "the IDs to be included in the response. If this field is not "
                + "specified, all specified levels shall be returned."),
        },
        responses = {
            @OpenApiResponse(status = STATUS_200,
                content = {
                    @OpenApiContent(type = Formats.JSONV2, from =
                        SpecifiedLevel.class)
                }
            )
        },
        tags = {TAG}
    )
    @Override
    public void getAll(Context ctx) {
        String office = ctx.queryParam(OFFICE);
        String templateIdMask = ctx.queryParam(TEMPLATE_ID_MASK);

        String formatHeader = ctx.header(Header.ACCEPT);
        ContentType contentType = Formats.parseHeader(formatHeader, SpecifiedLevel.class);
        try (Timer.Context timeContext = markAndTime(GET_ALL)) {
            DSLContext dsl = getDslContext(ctx);

            SpecifiedLevelDao dao = getDao(dsl);
            List<SpecifiedLevel> levels = dao.getSpecifiedLevels(office, templateIdMask);

            ctx.contentType(contentType.toString());

            String result = Formats.format(contentType, levels, SpecifiedLevel.class);
            requestResultSize.update(result.length());
            ctx.status(HttpServletResponse.SC_OK);

            byte[] bytes = result.getBytes();
            ctx.header(Header.CONTENT_LENGTH, String.valueOf(bytes.length));
            ctx.res.getOutputStream().write(bytes);
        } catch (IOException ex) {
            CdaError error = ExceptionTraceSupport.buildError(ctx,
                "Failed to process request to retrieve Specified Levels", ex);
            LOGGER.atSevere().withCause(ex).log("Failed to process request to retrieve Specified Levels");
            ctx.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR).json(error);
        }

    }

    @OpenApi(ignore = true)
    @Override
    public void getOne(@NotNull Context ctx, @NotNull String templateId) {
        ctx.status(HttpServletResponse.SC_NOT_IMPLEMENTED).json(CdaError.notImplemented());
    }

    @OpenApi(
        description = "Create new SpecifiedLevel",
        requestBody = @OpenApiRequestBody(
            content = {
                @OpenApiContent(from = SpecifiedLevel.class, type = Formats.JSONV2)
            },
            required = true),
        queryParams = {
            @OpenApiParam(name = FAIL_IF_EXISTS, type = Boolean.class,
                description = "Create will fail if provided ID already exists. Default: true")
        },
        method = HttpMethod.POST,
        tags = {TAG}
    )
    @Override
    public void create(@NotNull Context ctx) {
        try (Timer.Context ignored = markAndTime(CREATE)) {
            DSLContext dsl = getDslContext(ctx);

            String formatHeader = ctx.req.getContentType();
            String body = ctx.body();
            ContentType contentType = Formats.parseHeader(formatHeader, SpecifiedLevel.class);
            SpecifiedLevel deserialize = Formats.parseContent(contentType, body, SpecifiedLevel.class);
            SpecifiedLevelDao dao = getDao(dsl);
            boolean failIfExists = ctx.queryParamAsClass(FAIL_IF_EXISTS, Boolean.class).getOrDefault(true);
            dao.create(deserialize, failIfExists);
            ctx.status(HttpServletResponse.SC_CREATED);
        }
    }

    @OpenApi(
        description = "Renames the requested specified level id",
        pathParams = {
            @OpenApiParam(name = SPECIFIED_LEVEL_ID, description = "The specified level id to be renamed"),
        },
        queryParams = {
            @OpenApiParam(name = OFFICE, required = true, description = "Specifies the "
                + "owning office of the specified level to be renamed"),
            @OpenApiParam(name = SPECIFIED_LEVEL_ID, required = true, description = "The new specified level id.")
        },
        method = HttpMethod.PATCH,
        tags = {TAG}
    )
    @Override
    public void update(@NotNull Context ctx, @NotNull String oldSpecifiedLevelId) {
        try (Timer.Context ignored = markAndTime(UPDATE)) {
            DSLContext dsl = getDslContext(ctx);

            SpecifiedLevelDao dao = getDao(dsl);
            String newSpecifiedLevelId = requiredParam(ctx, SPECIFIED_LEVEL_ID);
            String office = requiredParam(ctx, OFFICE);
            dao.update(oldSpecifiedLevelId, newSpecifiedLevelId, office);
            ctx.status(HttpServletResponse.SC_NO_CONTENT);
        }
    }



    @OpenApi(
        description = "Deletes requested specified level id",
        pathParams = {
            @OpenApiParam(name = SPECIFIED_LEVEL_ID, description = "The specified level id to be deleted"),
        },
        queryParams = {
            @OpenApiParam(name = OFFICE, required = true, description = "Specifies the "
                + "owning office of the timeseries identifier to be deleted"),
        },
        method = HttpMethod.DELETE,
        tags = {TAG}
    )
    @Override
    public void delete(@NotNull Context ctx, @NotNull String specifiedLevelId) {
        try (Timer.Context ignored = markAndTime(UPDATE)) {
            DSLContext dsl = getDslContext(ctx);

            SpecifiedLevelDao dao = getDao(dsl);
            String office = requiredParam(ctx, OFFICE);
            dao.delete(specifiedLevelId, office);
            ctx.status(HttpServletResponse.SC_NO_CONTENT);
        }
    }

}
