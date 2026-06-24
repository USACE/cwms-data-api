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

package cwms.cda.api;

import static cwms.cda.api.Controllers.CATEGORY;
import static cwms.cda.api.Controllers.CREATE;
import static cwms.cda.api.Controllers.DELETE;
import static cwms.cda.api.Controllers.GET_ALL;
import static cwms.cda.api.Controllers.NAME;
import static cwms.cda.api.Controllers.OFFICE;
import static cwms.cda.api.Controllers.PREFIX;
import static cwms.cda.api.Controllers.STATUS_200;
import static cwms.cda.api.Controllers.STATUS_204;
import static cwms.cda.api.Controllers.STATUS_404;
import static cwms.cda.api.Controllers.UPDATE;
import static cwms.cda.api.Controllers.requiredParam;
import static cwms.cda.data.dao.JooqDao.getDslContext;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.flogger.FluentLogger;
import cwms.cda.api.errors.CdaError;
import cwms.cda.api.errors.ExceptionTraceSupport;
import cwms.cda.data.dao.LookupTypeDao;
import cwms.cda.data.dto.LookupType;
import cwms.cda.data.dto.StatusResponse;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
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

public final class LookupTypeController extends BaseCrudHandler {
    private static final FluentLogger LOGGER = FluentLogger.forEnclosingClass();

    static final String TAG = "LookupTypes";

    public LookupTypeController(MetricRegistry metrics) {
        super(metrics);
    }

    @OpenApi(
            queryParams = {
                    @OpenApiParam(name = CATEGORY, required = true, description = "Filters lookup types to the specified category"),
                    @OpenApiParam(name = PREFIX, required = true, description = "Filters lookup types to the specified prefix"),
                    @OpenApiParam(name = OFFICE, required = true, description = "Filters lookup types to the specified office ID"),
            },
            responses = {
                    @OpenApiResponse(status = STATUS_200, content = {
                            @OpenApiContent(isArray = true, type = Formats.JSON, from = LookupType.class)
                    })
            },
            description = "Returns matching CWMS Lookup Type Data.",
            tags = {TAG}
    )
    @Override
    public void getAll(Context ctx) {
        String officeId = requiredParam(ctx, OFFICE);
        String category = requiredParam(ctx, CATEGORY);
        String prefix = requiredParam(ctx, PREFIX);
        try (Timer.Context ignored = markAndTime(GET_ALL)) {
            DSLContext dsl = getDslContext(ctx);
            LookupTypeDao dao = new LookupTypeDao(dsl);
            List<LookupType> lookupTypes = dao.retrieveLookupTypes(category, prefix, officeId);
            String formatHeader = ctx.header(Header.ACCEPT);
            ContentType contentType = Formats.parseHeader(formatHeader, LookupType.class);
            ctx.contentType(contentType.toString());
            String serialized = Formats.format(contentType, lookupTypes, LookupType.class);
            ctx.status(HttpServletResponse.SC_OK);
            updateResultSize(serialized.length());

            byte[] bytes = serialized.getBytes();
            ctx.header(Header.CONTENT_LENGTH, String.valueOf(bytes.length));
            ctx.res.getOutputStream().write(bytes);
        } catch (IOException ex) {
            CdaError error = ExceptionTraceSupport.buildError(ctx,
                "Failed to process request to retrieve Lookup Types", ex);
            LOGGER.atSevere().withCause(ex).log("Failed to process request to retrieve Lookup Types");
            ctx.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR).json(error);
        }
    }

    @OpenApi(ignore = true)
    @Override
    public void getOne(@NotNull Context context, @NotNull String s) {
        context.status(HttpServletResponse.SC_NOT_IMPLEMENTED).json(CdaError.notImplemented());
    }

    @OpenApi(
            queryParams = {
                    @OpenApiParam(name = CATEGORY, required = true, description = "Specifies the category id of the lookup type to be created."),
                    @OpenApiParam(name = PREFIX, required = true, description = "Specifies the prefix of the lookup type to be created."),
            },
            requestBody = @OpenApiRequestBody(
                    content = {
                            @OpenApiContent(from = LookupType.class, type = Formats.JSON)
                    },
                    required = true),
            description = "Create CWMS Lookup Type",
            method = HttpMethod.POST,
            tags = {TAG},
            responses = {
                    @OpenApiResponse(status = STATUS_204, description = "Lookup Type successfully stored to CWMS.")
            }
    )
    @Override
    public void create(Context ctx) {
        String category = requiredParam(ctx, CATEGORY);
        String prefix = requiredParam(ctx, PREFIX);
        try (Timer.Context ignored = markAndTime(CREATE)) {
            String formatHeader = ctx.req.getContentType();
            ContentType contentType = Formats.parseHeader(formatHeader, LookupType.class);
            LookupType lookupType = Formats.parseContent(contentType, ctx.body(), LookupType.class);
            DSLContext dsl = getDslContext(ctx);
            LookupTypeDao dao = new LookupTypeDao(dsl);
            dao.storeLookupType(category, prefix, lookupType);
            StatusResponse re = new StatusResponse(lookupType.getOfficeId(), "Lookup Type successfully stored to CWMS.",
                    lookupType.getDisplayValue());
            ctx.status(HttpServletResponse.SC_CREATED).json(re);
        }
    }

    @OpenApi(
            pathParams = {
                    @OpenApiParam(name = NAME, required = true, description = "Specifies the location type to update.")
            },
            queryParams = {
                    @OpenApiParam(name = CATEGORY, required = true, description = "Specifies the category id of the lookup type to be updated."),
                    @OpenApiParam(name = PREFIX, required = true, description = "Specifies the prefix of the lookup type to be updated."),
            },
            requestBody = @OpenApiRequestBody(
                    content = {
                            @OpenApiContent(from = LookupType.class, type = Formats.JSON)
                    },
                    required = true),
            description = "Update CWMS Lookup Type",
            method = HttpMethod.PATCH,
            tags = {TAG},
            responses = {
                    @OpenApiResponse(status = STATUS_200, description = "Updated Lookup Type")
            }
    )
    @Override
    public void update(Context ctx, String name) {
        logUnusedPathParameter(ctx, NAME, "Body has required information");
        String category = requiredParam(ctx, CATEGORY);
        String prefix = requiredParam(ctx, PREFIX);
        try (Timer.Context ignored = markAndTime(UPDATE)) {
            String formatHeader = ctx.req.getContentType();
            ContentType contentType = Formats.parseHeader(formatHeader, LookupType.class);
            LookupType lookupType = Formats.parseContent(contentType, ctx.body(), LookupType.class);
            DSLContext dsl = getDslContext(ctx);
            LookupTypeDao dao = new LookupTypeDao(dsl);
            dao.updateLookupType(category, prefix, lookupType);
            StatusResponse re = new StatusResponse(lookupType.getOfficeId(), "Updated Lookup Type",
                    lookupType.getDisplayValue());
            ctx.status(HttpServletResponse.SC_OK).json(re);
        }
    }

    @OpenApi(
            pathParams = {
                    @OpenApiParam(name = NAME, required = true, description = "Specifies the location type to delete.")
            },
            queryParams = {
                    @OpenApiParam(name = CATEGORY, required = true, description = "Specifies the category id of the lookup type to be deleted."),
                    @OpenApiParam(name = PREFIX, required = true, description = "Specifies the prefix of the lookup type to be deleted."),
                    @OpenApiParam(name = OFFICE, required = true, description = "Specifies the owning office of the lookup type to be deleted."),
            },
            description = "Delete CWMS Lookup Type",
            method = HttpMethod.DELETE,
            tags = {TAG},
            responses = {
                    @OpenApiResponse(status = STATUS_200, description = "Lookup Type successfully deleted from CWMS."),
                    @OpenApiResponse(status = STATUS_404, description = "Based on the combination of inputs provided the lookup type was not found.")
            }
    )
    @Override
    public void delete(Context ctx, @NotNull String displayValue) {
        String officeId = requiredParam(ctx, OFFICE);
        String category = requiredParam(ctx, CATEGORY);
        String prefix = requiredParam(ctx, PREFIX);
        try (Timer.Context ignored = markAndTime(DELETE)) {
            DSLContext dsl = getDslContext(ctx);
            LookupTypeDao dao = new LookupTypeDao(dsl);
            dao.deleteLookupType(category, prefix, officeId, displayValue);
            StatusResponse re = new StatusResponse(officeId, "Lookup Type successfully deleted from CWMS.", displayValue);
            ctx.status(HttpServletResponse.SC_OK).json(re);
        }
    }

}