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

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.cda.data.dao.LookupTypeDao;
import cwms.cda.data.dto.LookupType;
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
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;

import javax.servlet.http.HttpServletResponse;
import java.util.List;

import static com.codahale.metrics.MetricRegistry.name;
import static cwms.cda.api.Controllers.*;
import static cwms.cda.data.dao.JooqDao.getDslContext;

public final class LookupTypeController implements CrudHandler {

    static final String TAG = "LookupTypes";
    private final MetricRegistry metrics;

    private final Histogram requestResultSize;


    public LookupTypeController(MetricRegistry metrics) {
        this.metrics = metrics;
        String className = this.getClass().getName();

        requestResultSize = this.metrics.histogram((name(className, RESULTS, SIZE)));
    }

    private Timer.Context markAndTime(String subject) {
        return Controllers.markAndTime(metrics, getClass().getName(), subject);
    }

    @OpenApi(
            queryParams = {
                    @OpenApiParam(name = CATEGORY, description = "Filters lookup types to the specified category"),
                    @OpenApiParam(name = PREFIX, description = "Filters lookup types to the specified prefix"),
                    @OpenApiParam(name = OFFICE, description = "Filters lookup types to the specified office ID"),
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
            String formatHeader = ctx.header(Header.ACCEPT) != null ? ctx.header(Header.ACCEPT) : Formats.JSON;
            ContentType contentType = Formats.parseHeader(formatHeader);
            ctx.contentType(contentType.toString());
            String serialized = Formats.format(contentType, lookupTypes, LookupType.class);
            ctx.result(serialized);
            ctx.status(HttpServletResponse.SC_OK);
            requestResultSize.update(serialized.length());
        }
    }

    @OpenApi(ignore = true)
    @Override
    public void getOne(@NotNull Context context, @NotNull String s) {
        try (final Timer.Context ignored = markAndTime(GET_ONE)) {
            throw new UnsupportedOperationException(NOT_SUPPORTED_YET);
        }
    }

    @OpenApi(
            queryParams = {
                    @OpenApiParam(name = CATEGORY, description = "Specifies the category id of the lookup type to be created."),
                    @OpenApiParam(name = PREFIX, description = "Specifies the prefix of the lookup type to be created."),
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
            String acceptHeader = ctx.req.getContentType();
            String formatHeader = acceptHeader != null ? acceptHeader : Formats.JSON;
            ContentType contentType = Formats.parseHeader(formatHeader);
            LookupType lookupType = Formats.parseContent(contentType, ctx.body(), LookupType.class);
            lookupType.validate();
            DSLContext dsl = getDslContext(ctx);
            LookupTypeDao dao = new LookupTypeDao(dsl);
            dao.storeLookupType(category, prefix, lookupType);
            ctx.status(HttpServletResponse.SC_CREATED).json("Created Lookup Type");
        }
    }

    @OpenApi(
            queryParams = {
                    @OpenApiParam(name = CATEGORY, description = "Specifies the category id of the lookup type to be updated."),
                    @OpenApiParam(name = PREFIX, description = "Specifies the prefix of the lookup type to be updated."),
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
                    @OpenApiResponse(status = STATUS_204, description = "Lookup Type successfully stored to CWMS.")
            }
    )
    @Override
    public void update(Context ctx, String name) {
        String category = requiredParam(ctx, CATEGORY);
        String prefix = requiredParam(ctx, PREFIX);
        try (Timer.Context ignored = markAndTime(UPDATE)) {
            String acceptHeader = ctx.req.getContentType();
            String formatHeader = acceptHeader != null ? acceptHeader : Formats.JSON;
            ContentType contentType = Formats.parseHeader(formatHeader);
            LookupType lookupType = Formats.parseContent(contentType, ctx.body(), LookupType.class);
            lookupType.validate();
            DSLContext dsl = getDslContext(ctx);
            LookupTypeDao dao = new LookupTypeDao(dsl);
            dao.updateLookupType(category, prefix, lookupType);
            ctx.status(HttpServletResponse.SC_OK).json("Updated Lookup Type");
        }
    }

    @OpenApi(
            queryParams = {
                    @OpenApiParam(name = CATEGORY, description = "Specifies the category id of the lookup type to be deleted."),
                    @OpenApiParam(name = PREFIX, description = "Specifies the prefix of the lookup type to be deleted."),
                    @OpenApiParam(name = OFFICE, description = "Specifies the owning office of the lookup type to be deleted."),
            },
            description = "Delete CWMS Lookup Type",
            method = HttpMethod.DELETE,
            tags = {TAG},
            responses = {
                    @OpenApiResponse(status = STATUS_204, description = "Lookup Type successfully deleted from CWMS."),
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
            ctx.status(HttpServletResponse.SC_NO_CONTENT).json(displayValue + " Deleted");
        }
    }

}