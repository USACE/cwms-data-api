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
import cwms.cda.data.dao.PropertyDao;
import cwms.cda.data.dto.Property;
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
import org.jooq.DSLContext;

import javax.servlet.http.HttpServletResponse;
import java.util.List;

import static com.codahale.metrics.MetricRegistry.name;
import static cwms.cda.api.Controllers.*;
import static cwms.cda.data.dao.JooqDao.getDslContext;

public final class PropertyController implements CrudHandler {
    
    static final String TAG = "Properties";
    private final MetricRegistry metrics;

    private final Histogram requestResultSize;


    public PropertyController(MetricRegistry metrics) {
        this.metrics = metrics;
        String className = this.getClass().getName();

        requestResultSize = this.metrics.histogram((name(className, RESULTS, SIZE)));
    }

    private Timer.Context markAndTime(String subject) {
        return Controllers.markAndTime(metrics, getClass().getName(), subject);
    }

    @OpenApi(
            pathParams = {
            },
            queryParams = {
                @OpenApiParam(name = OFFICE_MASK, description = "Filters properties to the specified office mask"),
                @OpenApiParam(name = CATEGORY_ID, description = "Filters properties to the specified category mask"),
                @OpenApiParam(name = NAME_MASK, description = "Filters properties to the specified name mask"),
            },
            responses = {
                @OpenApiResponse(status = STATUS_200, content = {
                        @OpenApiContent(isArray = true, type = Formats.JSON, from = Property.class)
                })
            },
            description = "Returns matching CWMS Property Data.",
            tags = {TAG}
    )
    @Override
    public void getAll(Context ctx) {
        String officeMask = ctx.queryParam(OFFICE_MASK);
        String categoryMask = ctx.queryParam(CATEGORY_ID_MASK);
        String nameMask = ctx.queryParam(NAME_MASK);
        try (Timer.Context ignored = markAndTime(GET_ALL)) {
            DSLContext dsl = getDslContext(ctx);
            PropertyDao dao = new PropertyDao(dsl);
            List<Property> properties = dao.retrieveProperties(officeMask, categoryMask, nameMask);
            String formatHeader = ctx.header(Header.ACCEPT);
            ContentType contentType = Formats.parseHeader(formatHeader, Property.class);
            ctx.contentType(contentType.toString());
            String serialized = Formats.format(contentType, properties, Property.class);
            ctx.result(serialized);
            ctx.status(HttpServletResponse.SC_OK);
            requestResultSize.update(serialized.length());
        }
    }

    @OpenApi(
            pathParams = {
                @OpenApiParam(name = NAME, required = true, description = "Specifies the name of "
                        + "the property to be retrieved."),
            },
            queryParams = {
                @OpenApiParam(name = OFFICE, required = true, description = "Specifies the owning office of "
                        + "the property to be retrieved."),
                @OpenApiParam(name = CATEGORY_ID, required = true,description = "Specifies the category id of "
                        + "the property to be retrieved."),
                @OpenApiParam(name = DEFAULT_VALUE, description = "Specifies the default value "
                        + "if the property does not exist."),
            },
            responses = {
                @OpenApiResponse(status = STATUS_200,
                        content = {
                                @OpenApiContent(type = Formats.JSON, from = Property.class)
                        })
            },
            description = "Returns CWMS Property Data",
            tags = {TAG}
    )
    @Override
    public void getOne(Context ctx, String name) {
        String office = requiredParam(ctx, OFFICE);
        String category = requiredParam(ctx, CATEGORY_ID);
        String defaultValue = ctx.queryParam(DEFAULT_VALUE);
        try (Timer.Context ignored = markAndTime(GET_ONE)) {
            DSLContext dsl = getDslContext(ctx);
            PropertyDao dao = new PropertyDao(dsl);
            Property property = dao.retrieveProperty(office, category, name, defaultValue);
            String formatHeader = ctx.header(Header.ACCEPT);
            ContentType contentType = Formats.parseHeader(formatHeader, Property.class);
            ctx.contentType(contentType.toString());
            String serialized = Formats.format(contentType, property);
            ctx.result(serialized);
            ctx.status(HttpServletResponse.SC_OK);
            requestResultSize.update(serialized.length());
        }
    }


    @OpenApi(
            requestBody = @OpenApiRequestBody(
                    content = {
                        @OpenApiContent(from = Property.class, type = Formats.JSON)
                    },
                    required = true),
            description = "Create CWMS Property",
            method = HttpMethod.POST,
            tags = {TAG},
            responses = {
                @OpenApiResponse(status = STATUS_204, description = "Property successfully stored to CWMS.")
            }
    )
    @Override
    public void create(Context ctx) {
        try (Timer.Context ignored = markAndTime(CREATE)) {
            String formatHeader = ctx.req.getContentType();
            ContentType contentType = Formats.parseHeader(formatHeader, Property.class);
            Property property = Formats.parseContent(contentType, ctx.body(), Property.class);
            DSLContext dsl = getDslContext(ctx);
            PropertyDao dao = new PropertyDao(dsl);
            dao.storeProperty(property);
            ctx.status(HttpServletResponse.SC_CREATED).json("Created Property");
        }

    }

    @OpenApi(
            requestBody = @OpenApiRequestBody(
                    content = {
                        @OpenApiContent(from = Property.class, type = Formats.JSON)
                    },
                    required = true),
            description = "Update CWMS Property",
            method = HttpMethod.PATCH,
            tags = {TAG},
            responses = {
                @OpenApiResponse(status = STATUS_204, description = "Property successfully stored to CWMS.")
            }
    )
    @Override
    public void update(Context ctx, String name) {
        try (Timer.Context ignored = markAndTime(UPDATE)) {
            String formatHeader = ctx.req.getContentType();
            ContentType contentType = Formats.parseHeader(formatHeader, Property.class);
            Property property = Formats.parseContent(contentType, ctx.body(), Property.class);
            DSLContext dsl = getDslContext(ctx);
            PropertyDao dao = new PropertyDao(dsl);
            dao.updateProperty(property);
            ctx.status(HttpServletResponse.SC_OK).json("Updated Property");
        }

    }

    @OpenApi(
            pathParams = {
                @OpenApiParam(name = NAME, required = true, description = "Specifies the name of "
                        + "the property to be deleted."),
            },
            queryParams = {
                @OpenApiParam(name = OFFICE, required = true, description = "Specifies the owning office of "
                        + "the property to be deleted."),
                @OpenApiParam(name = CATEGORY_ID, required = true, description = "Specifies the category id of "
                        + "the property to be deleted."),
            },
            description = "Delete CWMS Property",
            method = HttpMethod.DELETE,
            tags = {TAG},
            responses = {
                @OpenApiResponse(status = STATUS_204, description = "Property successfully deleted from CWMS."),
                @OpenApiResponse(status = STATUS_404, description = "Based on the combination of "
                        + "inputs provided the property was not found.")
            }
    )
    @Override
    public void delete(Context ctx, String name) {
        String office = requiredParam(ctx, OFFICE);
        String category = requiredParam(ctx, CATEGORY_ID);
        try (Timer.Context ignored = markAndTime(DELETE)) {
            DSLContext dsl = getDslContext(ctx);
            PropertyDao dao = new PropertyDao(dsl);
            dao.deleteProperty(office, category, name);
            ctx.status(HttpServletResponse.SC_NO_CONTENT).json(name + " Deleted");
        }
    }
}
