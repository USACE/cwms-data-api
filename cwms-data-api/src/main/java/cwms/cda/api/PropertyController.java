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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.flogger.FluentLogger;
import cwms.cda.data.dao.PropertyDao;
import cwms.cda.data.dto.Property;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.FormattingException;
import cwms.cda.formatters.json.JsonV2;
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
import java.io.IOException;
import java.util.List;

import static com.codahale.metrics.MetricRegistry.name;
import static cwms.cda.api.Controllers.*;
import static cwms.cda.data.dao.JooqDao.getDslContext;

public final class PropertyController implements CrudHandler {
    
    static final String TAG = "Properties";
    private static final FluentLogger LOGGER = FluentLogger.forEnclosingClass();
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
                        @OpenApiContent(isArray = true, type = Formats.JSONV2, from = Property.class)
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
            ContentType contentType = getContentType(ctx);
            ctx.contentType(contentType.toString());
            ObjectMapper om = getObjectMapperForFormat(contentType);
            String serialized = om.writeValueAsString(properties);
            ctx.result(serialized);
            ctx.status(HttpServletResponse.SC_OK);
            requestResultSize.update(serialized.length());
        } catch (IOException ex) {
            String errorMsg = "Error retrieving properties.";
            LOGGER.atWarning().withCause(ex).log("Error deserializing properties"
                    + " with parameters: " + ctx.queryParamMap());
            throw new FormattingException(errorMsg, ex);
        }
    }

    @OpenApi(
            pathParams = {
                @OpenApiParam(name = NAME, description = "Specifies the name of "
                        + "the property to be retrieved."),
            },
            queryParams = {
                @OpenApiParam(name = OFFICE, description = "Specifies the owning office of "
                        + "the property to be retrieved."),
                @OpenApiParam(name = CATEGORY_ID, description = "Specifies the category id of "
                        + "the property to be retrieved."),
                @OpenApiParam(name = DEFAULT_VALUE, description = "Specifies the default value "
                        + "if the property does not exist."),
            },
            responses = {
                @OpenApiResponse(status = STATUS_200,
                        content = {
                                @OpenApiContent(type = Formats.JSONV2, from = Property.class)
                        })
            },
            description = "Returns CWMS Property Data",
            tags = {TAG}
    )
    @Override
    public void getOne(Context ctx, String name) {
        String office = ctx.queryParam(OFFICE);
        String category = ctx.queryParam(CATEGORY_ID);
        String defaultValue = ctx.queryParam(DEFAULT_VALUE);
        try (Timer.Context ignored = markAndTime(GET_ONE)) {
            DSLContext dsl = getDslContext(ctx);
            PropertyDao dao = new PropertyDao(dsl);
            Property property = dao.retrieveProperty(office, category, name, defaultValue);
            ContentType contentType = getContentType(ctx);
            ctx.contentType(contentType.toString());
            ObjectMapper om = getObjectMapperForFormat(contentType);
            String serialized = om.writeValueAsString(property);
            ctx.result(serialized);
            ctx.status(HttpServletResponse.SC_OK);
            requestResultSize.update(serialized.length());
        } catch (IOException ex) {
            String errorMsg = "Error retrieving property " + name;
            LOGGER.atWarning().withCause(ex).log("Error deserializing property: " + name
                    + " with parameters: " + ctx.queryParamMap());
            throw new FormattingException(errorMsg, ex);
        }
    }

    private static @NotNull ContentType getContentType(Context ctx) {
        String formatHeader = ctx.header(Header.ACCEPT) != null ? ctx.header(Header.ACCEPT) :
                Formats.JSONV2;
        ContentType contentType = Formats.parseHeader(formatHeader);
        if (contentType == null) {
            throw new FormattingException("Format header could not be parsed");
        }
        return contentType;
    }


    @OpenApi(
            requestBody = @OpenApiRequestBody(
                    content = {
                        @OpenApiContent(from = Property.class, type = Formats.JSONV2)
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
            String acceptHeader = ctx.req.getContentType();
            String formatHeader = acceptHeader != null ? acceptHeader : Formats.JSONV2;
            ContentType contentType = Formats.parseHeader(formatHeader);
            if (contentType == null) {
                throw new FormattingException("Format header could not be parsed");
            }
            Property property = deserializeProperty(ctx.body(), contentType);
            property.validate();
            DSLContext dsl = getDslContext(ctx);
            PropertyDao dao = new PropertyDao(dsl);
            dao.storeProperty(property);
            ctx.status(HttpServletResponse.SC_CREATED).json("Created Property");
        } catch (IOException ex) {
            throw new IllegalArgumentException("Unable to parse property from content body", ex);
        }

    }

    @OpenApi(
            requestBody = @OpenApiRequestBody(
                    content = {
                        @OpenApiContent(from = Property.class, type = Formats.JSONV2)
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
            String acceptHeader = ctx.req.getContentType();
            String formatHeader = acceptHeader != null ? acceptHeader : Formats.JSONV2;
            ContentType contentType = Formats.parseHeader(formatHeader);
            if (contentType == null) {
                throw new FormattingException("Format header could not be parsed");
            }
            Property property = deserializeProperty(ctx.body(), contentType);
            property.validate();
            DSLContext dsl = getDslContext(ctx);
            PropertyDao dao = new PropertyDao(dsl);
            dao.updateProperty(property);
            ctx.status(HttpServletResponse.SC_OK).json("Updated Property");
        } catch (IOException ex) {
            throw new IllegalArgumentException("Unable to parse property from content body", ex);
        }

    }

    @OpenApi(
            pathParams = {
                @OpenApiParam(name = NAME, description = "Specifies the name of "
                        + "the property to be deleted."),
            },
            queryParams = {
                @OpenApiParam(name = OFFICE, description = "Specifies the owning office of "
                        + "the property to be deleted."),
                @OpenApiParam(name = CATEGORY_ID, description = "Specifies the category id of "
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
        String office = ctx.queryParam(OFFICE);
        String category = ctx.queryParam(CATEGORY_ID);
        try (Timer.Context ignored = markAndTime(DELETE)) {
            DSLContext dsl = getDslContext(ctx);
            PropertyDao dao = new PropertyDao(dsl);
            dao.deleteProperty(office, category, name);
            ctx.status(HttpServletResponse.SC_NO_CONTENT).json(name + " Deleted");
        }
    }

    private static Property deserializeProperty(String body, ContentType contentType)
            throws IOException {
        ObjectMapper om = getObjectMapperForFormat(contentType);
        Property retVal;
        try {
            retVal = om.readValue(body, Property.class);
        } catch (Exception e) {
            throw new IOException("Failed to deserialize property", e);
        }
        return retVal;
    }

    private static ObjectMapper getObjectMapperForFormat(ContentType contentType) {
        ObjectMapper om;
        if (ContentType.equivalent(Formats.JSONV2, contentType.toString())) {
            om = JsonV2.buildObjectMapper();
        } else {
            throw new FormattingException("Format is not currently supported for Properties");
        }
        return om;
    }
}
