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
import static cwms.cda.api.Controllers.*;
import static cwms.cda.data.dao.JooqDao.getDslContext;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import cwms.cda.api.errors.CdaError;
import cwms.cda.data.dao.LocationCategoryDao;
import cwms.cda.data.dto.LocationCategory;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.json.JsonV1;
import io.javalin.apibuilder.CrudHandler;
import io.javalin.core.util.Header;
import io.javalin.http.Context;
import io.javalin.plugin.openapi.annotations.HttpMethod;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiRequestBody;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import java.util.List;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.servlet.http.HttpServletResponse;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;

public class LocationCategoryController implements CrudHandler {
    public static final Logger logger =
            Logger.getLogger(LocationCategoryController.class.getName());
    private static final String TAG = "Location Categories";

    private final MetricRegistry metrics;

    private final Histogram requestResultSize;

    public LocationCategoryController(MetricRegistry metrics) {
        this.metrics = metrics;
        String className = this.getClass().getName();

        requestResultSize = this.metrics.histogram((name(className, RESULTS, SIZE)));
    }

    private Timer.Context markAndTime(String subject) {
        return Controllers.markAndTime(metrics, getClass().getName(), subject);
    }

    @OpenApi(queryParams = {
            @OpenApiParam(name = OFFICE, description = "Specifies the owning office of the "
                    + "location category(ies) whose data is to be included in the response. If "
                    + "this field is not specified, matching location category information from "
                    + "all offices shall be returned."),},
            responses = {@OpenApiResponse(status = STATUS_200,
                    content = {
                            @OpenApiContent(isArray = true, from = LocationCategory.class, type =
                                    Formats.JSON)
                    })
            },

            description = "Returns CWMS Location Category Data",
            tags = {TAG}
    )
    @Override
    public void getAll(Context ctx) {

        try (final Timer.Context timeContext = markAndTime(GET_ALL)) {
            DSLContext dsl = getDslContext(ctx);
            LocationCategoryDao dao = new LocationCategoryDao(dsl);
            String office = ctx.queryParam(OFFICE);

            List<LocationCategory> cats = dao.getLocationCategories(office);

            if (!cats.isEmpty()) {
                String formatHeader = ctx.header(Header.ACCEPT);
                ContentType contentType = Formats.parseHeaderAndQueryParm(formatHeader, null);

                String result = Formats.format(contentType, cats, LocationCategory.class);

                ctx.result(result).contentType(contentType.toString());
                requestResultSize.update(result.length());

                ctx.status(HttpServletResponse.SC_OK);
            } else {
                CdaError re = new CdaError("Cannot find requested location category for "
                        + "office provided: " + office);

                logger.info(() -> {
                    StringBuilder builder = new StringBuilder(re.toString())
                            .append("with url:").append(ctx.fullUrl());
                    return builder.toString();
                });
                ctx.json(re).status(HttpServletResponse.SC_NOT_FOUND);
            }

        }

    }

    @OpenApi(
            pathParams = {
                    @OpenApiParam(name = CATEGORY_ID, required = true, description = "Specifies"
                            + " the Category whose data is to be included in the response."),
            },
            queryParams = {
                    @OpenApiParam(name = OFFICE, required = true, description = "Specifies the "
                            + "owning office of the Location Category whose data is to be "
                            + "included in the response."),
            },
            responses = {
                    @OpenApiResponse(status = STATUS_200,
                            content = {
                                    @OpenApiContent(from = LocationCategory.class, type =
                                            Formats.JSON)
                            }
                    )
            },
            description = "Retrieves requested Location Category",
            tags = {TAG})
    @Override
    public void getOne(Context ctx, @NotNull String categoryId) {

        try (final Timer.Context timeContext = markAndTime(GET_ONE)){
            DSLContext dsl = getDslContext(ctx);

            LocationCategoryDao dao = new LocationCategoryDao(dsl);
            String office = requiredParam(ctx, OFFICE);

            Optional<LocationCategory> grp = dao.getLocationCategory(office, categoryId);
            if (grp.isPresent()) {
                String formatHeader = ctx.header(Header.ACCEPT);
                ContentType contentType = Formats.parseHeaderAndQueryParm(formatHeader, null);

                String result = Formats.format(contentType, grp.get());

                ctx.result(result).contentType(contentType.toString());
                requestResultSize.update(result.length());

                ctx.status(HttpServletResponse.SC_OK);
            } else {
                CdaError re = new CdaError("Cannot find requested location category id: " + categoryId
                    + " with office: " + office);

                logger.info(() -> {
                    StringBuilder builder = new StringBuilder(re.toString())
                            .append("with url:").append(ctx.fullUrl());
                    return builder.toString();
                });
                ctx.json(re).status(HttpServletResponse.SC_NOT_FOUND);
            }

        }

    }

    @OpenApi(
        description = "Create new LocationCategory",
        requestBody = @OpenApiRequestBody(
            content = {
                @OpenApiContent(from = LocationCategory.class, type = Formats.JSON)
            },
            required = true),
        method = HttpMethod.POST,
        tags = {TAG}
    )
    @Override
    public void create(Context ctx) {
        try (Timer.Context ignored = markAndTime(CREATE)){
            DSLContext dsl = getDslContext(ctx);

            String reqContentType = ctx.req.getContentType();
            String formatHeader = reqContentType != null ? reqContentType : Formats.JSON;
            String body = ctx.body();
            LocationCategory deserialize = deserialize(body, formatHeader);
            LocationCategoryDao dao = new LocationCategoryDao(dsl);
            dao.create(deserialize);
            ctx.status(HttpServletResponse.SC_CREATED);
        } catch (JsonProcessingException ex) {
            CdaError re = new CdaError("Failed to process create request");
            logger.log(Level.SEVERE, re.toString(), ex);
            ctx.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR).json(re);
        }
    }

    private LocationCategory deserialize(String body, String format) throws JsonProcessingException {
        LocationCategory retval;
        if (ContentType.equivalent(Formats.JSON, format)) {
            ObjectMapper om = JsonV1.buildObjectMapper();
            retval = om.readValue(body, LocationCategory.class);
        } else {
            throw new IllegalArgumentException("Unsupported format: " + format);
        }
        return retval;
    }

    @OpenApi(ignore = true)
    @Override
    public void update(Context ctx, @NotNull String categoryId) {
        ctx.status(HttpServletResponse.SC_NOT_IMPLEMENTED).json(CdaError.notImplemented());
    }

    @OpenApi(
        description = "Deletes requested location category",
        pathParams = {
            @OpenApiParam(name = CATEGORY_ID, description = "The location category to be deleted"),
        },
        queryParams = {
            @OpenApiParam(name = OFFICE, required = true, description = "Specifies the "
                + "owning office of the location category to be deleted"),
            @OpenApiParam(name = CASCADE_DELETE, type = Boolean.class,
                description = "Specifies whether to delete any location groups in this location category. Default: false"),
        },
        method = HttpMethod.DELETE,
        tags = {TAG}
    )
    @Override
    public void delete(Context ctx, @NotNull String categoryId) {
        try (Timer.Context ignored = markAndTime(UPDATE)){
            DSLContext dsl = getDslContext(ctx);

            LocationCategoryDao dao = new LocationCategoryDao(dsl);
            String office = requiredParam(ctx, OFFICE);
            boolean cascadeDelete = ctx.queryParamAsClass(CASCADE_DELETE, Boolean.class).getOrDefault(false);
            dao.delete(categoryId, cascadeDelete, office);
            ctx.status(HttpServletResponse.SC_NO_CONTENT);
        }
    }
}
