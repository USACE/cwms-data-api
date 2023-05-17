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

package cwms.radar.api;

import static com.codahale.metrics.MetricRegistry.name;
import static cwms.radar.api.Controllers.*;
import static cwms.radar.data.dao.JooqDao.getDslContext;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.radar.api.errors.RadarError;
import cwms.radar.data.dao.LocationCategoryDao;
import cwms.radar.data.dto.LocationCategory;
import cwms.radar.formatters.ContentType;
import cwms.radar.formatters.Formats;
import io.javalin.apibuilder.CrudHandler;
import io.javalin.core.util.Header;
import io.javalin.http.Context;
import io.javalin.plugin.openapi.annotations.HttpMethod;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import java.util.List;
import java.util.Optional;
import java.util.logging.Logger;
import javax.servlet.http.HttpServletResponse;
import org.jooq.DSLContext;

public class LocationCategoryController implements CrudHandler {
    public static final Logger logger =
            Logger.getLogger(LocationCategoryController.class.getName());
    private static final String TAG = "Location Categories-Beta";

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
            responses = {@OpenApiResponse(status = "200",
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

        try (final Timer.Context timeContext = markAndTime(GET_ALL);
             DSLContext dsl = getDslContext(ctx)) {
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
                final RadarError re = new RadarError("Cannot requested location category for "
                        + "office provided");

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
                    @OpenApiResponse(status = "200",
                            content = {
                                    @OpenApiContent(from = LocationCategory.class, type =
                                            Formats.JSON)
                            }
                    )
            },
            description = "Retrieves requested Location Category",
            tags = {TAG})
    @Override
    public void getOne(Context ctx, String categoryId) {

        try (final Timer.Context timeContext = markAndTime(GET_ONE);
             DSLContext dsl = getDslContext(ctx)) {
            LocationCategoryDao dao = new LocationCategoryDao(dsl);
            String office = ctx.queryParam(OFFICE);

            Optional<LocationCategory> grp = dao.getLocationCategory(office, categoryId);
            if (grp.isPresent()) {
                String formatHeader = ctx.header(Header.ACCEPT);
                ContentType contentType = Formats.parseHeaderAndQueryParm(formatHeader, null);

                String result = Formats.format(contentType, grp.get());

                ctx.result(result).contentType(contentType.toString());
                requestResultSize.update(result.length());

                ctx.status(HttpServletResponse.SC_OK);
            } else {
                final RadarError re = new RadarError("Cannot requested location category id");

                logger.info(() -> {
                    StringBuilder builder = new StringBuilder(re.toString())
                            .append("with url:").append(ctx.fullUrl());
                    return builder.toString();
                });
                ctx.json(re).status(HttpServletResponse.SC_NOT_FOUND);
            }

        }

    }

    @OpenApi(ignore = true)
    @Override
    public void create(Context ctx) {
        ctx.status(HttpServletResponse.SC_NOT_IMPLEMENTED).json(RadarError.notImplemented());
    }

    @OpenApi(ignore = true)
    @Override
    public void update(Context ctx, String categoryId) {
        ctx.status(HttpServletResponse.SC_NOT_IMPLEMENTED).json(RadarError.notImplemented());
    }

    @OpenApi(
        description = "Deletes requested location category",
        pathParams = {
            @OpenApiParam(name = CATEGORY_ID, description = "The location category to be deleted"),
        },
        queryParams = {
            @OpenApiParam(name = OFFICE, required = true, description = "Specifies the "
                + "owning office of the timeseries identifier to be deleted"),
            @OpenApiParam(name = CASCADE_DELETE, type = Boolean.class, required = false,
                description = "Specifies whether to delete any location groups in this location category. Default: false"),
        },
        method = HttpMethod.DELETE,
        tags = {TAG}
    )
    @Override
    public void delete(Context ctx, String categoryId) {
        try (Timer.Context ignored = markAndTime(UPDATE);
             DSLContext dsl = getDslContext(ctx)) {
            LocationCategoryDao dao = new LocationCategoryDao(dsl);
            String office = ctx.queryParam(OFFICE);
            boolean cascadeDelete = ctx.queryParamAsClass(CASCADE_DELETE, Boolean.class).getOrDefault(false);
            dao.delete(categoryId, cascadeDelete, office);
            ctx.status(HttpServletResponse.SC_NO_CONTENT);
        }
    }
}
