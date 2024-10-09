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
import cwms.cda.data.dao.LocationGroupDao;
import cwms.cda.data.dto.LocationGroup;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.csv.CsvV1LocationGroup;
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
import org.geojson.FeatureCollection;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;

public class LocationGroupController implements CrudHandler {
    public static final Logger logger = Logger.getLogger(LocationGroupController.class.getName());

    public static final String TAG = "Location Groups";
    private final MetricRegistry metrics;

    private final Histogram requestResultSize;

    public LocationGroupController(MetricRegistry metrics) {
        this.metrics = metrics;
        String className = this.getClass().getName();

        requestResultSize = this.metrics.histogram((name(className, RESULTS, SIZE)));
    }

    private Timer.Context markAndTime(String subject) {
        return Controllers.markAndTime(metrics, getClass().getName(), subject);
    }

    @OpenApi(queryParams = {
        @OpenApiParam(name = OFFICE, description = "Specifies the owning office of the "
                + "location group(s) whose data is to be included in the response. If this "
                + "field is not specified, matching location groups information from all "
                + "offices shall be returned."),
        @OpenApiParam(name = INCLUDE_ASSIGNED, type = Boolean.class, description = "Include"
                + " the assigned locations in the returned location groups. (default: false)"),
        @OpenApiParam(name = LOCATION_CATEGORY_LIKE, description = "Posix <a href=\"regexp.html\">regular expression</a> "
                + "matching against the location category id"), },
        responses = {
            @OpenApiResponse(status = STATUS_200,
                    content = {
                        @OpenApiContent(isArray = true, from = LocationGroup.class, type = Formats.JSON),
                        @OpenApiContent(isArray = true, from = CsvV1LocationGroup.class, type = Formats.CSV)
                    })
        },
        description = "Returns CWMS Location Groups Data", tags = {TAG})
    @Override
    public void getAll(@NotNull Context ctx) {

        try (final Timer.Context ignored = markAndTime(GET_ALL)) {
            DSLContext dsl = getDslContext(ctx);
            LocationGroupDao cdm = new LocationGroupDao(dsl);

            String office = ctx.queryParam(OFFICE);

            boolean includeAssigned = queryParamAsClass(ctx, new String[]{INCLUDE_ASSIGNED},
                    Boolean.class, false, metrics, name(LocationGroupController.class.getName(),
                            GET_ALL));

            String locCategoryLike = queryParamAsClass(ctx, new String[]{LOCATION_CATEGORY_LIKE},
                    String.class, null, metrics, name(LocationGroupController.class.getName(), GET_ALL));

            List<LocationGroup> grps = cdm.getLocationGroups(office, includeAssigned, locCategoryLike);

            if (!grps.isEmpty()) {
                String formatHeader = ctx.header(Header.ACCEPT);
                ContentType contentType = Formats.parseHeader(formatHeader, LocationGroup.class);

                String result = Formats.format(contentType, grps, LocationGroup.class);

                ctx.result(result);
                ctx.contentType(contentType.toString());
                requestResultSize.update(result.length());

                ctx.status(HttpServletResponse.SC_OK);
            } else {
                CdaError re = new CdaError("No location groups for office provided");
                logger.info(() -> re + System.lineSeparator() + "for request " + ctx.fullUrl());
                ctx.status(HttpServletResponse.SC_NOT_FOUND).json(re);
            }

        }

    }

    @OpenApi(
        pathParams = {
            @OpenApiParam(name = GROUP_ID, required = true, description = "Specifies "
                    + "the location_group whose data is to be included in the response")
        },
        queryParams = {
            @OpenApiParam(name = OFFICE, required = true, description = "Specifies the "
                    + "owning office of the location group whose data is to be included "
                    + "in the response."),
            @OpenApiParam(name = CATEGORY_ID, required = true, description = "Specifies"
                    + " the category containing the location group whose data is to be "
                    + "included in the response."),
        },
        responses = {
            @OpenApiResponse(status = STATUS_200, content = {
                @OpenApiContent(from = LocationGroup.class, type = Formats.JSON),
                @OpenApiContent(from = CsvV1LocationGroup.class, type = Formats.CSV),
                @OpenApiContent(type = Formats.GEOJSON)
            })
        },
        description = "Retrieves requested Location Group", tags = {TAG})
    @Override
    public void getOne(@NotNull Context ctx, @NotNull String groupId) {

        try (final Timer.Context ignored = markAndTime(GET_ONE)) {
            DSLContext dsl = getDslContext(ctx);
            LocationGroupDao cdm = new LocationGroupDao(dsl);
            String office = requiredParam(ctx, OFFICE);
            String categoryId = requiredParam(ctx, CATEGORY_ID);

            String formatHeader = ctx.header(Header.ACCEPT);
            String result;
            ContentType contentType;
            if (formatHeader != null && formatHeader.contains(Formats.GEOJSON)) {
                contentType = new ContentType(Formats.GEOJSON);
                FeatureCollection fc = cdm.buildFeatureCollectionForLocationGroup(office,
                        categoryId, groupId, "EN");
                ObjectMapper mapper = ctx.appAttribute("ObjectMapper");
                result = mapper.writeValueAsString(fc);
            } else {
                contentType = Formats.parseHeader(formatHeader, LocationGroup.class);
                Optional<LocationGroup> grp = cdm.getLocationGroup(office, categoryId, groupId);
                if (grp.isPresent()) {
                    result = Formats.format(contentType, grp.get());
                } else {
                    CdaError re = new CdaError("Unable to find location group based on parameters given");
                    logger.info(() -> re + System.lineSeparator() + "for request " + ctx.fullUrl());
                    ctx.status(HttpServletResponse.SC_NOT_FOUND).json(re);
                    return;
                }

            }
            ctx.result(result);
            ctx.contentType(contentType.toString());

            requestResultSize.update(result.length());

            ctx.status(HttpServletResponse.SC_OK);
        } catch (JsonProcessingException e) {
            CdaError re = new CdaError("Failed to process request");
            logger.log(Level.SEVERE, re.toString(), e);
            ctx.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR).json(re);
        }

    }

    @OpenApi(
        description = "Create new LocationGroup",
        requestBody = @OpenApiRequestBody(
            content = {
                @OpenApiContent(from = LocationGroup.class, type = Formats.JSON)
            },
            required = true),
        method = HttpMethod.POST,
        tags = {TAG}
    )
    @Override
    public void create(@NotNull Context ctx) {
        try (Timer.Context ignored = markAndTime(CREATE)) {
            DSLContext dsl = getDslContext(ctx);

            String formatHeader = ctx.req.getContentType();
            String body = ctx.body();
            ContentType contentType = Formats.parseHeader(formatHeader, LocationGroup.class);
            LocationGroup deserialize = Formats.parseContent(contentType, body, LocationGroup.class);
            LocationGroupDao dao = new LocationGroupDao(dsl);
            dao.create(deserialize);
            ctx.status(HttpServletResponse.SC_CREATED);
        }
    }

    @OpenApi(
        description = "Update existing LocationGroup",
        requestBody = @OpenApiRequestBody(
            content = {
                @OpenApiContent(from = LocationGroup.class, type = Formats.JSON)
            },
            required = true),
        queryParams = {
            @OpenApiParam(name = REPLACE_ASSIGNED_LOCS, type = Boolean.class, description = "Specifies whether to "
                + "unassign all existing locations before assigning new locations specified in the content body "
                + "Default: false"),
            @OpenApiParam(name = OFFICE, required = true, description = "Specifies the "
                + "user office making the request"),
        },
        method = HttpMethod.PATCH,
        tags = {TAG}
    )
    @Override
    public void update(@NotNull Context ctx, @NotNull String oldGroupId) {

        try (Timer.Context ignored = markAndTime(CREATE)) {
            DSLContext dsl = getDslContext(ctx);
            final String CWMS_OFFICE = "CWMS";
            String formatHeader = ctx.req.getContentType();
            String body = ctx.body();
            String office = requiredParam(ctx, OFFICE);
            ContentType contentType = Formats.parseHeader(formatHeader, LocationGroup.class);
            LocationGroup deserialize = Formats.parseContent(contentType, body, LocationGroup.class);
            boolean replaceAssignedLocs = ctx.queryParamAsClass(REPLACE_ASSIGNED_LOCS,
                    Boolean.class).getOrDefault(false);
            LocationGroupDao locationGroupDao = new LocationGroupDao(dsl);
            if (!office.equalsIgnoreCase(CWMS_OFFICE) && !oldGroupId.equals(deserialize.getId())) {
                locationGroupDao.renameLocationGroup(oldGroupId, deserialize);
            }
            if (replaceAssignedLocs) {
                locationGroupDao.unassignAllLocs(deserialize, office);
            }
            locationGroupDao.assignLocs(deserialize, office);
            ctx.status(HttpServletResponse.SC_OK);
        }
    }

    @OpenApi(
        description = "Deletes requested location group",
        pathParams = {
            @OpenApiParam(name = GROUP_ID, description = "The location group to be deleted"),
        },
        queryParams = {
            @OpenApiParam(name = CATEGORY_ID, required = true, description = "Specifies the "
                + "location category of the location group to be deleted"),
            @OpenApiParam(name = OFFICE, required = true, description = "Specifies the "
                + "owning office of the location group to be deleted"),
            @OpenApiParam(name = CASCADE_DELETE, type = Boolean.class,
                description = "Specifies whether to specifies whether to unassign any "
                        + "location assignments. Default: false"),
        },
        method = HttpMethod.DELETE,
        tags = {TAG}
    )
    @Override
    public void delete(@NotNull Context ctx, @NotNull String groupId) {
        try (Timer.Context ignored = markAndTime(UPDATE)) {
            DSLContext dsl = getDslContext(ctx);

            LocationGroupDao dao = new LocationGroupDao(dsl);
            String office = ctx.queryParam(OFFICE);
            String categoryId = ctx.queryParam(CATEGORY_ID);
            boolean cascadeDelete = ctx.queryParamAsClass(CASCADE_DELETE, Boolean.class).getOrDefault(false);
            dao.delete(categoryId, groupId, cascadeDelete, office);
            ctx.status(HttpServletResponse.SC_NO_CONTENT);
        }
    }
}
