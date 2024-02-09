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
import cwms.cda.data.dao.TimeSeriesGroupDao;
import cwms.cda.data.dto.TimeSeriesGroup;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.json.JsonV1;
import io.javalin.apibuilder.CrudHandler;
import io.javalin.core.util.Header;
import io.javalin.http.Context;
import io.javalin.http.HttpCode;
import io.javalin.plugin.openapi.annotations.HttpMethod;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiRequestBody;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.servlet.http.HttpServletResponse;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.jooq.DSLContext;

public class TimeSeriesGroupController implements CrudHandler {
    public static final Logger logger = Logger.getLogger(TimeSeriesGroupController.class.getName());
    public static final String TAG = "Timeseries Groups";

    private final MetricRegistry metrics;

    private final Histogram requestResultSize;

    public TimeSeriesGroupController(MetricRegistry metrics) {
        this.metrics = metrics;
        String className = this.getClass().getName();

        requestResultSize = this.metrics.histogram((name(className, RESULTS, SIZE)));
    }

    private Timer.Context markAndTime(String subject) {
        return Controllers.markAndTime(metrics, getClass().getName(), subject);
    }

    @OpenApi(queryParams = {
            @OpenApiParam(name = OFFICE, description = "Specifies the owning office of the "
                    + "timeseries group(s) whose data is to be included in the response. If this "
                    + "field is not specified, matching timeseries groups information from all "
                    + "offices shall be returned."),
            @OpenApiParam(name = INCLUDE_ASSIGNED, type = Boolean.class, description = "Include"
                    + " the assigned timeseries in the returned timeseries groups. (default: true)"),
            @OpenApiParam(name = TIMESERIES_CATEGORY_LIKE, description = "Posix <a href=\"regexp.html\">regular expression</a> "
                    + "matching against the timeseries category id"),
            @OpenApiParam(name = TIMESERIES_GROUP_LIKE, description = "Posix <a href=\"regexp.html\">regular expression</a> "
                    + "matching against the timeseries group id")
    },
            responses = {
                    @OpenApiResponse(status = STATUS_200,
                            content = {@OpenApiContent(isArray = true, from =
                                    TimeSeriesGroup.class, type = Formats.JSON)
                            }
                    ),
                    @OpenApiResponse(status = STATUS_404, description = "Based on the combination of "
                            + "inputs provided the timeseries group(s) were not found."),
                    @OpenApiResponse(status = STATUS_501, description = "request format is not "
                            + "implemented")}, description = "Returns CWMS Timeseries Groups "
            + "Data", tags = {TAG})
    @Override
    public void getAll(Context ctx) {
        try (final Timer.Context timeContext = markAndTime(GET_ALL)){
            DSLContext dsl = getDslContext(ctx);

            TimeSeriesGroupDao dao = new TimeSeriesGroupDao(dsl);
            String office = ctx.queryParam(OFFICE);

            boolean includeAssigned = queryParamAsClass(ctx, new String[]{INCLUDE_ASSIGNED},
                    Boolean.class, true, metrics, name(TimeSeriesGroupController.class.getName(),
                            GET_ALL));
            String tsCategoryLike = queryParamAsClass(ctx, new String[]{TIMESERIES_CATEGORY_LIKE},
                    String.class, null, metrics, name(TimeSeriesGroupController.class.getName(), GET_ALL));
            String tsGroupLike = queryParamAsClass(ctx, new String[]{TIMESERIES_GROUP_LIKE},
                    String.class, null, metrics, name(TimeSeriesGroupController.class.getName(), GET_ALL));

            List<TimeSeriesGroup> grps = dao.getTimeSeriesGroups(office, includeAssigned, tsCategoryLike, tsGroupLike);
            if (grps.isEmpty()) {
                CdaError re = new CdaError("No data found for The provided office");
                logger.info(() -> re + " for request " + ctx.fullUrl());
                ctx.status(HttpCode.NOT_FOUND).json(re);
            } else {
                String formatHeader = ctx.header(Header.ACCEPT);
                ContentType contentType = Formats.parseHeaderAndQueryParm(formatHeader, null);

                String result = Formats.format(contentType, grps, TimeSeriesGroup.class);

                ctx.result(result).contentType(contentType.toString());
                requestResultSize.update(result.length());

                ctx.status(HttpServletResponse.SC_OK);
            }
        }

    }

    @OpenApi(
            pathParams = {
                    @OpenApiParam(name = GROUP_ID, required = true, description = "Specifies "
                            + "the timeseries group whose data is to be included in the response")
            },
            queryParams = {
                    @OpenApiParam(name = OFFICE, required = true, description = "Specifies the "
                            + "owning office of the timeseries group whose data is to be included"
                            + " in the response."),
                    @OpenApiParam(name = CATEGORY_ID, required = true, description = "Specifies"
                            + " the category containing the timeseries group whose data is to be "
                            + "included in the response."),
            },
            responses = {
                    @OpenApiResponse(status = STATUS_200, content = {
                            @OpenApiContent(from = TimeSeriesGroup.class, type = Formats.JSON),
                    }

                    )},
            description = "Retrieves requested timeseries group", tags = {"Timeseries Groups"})
    @Override
    public void getOne(Context ctx, String groupId) {
        try (final Timer.Context timeContext = markAndTime(GET_ONE)){
            DSLContext dsl = getDslContext(ctx);

            TimeSeriesGroupDao dao = new TimeSeriesGroupDao(dsl);
            String office = ctx.queryParam(OFFICE);
            String categoryId = ctx.queryParam(CATEGORY_ID);

            String formatHeader = ctx.header(Header.ACCEPT);
            ContentType contentType = Formats.parseHeaderAndQueryParm(formatHeader, null);

            TimeSeriesGroup group = null;
            List<TimeSeriesGroup> timeSeriesGroups = dao.getTimeSeriesGroups(office, categoryId,
                    groupId);
            if (timeSeriesGroups != null && !timeSeriesGroups.isEmpty()) {
                if (timeSeriesGroups.size() == 1) {
                    group = timeSeriesGroups.get(0);
                } else {
                    // An error. [office, categoryId, groupId] should have, at most, one match
                    String message = String.format(
                            "Multiple TimeSeriesGroups returned from getTimeSeriesGroups "
                                    + "for:%s category:%s groupId:%s At most one match was "
                                    + "expected. Found:%s",
                            office, categoryId, groupId, timeSeriesGroups);
                    throw new IllegalArgumentException(message);
                }
            }
            if (group != null) {
                String result = Formats.format(contentType, group);


                ctx.result(result);
                ctx.contentType(contentType.toString());
                requestResultSize.update(result.length());

                ctx.status(HttpServletResponse.SC_OK);
            } else {
                CdaError re = new CdaError("Unable to find group based on parameters given");
                logger.info(() -> re + System.lineSeparator() + "for request " + ctx.fullUrl());
                ctx.status(HttpServletResponse.SC_NOT_FOUND).json(re);
            }

        }

    }

    @OpenApi(
        description = "Create new TimeSeriesGroup",
        requestBody = @OpenApiRequestBody(
            content = {
                @OpenApiContent(from = TimeSeriesGroup.class, type = Formats.JSON)
            },
            required = true),
        queryParams = {
            @OpenApiParam(name = FAIL_IF_EXISTS, type = Boolean.class,
                description = "Create will fail if provided ID already exists. Default: true"),
        },
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
            TimeSeriesGroup deserialize = deserialize(body, formatHeader);
            boolean failIfExists = ctx.queryParamAsClass(FAIL_IF_EXISTS, Boolean.class).getOrDefault(true);
            TimeSeriesGroupDao dao = new TimeSeriesGroupDao(dsl);
            dao.create(deserialize, failIfExists);
            ctx.status(HttpServletResponse.SC_CREATED);
        } catch (JsonProcessingException ex) {
            CdaError re = new CdaError("Failed to process create request");
            logger.log(Level.SEVERE, re.toString(), ex);
            ctx.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR).json(re);
        }
    }

    private TimeSeriesGroup deserialize(String body, String format) throws JsonProcessingException {
        TimeSeriesGroup retval;
        if (ContentType.equivalent(Formats.JSON, format)) {
            ObjectMapper om = JsonV1.buildObjectMapper();
            retval = om.readValue(body, TimeSeriesGroup.class);
        } else {
            throw new IllegalArgumentException("Unsupported format: " + format);
        }
        return retval;
    }

    @OpenApi(
        description = "Update existing TimeSeriesGroup",
        requestBody = @OpenApiRequestBody(
            content = {
                @OpenApiContent(from = TimeSeriesGroup.class, type = Formats.JSON)
            },
            required = true),
        queryParams = {
            @OpenApiParam(name = REPLACE_ASSIGNED_TS, type = Boolean.class, description = "Specifies whether to "
                + "unassign all existing time series before assigning new time series specified in the content body "
                + "Default: false"),
            @OpenApiParam(name = OFFICE, required = true, description = "Specifies the "
                + "owning office of the time series group to be updated"),
        },
        method = HttpMethod.PATCH,
        tags = {TAG}
    )
    @Override
    public void update(Context ctx, String oldGroupId) {

        try (Timer.Context ignored = markAndTime(CREATE)){
            DSLContext dsl = getDslContext(ctx);

            String reqContentType = ctx.req.getContentType();
            String formatHeader = reqContentType != null ? reqContentType : Formats.JSON;
            String body = ctx.body();
            TimeSeriesGroup deserialize = deserialize(body, formatHeader);
            boolean replaceAssignedTs = ctx.queryParamAsClass(REPLACE_ASSIGNED_TS, Boolean.class).getOrDefault(false);
            TimeSeriesGroupDao timeSeriesGroupDao = new TimeSeriesGroupDao(dsl);
            if (!oldGroupId.equals(deserialize.getId())) {
                timeSeriesGroupDao.renameTimeSeriesGroup(oldGroupId, deserialize);
            }
            if (replaceAssignedTs) {
                timeSeriesGroupDao.unassignAllTs(deserialize);
            }
            timeSeriesGroupDao.assignTs(deserialize);
            ctx.status(HttpServletResponse.SC_ACCEPTED);
        } catch (JsonProcessingException ex) {
            CdaError re = new CdaError("Failed to process create request");
            logger.log(Level.SEVERE, re.toString(), ex);
            ctx.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR).json(re);
        }
    }

    @OpenApi(
        description = "Deletes requested time series group",
        pathParams = {
            @OpenApiParam(name = GROUP_ID, description = "The time series group to be deleted"),
        },
        queryParams = {
            @OpenApiParam(name = CATEGORY_ID, required = true, description = "Specifies the "
                + "time series category of the time series group to be deleted"),
            @OpenApiParam(name = OFFICE, required = true, description = "Specifies the "
                + "owning office of the time series group to be deleted"),
        },
        method = HttpMethod.DELETE,
        tags = {TAG}
    )
    @Override
    public void delete(Context ctx, @NonNull String groupId) {
        try (Timer.Context ignored = markAndTime(UPDATE)){
            DSLContext dsl = getDslContext(ctx);

            TimeSeriesGroupDao dao = new TimeSeriesGroupDao(dsl);
            String office = ctx.queryParam(OFFICE);
            String categoryId = ctx.queryParam(CATEGORY_ID);
            dao.delete(categoryId, groupId, office);
            ctx.status(HttpServletResponse.SC_NO_CONTENT);
        }
    }
}
