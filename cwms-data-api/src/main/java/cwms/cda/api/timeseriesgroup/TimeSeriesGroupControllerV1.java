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

package cwms.cda.api.timeseriesgroup;

import static cwms.cda.api.Controllers.CASCADE_DELETE;
import static cwms.cda.api.Controllers.CATEGORY_ID;
import static cwms.cda.api.Controllers.CATEGORY_OFFICE_ID;
import static cwms.cda.api.Controllers.CREATE;
import static cwms.cda.api.Controllers.CWMS_OFFICE;
import static cwms.cda.api.Controllers.FAIL_IF_EXISTS;
import static cwms.cda.api.Controllers.GROUP_ID;
import static cwms.cda.api.Controllers.GROUP_OFFICE_ID;
import static cwms.cda.api.Controllers.IGNORE_MISSING;
import static cwms.cda.api.Controllers.IGNORE_NULLS;
import static cwms.cda.api.Controllers.INCLUDE_ASSIGNED;
import static cwms.cda.api.Controllers.OFFICE;
import static cwms.cda.api.Controllers.REPLACE_ASSIGNED_TS;
import static cwms.cda.api.Controllers.STATUS_200;
import static cwms.cda.api.Controllers.STATUS_404;
import static cwms.cda.api.Controllers.STATUS_501;
import static cwms.cda.api.Controllers.TIMESERIES_CATEGORY_LIKE;
import static cwms.cda.api.Controllers.TIMESERIES_GROUP_LIKE;
import static cwms.cda.api.Controllers.requiredParam;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.cda.data.dao.TimeSeriesGroupDao;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.timeseriesgroup.TimeSeriesGroup;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import io.javalin.http.Context;
import io.javalin.plugin.openapi.annotations.HttpMethod;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiRequestBody;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import java.util.List;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;

/**
 * Version 1 of the Timeseries Group controller. Create, delete, and retrieve behavior is shared
 * with {@link TimeSeriesGroupControllerV2} via {@link TimeSeriesGroupController}.
 * This version's PATCH/update accepts a full {@link TimeSeriesGroup} body; the assigned time
 * series in that body either replace or are added to the group's existing assignments.
 */
public class TimeSeriesGroupControllerV1 extends TimeSeriesGroupController {

    public TimeSeriesGroupControllerV1(MetricRegistry metrics) {
        super(metrics);
    }

    @OpenApi(
            queryParams = {
                @OpenApiParam(name = OFFICE, description = "Specifies the owning office of the "
                        + "timeseries assigned to the group(s) whose data is to be included in the response. If this "
                        + "field is not specified, group information for all assigned TS offices shall be returned."),
                @OpenApiParam(name = GROUP_OFFICE_ID, description = "Specifies the owning office of the "
                            + "timeseries group"),
                @OpenApiParam(name = INCLUDE_ASSIGNED, type = Boolean.class, description = "Include"
                        + " the assigned timeseries in the returned timeseries groups. (default: true)"),
                @OpenApiParam(name = TIMESERIES_CATEGORY_LIKE, description = "Posix <a href=\"regexp.html\">regular expression</a> "
                        + "matching against the timeseries category id"),
                @OpenApiParam(name = CATEGORY_OFFICE_ID, description = "Specifies the owning office of the "
                    + "timeseries group category"),
                @OpenApiParam(name = TIMESERIES_GROUP_LIKE, description = "Posix <a href=\"regexp.html\">regular expression</a> "
                        + "matching against the timeseries group id")
            },
            responses = {
                @OpenApiResponse(status = STATUS_200,
                        content = {@OpenApiContent(isArray = true, from =
                                TimeSeriesGroup.class, type = Formats.JSON)
                        }),
                @OpenApiResponse(status = STATUS_404, description = "Based on the combination of "
                        + "inputs provided the timeseries group(s) were not found."),
                @OpenApiResponse(status = STATUS_501, description = "request format is not "
                        + "implemented")}, description = "Returns CWMS Timeseries Groups Data",
            tags = {TAG})
    @Override
    public void getAll(@NotNull Context ctx) {
        String groupOffice = ctx.queryParam(GROUP_OFFICE_ID);
        super.getAll(ctx, groupOffice);
    }

    @OpenApi(
            pathParams = {
                @OpenApiParam(name = GROUP_ID, required = true, description = "Specifies "
                        + "the timeseries group whose data is to be included in the response")
            },
            queryParams = {
                @OpenApiParam(name = OFFICE, description = "Specifies the "
                        + "owning office of the timeseries assigned to the group whose data is to be included"
                        + " in the response. This will limit the assigned timeseries returned to only those"
                        + " assigned to the specified office."),
                @OpenApiParam(name = CATEGORY_OFFICE_ID, description = "Specifies the owning office of the "
                        + "timeseries group category"),
                @OpenApiParam(name = GROUP_OFFICE_ID, description = "Specifies the owning office of the "
                        + "timeseries group"),
                @OpenApiParam(name = CATEGORY_ID, description = "Specifies"
                        + " the category containing the timeseries group whose data is to be "
                        + "included in the response."),
            },
            responses = {
                @OpenApiResponse(status = STATUS_200, content = {
                    @OpenApiContent(from = TimeSeriesGroup.class, type = Formats.JSON),
                })
            },
            description = "Retrieves requested timeseries group", tags = {"Timeseries Groups"})
    @Override
    public void getOne(@NotNull Context ctx, @NotNull String groupId) {
        String groupOffice = ctx.queryParam(GROUP_OFFICE_ID);
        super.getOne(ctx, groupId, groupOffice);
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
            @OpenApiParam(name = IGNORE_MISSING, type = Boolean.class, description = "If true, do not fail when "
                + "attempting to assign a time series that does not exist to the group"),
            @OpenApiParam(name = IGNORE_NULLS, type = Boolean.class,
                        description = "Ignore null values in the request body.  Caution, if " + FAIL_IF_EXISTS
                                + " is false and " + IGNORE_NULLS + " is false, then the create will proceed whether "
                                + "there was an existing group or not.  If there was an existing group with a "
                                + "description and the provided body does not specify a description (its null) the "
                                + "combination of flags will cause the database to replace the description with null. "
                                + "If " + IGNORE_NULLS + " is false and the provided body does not specify the "
                                + "list of assigned time series this will result in the database replacing the list "
                                + "with an empty list."
                                + "Default: true")
        },
        method = HttpMethod.POST,
        tags = {TAG}
    )
    @Override
    public void create(@NotNull Context ctx) {
        super.create(ctx);
    }

    @OpenApi(
        description = "Update existing TimeSeriesGroup. Allows for renaming of the group, "
            + "assigning new time series, and unassigning all time series from the group.",
        requestBody = @OpenApiRequestBody(
            content = {
                @OpenApiContent(from = TimeSeriesGroup.class, type = Formats.JSON)
            },
            required = true),
        pathParams = {
            @OpenApiParam(name = GROUP_ID, required = true, description = "Specifies "
                + "the original timeseries group to rename.")
            },
        queryParams = {
            @OpenApiParam(name = REPLACE_ASSIGNED_TS, type = Boolean.class, description = "Specifies whether to "
                + "unassign all existing time series before assigning new time series specified in the content body "
                + "Default: false"),
            @OpenApiParam(name = IGNORE_MISSING, type = Boolean.class, description = "If true, do not fail when "
                + "time series to assign does not exist. Default is false"),
            @OpenApiParam(name = OFFICE, required = true, description = "Specifies the "
                + "office of the user making the request. This is the office that the timeseries, group, and category "
                + "belong to. If the group and/or category belong to the CWMS office, "
                + "this only identifies the timeseries."),
        },
        method = HttpMethod.PATCH,
        tags = {TAG}
    )
    @Override
    public void update(@NotNull Context ctx, @NotNull String oldGroupId) {
        try (Timer.Context ignored = markAndTime(CREATE)) {
            DSLContext dsl = getDslContext(ctx);
            String formatHeader = ctx.req.getContentType();
            String body = ctx.body();
            String office = requiredParam(ctx, OFFICE);
            ContentType contentType = Formats.parseHeader(formatHeader, TimeSeriesGroup.class);
            TimeSeriesGroup group = Formats.parseContent(contentType, body, TimeSeriesGroup.class);
            boolean replaceAssignedTs = ctx.queryParamAsClass(REPLACE_ASSIGNED_TS, Boolean.class)
                .getOrDefault(false);
            TimeSeriesGroupDao timeSeriesGroupDao = new TimeSeriesGroupDao(dsl);
            TimeSeriesGroup existingGroup = timeSeriesGroupDao.getTimeSeriesGroup(office, null,
                null, group.getTimeSeriesCategory().getId(), oldGroupId);
            updateDescriptionIfChanged(timeSeriesGroupDao, existingGroup, group.getDescription());
            if (!office.equalsIgnoreCase(CWMS_OFFICE) && !oldGroupId.equals(group.getId())) {
                timeSeriesGroupDao.renameTimeSeriesGroup(oldGroupId, group);
            }
            if (replaceAssignedTs) {
                timeSeriesGroupDao.unassignForOffice(group.getTimeSeriesCategory().getId(), group.getId(),
                    group.getOfficeId(), office);
            }
            boolean ignoreMissing = ctx.queryParamAsClass(IGNORE_MISSING, Boolean.class).getOrDefault(false);
            List<CwmsId> missingTimeSeries = timeSeriesGroupDao.assignTs(group, office, ignoreMissing);
            respondToMissingTimeSeries(ctx, missingTimeSeries, ignoreMissing);
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
            @OpenApiParam(name = CASCADE_DELETE, type = Boolean.class,
                        description = "Specifies whether to unassign time series in this group before deleting. "
                            + "Default: false"),
        },
        method = HttpMethod.DELETE,
        tags = {TAG}
    )
    @Override
    public void delete(@NotNull Context ctx, @NotNull String groupId) {
        String office = requiredParam(ctx, OFFICE);
        super.delete(ctx, groupId, office);
    }
}
