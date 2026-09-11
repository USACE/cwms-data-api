/*
 * MIT License
 *
 * Copyright (c) 2026 Hydrologic Engineering Center
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
import static cwms.cda.api.Controllers.CWMS_OFFICE;
import static cwms.cda.api.Controllers.FAIL_IF_EXISTS;
import static cwms.cda.api.Controllers.GROUP_ID;
import static cwms.cda.api.Controllers.IGNORE_MISSING;
import static cwms.cda.api.Controllers.IGNORE_NULLS;
import static cwms.cda.api.Controllers.INCLUDE_ASSIGNED;
import static cwms.cda.api.Controllers.OFFICE;
import static cwms.cda.api.Controllers.STATUS_200;
import static cwms.cda.api.Controllers.STATUS_404;
import static cwms.cda.api.Controllers.STATUS_501;
import static cwms.cda.api.Controllers.TIMESERIES_CATEGORY_LIKE;
import static cwms.cda.api.Controllers.TIMESERIES_GROUP_LIKE;
import static cwms.cda.api.Controllers.UPDATE;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import cwms.cda.api.errors.NotFoundException;
import cwms.cda.data.dao.TimeSeriesGroupDao;
import cwms.cda.data.dto.AssignedTimeSeries;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.timeseriesgroup.TimeSeriesGroup;
import cwms.cda.data.dto.timeseriesgroup.TimeSeriesGroupMembership;
import cwms.cda.data.dto.timeseriesgroup.TimeSeriesGroupPatch;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import io.javalin.http.Context;
import io.javalin.plugin.openapi.annotations.HttpMethod;
import io.javalin.plugin.openapi.annotations.OpenApi;
import io.javalin.plugin.openapi.annotations.OpenApiContent;
import io.javalin.plugin.openapi.annotations.OpenApiParam;
import io.javalin.plugin.openapi.annotations.OpenApiRequestBody;
import io.javalin.plugin.openapi.annotations.OpenApiResponse;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;

/**
 * Version 2 of the Timeseries Group controller. Create, delete, and retrieve are identical to v1
 * (see {@link TimeSeriesGroupController}). PATCH/update differs: instead of a full
 * {@link TimeSeriesGroup} body carrying the complete list of assigned time series, this version
 * accepts a {@link TimeSeriesGroupPatch} body whose {@link TimeSeriesGroupMembership} describes only the time
 * series ids to assign and/or unassign. This lets callers add or remove a handful of time series
 * from a (potentially very large) group without submitting the group's entire list of assigned
 * time series.
 */
public final class TimeSeriesGroupControllerV2 extends TimeSeriesGroupController {

    public TimeSeriesGroupControllerV2(MetricRegistry metrics) {
        super(metrics);
    }

    @OpenApi(
            pathParams = {
                @OpenApiParam(name = OFFICE, required = true, description = "Specifies the owning "
                        + "office of the timeseries group(s) to be included in the response. This is NOT the office of the "
                        + "category."),
            },
            queryParams = {
                @OpenApiParam(name = OFFICE, description = "Specifies the owning office of the "
                        + "timeseries assigned to the group(s) whose data is to be included in the response. If this "
                        + "field is not specified, group information for all assigned TS offices shall be returned. "
                        + "Not to be confused with the path parameter of the same name, which specifies the "
                        + "owning office of the group(s) themselves."),
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
        String groupOffice = ctx.pathParam(OFFICE);
        super.getAll(ctx, groupOffice);
    }

    @OpenApi(
            pathParams = {
                @OpenApiParam(name = OFFICE, required = true, description = "Specifies the owning "
                        + "office of the timeseries group whose data is to be included in the response."),
                @OpenApiParam(name = GROUP_ID, required = true, description = "Specifies "
                        + "the timeseries group whose data is to be included in the response")
            },
            queryParams = {
                @OpenApiParam(name = OFFICE, description = "Specifies the "
                        + "owning office of the timeseries assigned to the group whose data is to be included "
                        + "in the response. Not to be confused with the path parameter of the "
                        + "same name, which specifies the owning office of the group itself."),
                @OpenApiParam(name = CATEGORY_OFFICE_ID, description = "Specifies the owning office of the "
                        + "timeseries group category"),
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
        String groupOffice = ctx.pathParam(OFFICE);
        super.getOne(ctx, groupId, groupOffice);
    }

    @OpenApi(
        description = "Create new TimeSeriesGroup",
        pathParams = {
            @OpenApiParam(name = OFFICE, required = true, description = "Specifies the owning "
                + "office of the timeseries group to be created. Must match the office id in the "
                + "request body.")
        },
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
        String officeFromPath = ctx.pathParam(OFFICE);
        validateOffice(officeFromPath, deserializeGroup(ctx).getOfficeId());
        super.create(ctx);
    }

    @OpenApi(
        description = "Update an existing TimeSeriesGroup using time series membership changes. Allows "
            + "renaming the group, updating its description, and assigning and/or unassigning specific "
            + "time series without having to submit the group's full list of assigned time series.",
        requestBody = @OpenApiRequestBody(
            content = {
                @OpenApiContent(from = TimeSeriesGroupPatch.class, type = Formats.JSON)
            },
            required = true),
        pathParams = {
            @OpenApiParam(name = OFFICE, required = true, description = "Specifies the "
                + "office of the timeseries group, category, and time series being patched. Must "
                + "match the office id in the request body."),
            @OpenApiParam(name = GROUP_ID, required = true, description = "Specifies "
                + "the original timeseries group to rename.")
            },
        queryParams = {
            @OpenApiParam(name = IGNORE_MISSING, type = Boolean.class, description = "If true, do not fail when "
                + "a time series to assign does not exist. Default is false"),
            @OpenApiParam(name = IGNORE_NULLS, type = Boolean.class, description = "Ignore null values in the request body. "
                + IGNORE_NULLS + " is not used to unassign time series. Unassignment must be explicitly specified in the request body. "
                + "Default: true")
        },
        method = HttpMethod.PATCH,
        tags = {TAG}
    )
    @Override
    public void update(@NotNull Context ctx, @NotNull String oldGroupId) {
        try (Timer.Context ignored = markAndTime(UPDATE)) {
            DSLContext dsl = getDslContext(ctx);
            String office = ctx.pathParam(OFFICE);
            Boolean ignoreNulls = ctx.queryParamAsClass(IGNORE_NULLS, Boolean.class).getOrDefault(true);
            ContentType contentType = Formats.parseHeader(ctx.req.getContentType(), TimeSeriesGroupPatch.class);
            TimeSeriesGroupPatch patch = Formats.parseContent(contentType, ctx.body(), TimeSeriesGroupPatch.class);
            validateOffice(office, patch.getOfficeId());

            TimeSeriesGroupMembership membership = patch.getMembership();
            validateNoAssignUnassignOverlap(membership);

            TimeSeriesGroupDao dao = new TimeSeriesGroupDao(dsl);
            String categoryId = patch.getTimeSeriesCategory().getId();
            TimeSeriesGroup existingGroup = dao.getTimeSeriesGroup(office, null, null, categoryId, oldGroupId);
            if(existingGroup == null) {
                throw new NotFoundException("Time series group " + oldGroupId + " does not exist in category "
                        + categoryId + " for group office " + office);
            }

            boolean ignoreMissing = ctx.queryParamAsClass(IGNORE_MISSING, Boolean.class).getOrDefault(false);
            List<AssignedTimeSeries> newAndExistingAssignedTimeSeries = mergeAssigned(existingGroup, membership);
            // Store metadata/assignments against the group's CURRENT id - renaming (if requested) is
            // a separate step below. Targeting patch.getId() here would create a second row under
            // the new id before the rename call runs, and the rename would then collide with it.
            TimeSeriesGroup groupWithAssignment = new TimeSeriesGroup(new TimeSeriesGroup(patch.getTimeSeriesCategory(),
                    patch.getOfficeId(),
                    oldGroupId,
                    patch.getDescription(),
                    patch.getSharedAliasId(),
                    patch.getSharedRefTsId()), newAndExistingAssignedTimeSeries);
            List<CwmsId> missingTimeSeries = dao.create(groupWithAssignment, false, ignoreNulls, ignoreMissing);

            //Handle rename
            String currentGroupId = oldGroupId;
            if (!office.equalsIgnoreCase(CWMS_OFFICE) && patch.getId() != null
                    && !oldGroupId.equals(patch.getId())) {
                TimeSeriesGroup renameTarget = new TimeSeriesGroup(existingGroup.getTimeSeriesCategory(),
                        existingGroup.getOfficeId(), patch.getId(), existingGroup.getDescription(),
                        existingGroup.getSharedAliasId(), existingGroup.getSharedRefTsId());
                dao.renameTimeSeriesGroup(oldGroupId, renameTarget);
                currentGroupId = patch.getId();
            }

            //Handle unassignment
            if (membership != null) {
                List<CwmsId> unassign = membership.getUnassign();
                if (unassign != null && !unassign.isEmpty()) {
                    dao.unassignTsIds(categoryId, currentGroupId, office, unassign);
                }
            }

            respondToMissingTimeSeries(ctx, missingTimeSeries, ignoreMissing);
        }
    }

    private static List<AssignedTimeSeries> mergeAssigned(TimeSeriesGroup existingGroup, TimeSeriesGroupMembership membership) {
        Map<String, AssignedTimeSeries> byKey = new LinkedHashMap<>();
        for (AssignedTimeSeries ts : existingGroup.getAssignedTimeSeries()) {
            byKey.put(key(ts.getOfficeId(), ts.getTimeseriesId()), ts);
        }
        if (membership != null) {
            for (AssignedTimeSeries ts : membership.getAssign()) {
                byKey.put(key(ts.getOfficeId(), ts.getTimeseriesId()), ts);
            }
        }
        return new ArrayList<>(byKey.values());
    }

    /**
     * A time series can't be assigned and unassigned by the same patch - reject the request
     * up front rather than letting the outcome depend on call order.
     */
    private void validateNoAssignUnassignOverlap(TimeSeriesGroupMembership membership) {
        if (membership == null) {
            return;
        }
        List<CwmsId> unassign = membership.getUnassign();
        List<AssignedTimeSeries> assign = membership.getAssign();
        if (unassign == null || unassign.isEmpty() || assign == null || assign.isEmpty()) {
            return;
        }

        Set<String> unassignKeys = new HashSet<>();
        for (CwmsId id : unassign) {
            unassignKeys.add(key(id.getOfficeId(), id.getName()));
        }
        for (AssignedTimeSeries ts : assign) {
            String key = key(ts.getOfficeId(), ts.getTimeseriesId());
            if (unassignKeys.contains(key)) {
                String tsOffice = ts.getOfficeId();
                throw new IllegalArgumentException("Time series " + ts.getTimeseriesId() + " (office " + tsOffice
                        + ") cannot be included in both the assign and unassign lists.");
            }
        }
    }

    private static String key(String office, String tsId) {
        return office.toUpperCase() + "/" + tsId.toUpperCase();
    }

    @OpenApi(
        description = "Deletes requested time series group",
        pathParams = {
            @OpenApiParam(name = OFFICE, required = true, description = "Specifies the "
                + "owning office of the time series group to be deleted"),
            @OpenApiParam(name = GROUP_ID, description = "The time series group to be deleted"),
        },
        queryParams = {
            @OpenApiParam(name = CATEGORY_ID, required = true, description = "Specifies the "
                + "time series category of the time series group to be deleted"),
            @OpenApiParam(name = CASCADE_DELETE, type = Boolean.class,
                        description = "Specifies whether to unassign time series in this group before deleting. "
                            + "Default: false"),
        },
        method = HttpMethod.DELETE,
        tags = {TAG}
    )
    @Override
    public void delete(@NotNull Context ctx, @NotNull String groupId) {
        String office = ctx.pathParam(OFFICE);
        super.delete(ctx, groupId, office);
    }

    /**
     * v2 primary-resource standard: the office in the path must match the office embedded in
     * the request body (the group's own owning office, per {@link TimeSeriesGroup#getOfficeId()}
     * / {@link TimeSeriesGroupPatch#getOfficeId()}).
     */
    private void validateOffice(String officeFromPath, String officeFromBody) {
        if (officeFromPath == null) {
            throw new IllegalArgumentException("Office ID is required in the path parameter.");
        }
        if (!officeFromPath.equalsIgnoreCase(officeFromBody)) {
            throw new IllegalArgumentException("Office ID in path parameter does not match office ID in "
                    + "request body.");
        }
    }
}
