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

import static com.codahale.metrics.MetricRegistry.name;
import static cwms.cda.api.Controllers.CASCADE_DELETE;
import static cwms.cda.api.Controllers.CATEGORY_ID;
import static cwms.cda.api.Controllers.CATEGORY_OFFICE_ID;
import static cwms.cda.api.Controllers.CREATE;
import static cwms.cda.api.Controllers.CWMS_OFFICE;
import static cwms.cda.api.Controllers.FAIL_IF_EXISTS;
import static cwms.cda.api.Controllers.GET_ALL;
import static cwms.cda.api.Controllers.GET_ONE;
import static cwms.cda.api.Controllers.IGNORE_MISSING;
import static cwms.cda.api.Controllers.IGNORE_NULLS;
import static cwms.cda.api.Controllers.INCLUDE_ASSIGNED;
import static cwms.cda.api.Controllers.OFFICE;
import static cwms.cda.api.Controllers.TIMESERIES_CATEGORY_LIKE;
import static cwms.cda.api.Controllers.TIMESERIES_GROUP_LIKE;
import static cwms.cda.api.Controllers.UPDATE;
import static cwms.cda.api.Controllers.queryParamAsClass;
import static cwms.cda.api.Controllers.requiredParam;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.flogger.FluentLogger;
import cwms.cda.api.BaseCrudHandler;
import cwms.cda.api.errors.CdaError;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dao.TimeSeriesGroupDao;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.timeseriesgroup.TimeSeriesGroup;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import io.javalin.core.util.Header;
import io.javalin.http.Context;
import io.javalin.http.HttpCode;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.servlet.http.HttpServletResponse;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;

public abstract class TimeSeriesGroupController extends BaseCrudHandler {
    private static final FluentLogger logger = FluentLogger.forEnclosingClass();
    protected static final String TAG = "Timeseries Groups";

    protected TimeSeriesGroupController(MetricRegistry metrics) {
        super(metrics);
    }

    protected DSLContext getDslContext(Context ctx) {
        return JooqDao.getDslContext(ctx);
    }

    protected void getAll(@NotNull Context ctx, String groupOffice) {
        try (final Timer.Context ignored = markAndTime(GET_ALL)) {
            DSLContext dsl = getDslContext(ctx);

            TimeSeriesGroupDao dao = new TimeSeriesGroupDao(dsl);
            String tsOffice = ctx.queryParam(OFFICE);
            String categoryOffice = ctx.queryParam(CATEGORY_OFFICE_ID);

            boolean includeAssigned = queryParamAsClass(ctx, new String[]{INCLUDE_ASSIGNED},
                    Boolean.class, true, getMetrics(), name(getClass().getName(), GET_ALL));
            String tsCategoryLike = queryParamAsClass(ctx, new String[]{TIMESERIES_CATEGORY_LIKE},
                    String.class, null, getMetrics(), name(getClass().getName(), GET_ALL));
            String tsGroupLike = queryParamAsClass(ctx, new String[]{TIMESERIES_GROUP_LIKE},
                    String.class, null, getMetrics(), name(getClass().getName(), GET_ALL));

            List<TimeSeriesGroup> grps = dao.getTimeSeriesGroups(tsOffice, groupOffice, categoryOffice,
                    includeAssigned, tsCategoryLike, tsGroupLike);
            if (grps.isEmpty()) {
                CdaError re = new CdaError("No data found for The provided office");
                logger.atInfo().log("%s for request %s", re, ctx.fullUrl());
                ctx.status(HttpCode.NOT_FOUND).json(re);
            } else {
                String formatHeader = ctx.header(Header.ACCEPT);
                ContentType contentType = Formats.parseHeader(formatHeader, TimeSeriesGroup.class);

                String result = Formats.format(contentType, grps, TimeSeriesGroup.class);

                updateResultSize(result);

                ctx.status(HttpServletResponse.SC_OK);
                ctx.contentType(contentType.toString());

                byte[] bytes = result.getBytes();
                ctx.header(Header.CONTENT_LENGTH, String.valueOf(bytes.length));
                ctx.res.getOutputStream().write(bytes);
            }
        } catch (IOException ex) {
            CdaError re = new CdaError("Failure to process request to retrieve time series groups");
            logger.atSevere().withCause(ex).log("Failed to process request to retrieve time series groups");
            ctx.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR).json(re);
        }
    }

    protected void getOne(@NotNull Context ctx, @NotNull String groupId, String groupOffice) {
        try (final Timer.Context ignored = markAndTime(GET_ONE)) {
            DSLContext dsl = getDslContext(ctx);

            TimeSeriesGroupDao dao = new TimeSeriesGroupDao(dsl);
            String tsOffice = ctx.queryParam(OFFICE);
            String categoryId = ctx.queryParam(CATEGORY_ID);
            String categoryOffice = ctx.queryParam(CATEGORY_OFFICE_ID);

            String formatHeader = ctx.header(Header.ACCEPT);
            ContentType contentType = Formats.parseHeader(formatHeader, TimeSeriesGroup.class);

            TimeSeriesGroup group = dao.getTimeSeriesGroup(tsOffice, groupOffice, categoryOffice, categoryId, groupId);

            if (group != null) {
                String result = Formats.format(contentType, group);

                ctx.contentType(contentType.toString());
                updateResultSize(result);

                ctx.status(HttpServletResponse.SC_OK);

                byte[] bytes = result.getBytes();
                ctx.header(Header.CONTENT_LENGTH, String.valueOf(bytes.length));
                ctx.res.getOutputStream().write(bytes);
            } else {
                CdaError re = new CdaError("Unable to find group based on parameters given");
                logger.atInfo().log("%s%sfor request %s", re, System.lineSeparator(), ctx.fullUrl());
                ctx.status(HttpServletResponse.SC_NOT_FOUND).json(re);
            }
        } catch (IOException ex) {
            CdaError re = new CdaError("Failure to process request to retrieve time series group");
            logger.atSevere().withCause(ex).log("Failed to process request to retrieve time series group");
            ctx.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR).json(re);
        }
    }

    protected TimeSeriesGroup deserializeGroup(Context ctx) {
        ContentType contentType = Formats.parseHeader(ctx.req.getContentType(), TimeSeriesGroup.class);
        return Formats.parseContent(contentType, ctx.body(), TimeSeriesGroup.class);
    }

    @Override
    public void create(@NotNull Context ctx) {
        try (Timer.Context ignored = markAndTime(CREATE)) {
            DSLContext dsl = getDslContext(ctx);

            TimeSeriesGroup deserialize = deserializeGroup(ctx);

            if (!deserialize.getTimeSeriesCategory().getOfficeId().equalsIgnoreCase(CWMS_OFFICE)
                    && (!deserialize.getOfficeId().equalsIgnoreCase(deserialize.getTimeSeriesCategory().getOfficeId())
                    || deserialize.getOfficeId().equalsIgnoreCase(CWMS_OFFICE))) {
                throw new IllegalArgumentException("TimeSeries Group office ID cannot be CWMS and must match the "
                        + "TimeSeries Category office ID");
            }

            boolean ignoreNulls = ctx.queryParamAsClass(IGNORE_NULLS, Boolean.class).getOrDefault(true);
            boolean failIfExists = ctx.queryParamAsClass(FAIL_IF_EXISTS, Boolean.class).getOrDefault(true);
            boolean ignoreMissing = ctx.queryParamAsClass(IGNORE_MISSING, Boolean.class).getOrDefault(false);
            TimeSeriesGroupDao dao = new TimeSeriesGroupDao(dsl);
            List<CwmsId> missingTimeSeries = dao.create(deserialize, failIfExists, ignoreNulls, ignoreMissing);
            if (missingTimeSeries.isEmpty()) {
                ctx.status(HttpServletResponse.SC_CREATED);
            } else {
                Map<String, String> detailsMap = new HashMap<>();
                StringBuilder sb = new StringBuilder();
                for (CwmsId cwmsId : missingTimeSeries) {
                    sb.append(cwmsId.getName());
                    sb.append(", ");
                }
                sb.delete(sb.length() - 2, sb.length());
                detailsMap.put("missing-time-series", sb.toString());
                if (ignoreMissing) {
                    ctx.status(HttpCode.MULTI_STATUS);

                } else {
                    ctx.status(HttpServletResponse.SC_BAD_REQUEST);
                    detailsMap.put("message",
                        "One or more time series were not found and could not be assigned to the group");
                }
                ctx.json(detailsMap);
            }

        }
    }

    protected void delete(@NotNull Context ctx, @NotNull String groupId, @NotNull String office) {
        try (Timer.Context ignored = markAndTime(UPDATE)) {
            DSLContext dsl = getDslContext(ctx);

            TimeSeriesGroupDao dao = new TimeSeriesGroupDao(dsl);

            boolean cascadeDelete = ctx.queryParamAsClass(CASCADE_DELETE, Boolean.class).getOrDefault(false);
            String categoryId = requiredParam(ctx, CATEGORY_ID);
            dao.delete(categoryId, groupId, office, cascadeDelete);
            ctx.status(HttpServletResponse.SC_NO_CONTENT);
        }
    }

    /**
     * Persists a new description for a group, if the description has changed. Preserves the
     * group's existing assigned time series.
     *
     * @param dao The dao to use to persist the change.
     * @param existingGroup The group as currently stored.
     * @param newDescription The description provided in the request. If null, no update occurs.
     * @return The group reflecting the persisted description (or the unmodified existing group if
     *      no update was necessary).
     */
    protected TimeSeriesGroup updateDescriptionIfChanged(TimeSeriesGroupDao dao, TimeSeriesGroup existingGroup,
            String newDescription) {
        if (newDescription != null && !newDescription.equalsIgnoreCase(existingGroup.getDescription())) {
            TimeSeriesGroup updated = new TimeSeriesGroup(new TimeSeriesGroup(existingGroup.getTimeSeriesCategory(),
                    existingGroup.getOfficeId(), existingGroup.getId(), newDescription,
                    existingGroup.getSharedAliasId(), existingGroup.getSharedRefTsId()),
                    existingGroup.getAssignedTimeSeries());
            dao.create(updated, false, false);
            return updated;
        }
        return existingGroup;
    }

    /**
     * Sets the response status/body for an update that attempted to assign one or more time
     * series that do not exist. If there were no missing time series, this just sets a 200 OK
     * status. Shared between v1's full-body update and v2's membership-based update, both of
     * which can attempt to assign time series as part of a PATCH.
     *
     * @param ctx The request context to set the response on.
     * @param missingTimeSeries Time series that were requested to be assigned but do not exist.
     * @param ignoreMissing Whether missing time series should be tolerated (207) or treated as a
     *                      failure (400).
     */
    protected void respondToMissingTimeSeries(Context ctx, List<CwmsId> missingTimeSeries, boolean ignoreMissing) {
        if (missingTimeSeries.isEmpty()) {
            ctx.status(HttpServletResponse.SC_OK);
            return;
        }

        Map<String, String> detailsMap = new HashMap<>();
        StringBuilder sb = new StringBuilder();
        for (CwmsId cwmsId : missingTimeSeries) {
            sb.append(cwmsId.getName());
            sb.append(", ");
        }
        sb.delete(sb.length() - 2, sb.length());
        detailsMap.put("missing-timeseries", sb.toString());
        if (ignoreMissing) {
            ctx.status(HttpCode.MULTI_STATUS);
        } else {
            ctx.status(HttpServletResponse.SC_BAD_REQUEST);
            detailsMap.put("message",
                "One or more time series were not found and could not be assigned to the group");
        }
        ctx.json(detailsMap);
    }
}
