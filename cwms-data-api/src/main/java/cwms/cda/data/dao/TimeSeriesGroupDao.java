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

package cwms.cda.data.dao;

import static com.google.common.flogger.LazyArgs.lazy;
import static java.util.stream.Collectors.toList;

import com.google.common.flogger.FluentLogger;
import cwms.cda.data.dao.timeseriesgroup.DELETE_TS_GROUP_CASCADE;
import cwms.cda.data.dto.AssignedTimeSeries;
import cwms.cda.data.dto.TimeSeriesCategory;
import cwms.cda.data.dto.TimeSeriesGroup;
import java.math.BigDecimal;
import java.util.List;
import org.jetbrains.annotations.NotNull;
import org.jooq.Condition;
import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.jooq.Record5;
import org.jooq.Record8;
import org.jooq.Record9;
import org.jooq.RecordMapper;
import org.jooq.SelectConditionStep;
import org.jooq.SelectSeekStep4;
import org.jooq.conf.ParamType;
import org.jooq.impl.DSL;
import usace.cwms.db.jooq.codegen.packages.CWMS_TS_PACKAGE;
import usace.cwms.db.jooq.codegen.tables.AV_TS_CAT_GRP;
import usace.cwms.db.jooq.codegen.tables.AV_TS_GRP_ASSGN;
import usace.cwms.db.jooq.codegen.udt.records.TS_ALIAS_T;
import usace.cwms.db.jooq.codegen.udt.records.TS_ALIAS_TAB_T;


public class TimeSeriesGroupDao extends JooqDao<TimeSeriesGroup> {
    private static final FluentLogger logger = FluentLogger.forEnclosingClass();
    public static final String CWMS = "CWMS";

    private enum DeleteTsGroupCascadeMode {
        UNKNOWN,
        USE_CASCADE_ROUTINE,
        USE_UNASSIGN
    }

    private static volatile DeleteTsGroupCascadeMode deleteTsGroupCascadeMode = DeleteTsGroupCascadeMode.UNKNOWN;
    private static final Object deleteTsGroupCascadeModeLock = new Object();


    public TimeSeriesGroupDao(DSLContext dsl) {
        super(dsl);
    }

    public List<TimeSeriesGroup> getTimeSeriesGroups() {
        return getTimeSeriesGroups(null, null, null);
    }

    public List<TimeSeriesGroup> getTimeSeriesGroups(String tsOfficeId, String groupOfficeId, String categoryOfficeId) {
        Condition whereCond = DSL.noCondition();
        if (tsOfficeId != null) {
            whereCond = AV_TS_CAT_GRP.AV_TS_CAT_GRP.GRP_DB_OFFICE_ID.eq(tsOfficeId.toUpperCase());
        }

        return getTimeSeriesGroupsWhere(whereCond, tsOfficeId, groupOfficeId, categoryOfficeId);
    }

    public List<TimeSeriesGroup> getTimeSeriesGroups(String tsOfficeId, String groupOfficeId, String categoryOfficeId,
            boolean includeAssigned, String tsCategoryLike, String tsGroupLike) {

        Condition whereCond = DSL.noCondition();

        if (tsCategoryLike != null) {
            whereCond = whereCond.and(JooqDao.caseInsensitiveLikeRegex(AV_TS_CAT_GRP.AV_TS_CAT_GRP.TS_CATEGORY_ID,
                    tsCategoryLike));
        }

        if (tsGroupLike != null) {
            whereCond = whereCond.and(JooqDao.caseInsensitiveLikeRegex(AV_TS_CAT_GRP.AV_TS_CAT_GRP.TS_GROUP_ID,
                    tsGroupLike));
        } else {
            whereCond = whereCond.and(AV_TS_CAT_GRP.AV_TS_CAT_GRP.TS_GROUP_ID.isNotNull());
        }

        if (includeAssigned) {
            return getTimeSeriesGroupsWhere(whereCond, tsOfficeId, groupOfficeId, categoryOfficeId);
        } else {
            return getTimeSeriesGroupsWithoutAssigned(whereCond);
        }

    }

    public List<TimeSeriesGroup> getTimeSeriesGroups(String tsOfficeId, String groupOfficeId, String categoryOfficeId,
                                                     String categoryId, String groupId) {
        return getTimeSeriesGroupsWhere(buildWhereCondition(categoryId, groupId), tsOfficeId, groupOfficeId,
                categoryOfficeId);
    }

    public TimeSeriesGroup getTimeSeriesGroup(String tsOfficeId, String groupOfficeId, String categoryOfficeId,
                                              String categoryId, String groupId) {
        List<TimeSeriesGroup> timeSeriesGroups = getTimeSeriesGroups(tsOfficeId, groupOfficeId, categoryOfficeId, categoryId, groupId);
        if (timeSeriesGroups != null && !timeSeriesGroups.isEmpty()) {
            if (timeSeriesGroups.size() == 1) {
                return timeSeriesGroups.get(0);
            } else {
                throw new IllegalArgumentException(String.format("Multiple TimeSeriesGroups returned from "
                        + "getTimeSeriesGroups for office:%s category:%s group:%s At most one match was expected.",
                        tsOfficeId, categoryId, groupId));
            }
        }
        return null;
    }


    @NotNull
    private List<TimeSeriesGroup> getTimeSeriesGroupsWhere(Condition whereCond, String tsOfficeId, String groupOfficeId,
            String categoryOfficeId) {
        AV_TS_CAT_GRP catGrp = AV_TS_CAT_GRP.AV_TS_CAT_GRP;
        AV_TS_GRP_ASSGN grpAssgn = AV_TS_GRP_ASSGN.AV_TS_GRP_ASSGN;

        Condition whereCondGrpCat = DSL.noCondition();
        if (categoryOfficeId != null) {
            whereCondGrpCat = whereCondGrpCat.and(catGrp.CAT_DB_OFFICE_ID.eq(categoryOfficeId.toUpperCase()));
        }
        if (groupOfficeId != null) {
            whereCondGrpCat = whereCondGrpCat.and(catGrp.GRP_DB_OFFICE_ID.eq(groupOfficeId.toUpperCase()));
        }

        Condition joinCond = catGrp.TS_CATEGORY_ID.eq(grpAssgn.CATEGORY_ID)
                .and(catGrp.TS_GROUP_ID.eq(grpAssgn.GROUP_ID));
        if (tsOfficeId != null) {
            joinCond = joinCond.and(grpAssgn.DB_OFFICE_ID.eq(tsOfficeId.toUpperCase()));
        }

        SelectSeekStep4<Record9<String, String, String, String, String, String, String, String,
                List<AssignedTimeSeries>>, String, String, String, String> query = dsl
            .select(
                catGrp.CAT_DB_OFFICE_ID,
                catGrp.TS_CATEGORY_ID,
                catGrp.TS_CATEGORY_DESC,
                catGrp.GRP_DB_OFFICE_ID,
                catGrp.TS_GROUP_ID,
                catGrp.TS_GROUP_DESC,
                catGrp.SHARED_TS_ALIAS_ID,
                catGrp.SHARED_REF_TS_ID,
                DSL.multiset(
                    dsl
                        .select(
                            grpAssgn.TS_ID,
                            grpAssgn.DB_OFFICE_ID,
                            grpAssgn.ATTRIBUTE,
                            grpAssgn.ALIAS_ID,
                            grpAssgn.REF_TS_ID
                        )
                        .from(grpAssgn)
                        .where(joinCond)
                        .orderBy(grpAssgn.ATTRIBUTE) // Localized ordering inside the group
                ).convertFrom(rs -> rs.map(this::buildAssignedTimeSeries))
            )
            .from(catGrp)
            .where(whereCond)
            .and(whereCondGrpCat)
            .orderBy(catGrp.CAT_DB_OFFICE_ID,
                catGrp.TS_CATEGORY_ID,
                catGrp.GRP_DB_OFFICE_ID,
                catGrp.TS_GROUP_ID);

        logger.atFine().log("%s", lazy(() -> query.getSQL(ParamType.INLINED)));

        RecordMapper<? super Record9<String, String, String, String, String, String, String, String,
            List<AssignedTimeSeries>>, TimeSeriesGroup> mapperToTimeSeriesGroup =
                queryRecord -> {
                    TimeSeriesGroup group = buildTimeSeriesGroup(queryRecord);
                    List<AssignedTimeSeries> assignedTS = (List<AssignedTimeSeries>) queryRecord.get("multiset");

                    return new TimeSeriesGroup(group, assignedTS);
                };

        return query.fetch(mapperToTimeSeriesGroup);
    }

    @NotNull
    private List<TimeSeriesGroup> getTimeSeriesGroupsWithoutAssigned(Condition whereCond) {

        SelectConditionStep<Record8<String, String, String, String, String, String, String, String>> query = dsl.select(
                AV_TS_CAT_GRP.AV_TS_CAT_GRP.CAT_DB_OFFICE_ID,
                        AV_TS_CAT_GRP.AV_TS_CAT_GRP.TS_CATEGORY_ID,
                        AV_TS_CAT_GRP.AV_TS_CAT_GRP.TS_CATEGORY_DESC,
                        AV_TS_CAT_GRP.AV_TS_CAT_GRP.GRP_DB_OFFICE_ID,
                        AV_TS_CAT_GRP.AV_TS_CAT_GRP.TS_GROUP_ID,
                        AV_TS_CAT_GRP.AV_TS_CAT_GRP.TS_GROUP_DESC,
                        AV_TS_CAT_GRP.AV_TS_CAT_GRP.SHARED_TS_ALIAS_ID,
                        AV_TS_CAT_GRP.AV_TS_CAT_GRP.SHARED_REF_TS_ID)
                .from(AV_TS_CAT_GRP.AV_TS_CAT_GRP)
                .where(whereCond);

        logger.atFine().log("%s", lazy(() -> query.getSQL(ParamType.INLINED)));

        return query.fetch((RecordMapper<org.jooq.Record, TimeSeriesGroup>) this::buildTimeSeriesGroup);
    }

    private AssignedTimeSeries buildAssignedTimeSeries(Record5<String, String, BigDecimal, String, String> multisetRecord) {
        AssignedTimeSeries retval = null;

        if (multisetRecord != null) {
            String timeseriesId = multisetRecord.get(0, String.class);
            String officeId = multisetRecord.get(1, String.class);
            BigDecimal attrBD = multisetRecord.get(2, BigDecimal.class);
            String aliasId = multisetRecord.get(3, String.class);
            String refTsId = multisetRecord.get(4, String.class);

            Integer attr = null;
            if (attrBD != null) {
                attr = attrBD.intValue();
            }

            retval = new AssignedTimeSeries(officeId, timeseriesId, aliasId, refTsId, attr);
        }

        return retval;
    }

    private TimeSeriesGroup buildTimeSeriesGroup(org.jooq.Record queryRecord) {
        TimeSeriesCategory cat = buildTimeSeriesCategory(queryRecord);

        String grpOfficeId = queryRecord.get(AV_TS_CAT_GRP.AV_TS_CAT_GRP.GRP_DB_OFFICE_ID);
        String grpId = queryRecord.get(AV_TS_CAT_GRP.AV_TS_CAT_GRP.TS_GROUP_ID);
        String grpDesc = queryRecord.get(AV_TS_CAT_GRP.AV_TS_CAT_GRP.TS_GROUP_DESC);
        String sharedAliasId = queryRecord.get(AV_TS_CAT_GRP.AV_TS_CAT_GRP.SHARED_TS_ALIAS_ID);
        String sharedRefTsId = queryRecord.get(AV_TS_CAT_GRP.AV_TS_CAT_GRP.SHARED_REF_TS_ID);

        return new TimeSeriesGroup(cat, grpOfficeId, grpId, grpDesc, sharedAliasId, sharedRefTsId);
    }

    @NotNull
    private TimeSeriesCategory buildTimeSeriesCategory(org.jooq.Record queryRecord) {
        String catOfficeId = queryRecord.get(AV_TS_CAT_GRP.AV_TS_CAT_GRP.CAT_DB_OFFICE_ID);
        String catId = queryRecord.get(AV_TS_CAT_GRP.AV_TS_CAT_GRP.TS_CATEGORY_ID);
        String catDesc = queryRecord.get(AV_TS_CAT_GRP.AV_TS_CAT_GRP.TS_CATEGORY_DESC);
        return new TimeSeriesCategory(catOfficeId, catId, catDesc);
    }

    private Condition buildWhereCondition(String categoryId, String groupId) {
        AV_TS_CAT_GRP atcg = AV_TS_CAT_GRP.AV_TS_CAT_GRP;
        Condition whereCondition = DSL.noCondition();

        if (categoryId != null && !categoryId.isEmpty()) {
            whereCondition = whereCondition.and(atcg.TS_CATEGORY_ID.eq(categoryId));
        }

        if (groupId != null && !groupId.isEmpty()) {
            whereCondition = whereCondition.and(atcg.TS_GROUP_ID.eq(groupId));
        }
        return whereCondition;
    }

    public void delete(String categoryId, String groupId, String office, boolean cascade) {
        connection(dsl, conn -> {
            // If caller didn't ask for cascade behavior, we can always use the legacy routine.
            if (!cascade) {
                CWMS_TS_PACKAGE.call_DELETE_TS_GROUP(getDslContext(conn, office).configuration(), categoryId, groupId, office);
                return;
            }

            // Cascade requested:
            //  1) Prefer DELETE_TS_GROUP_CASCADE when it exists and binds successfully.
            //  2) If it does not exist / doesn't bind, fall back to the legacy method (unassign + delete).
            DeleteTsGroupCascadeMode mode = deleteTsGroupCascadeMode;

            if (mode == DeleteTsGroupCascadeMode.USE_CASCADE_ROUTINE) {
                call_DELETE_TS_GROUP_CASCADE(getDslContext(conn, office).configuration(), categoryId, groupId, formatBool(true), office);
                return;
            }

            if (mode == DeleteTsGroupCascadeMode.USE_UNASSIGN) {
                deleteViaUnassign(conn, categoryId, groupId, office, true);
                return;
            }

            // UNKNOWN: probe once, cache result.
            synchronized (deleteTsGroupCascadeModeLock) {
                // Re-read in case another thread already decided while we waited.
                mode = deleteTsGroupCascadeMode;

                if (mode == DeleteTsGroupCascadeMode.USE_CASCADE_ROUTINE) {
                    call_DELETE_TS_GROUP_CASCADE(getDslContext(conn, office).configuration(), categoryId, groupId, formatBool(true), office);
                    return;
                }

                if (mode == DeleteTsGroupCascadeMode.USE_UNASSIGN) {
                    deleteViaUnassign(conn, categoryId, groupId, office, true);
                    return;
                }

                try {
                    call_DELETE_TS_GROUP_CASCADE(getDslContext(conn, office).configuration(), categoryId, groupId, formatBool(true), office);
                    deleteTsGroupCascadeMode = DeleteTsGroupCascadeMode.USE_CASCADE_ROUTINE;
                } catch (RuntimeException e) {
                    if (isMissingOrBindFailure(e)) {
                        logger.atWarning().withCause(e).log(
                                "DELETE_TS_GROUP_CASCADE is not available. Falling back to iterative cascade delete.");
                        deleteTsGroupCascadeMode = DeleteTsGroupCascadeMode.USE_UNASSIGN;
                        deleteViaUnassign(conn, categoryId, groupId, office, true);
                    } else {
                        // Routine exists (or at least binds) but failed for some other reason.
                        throw e;
                    }
                }
            }
        });
    }

    private void deleteViaUnassign(java.sql.Connection conn, String categoryId, String groupId, String office, boolean cascade) {
        DSLContext dslContext = getDslContext(conn, office);
        dslContext.transaction((Configuration trx) -> {
            Configuration config = trx.dsl().configuration();
            if (cascade) {
                TimeSeriesGroup group = getTimeSeriesGroup(null, office, null, categoryId, groupId);
                if (group != null) {
                    unassignAllTs(group, office);
                }
            }
            CWMS_TS_PACKAGE.call_DELETE_TS_GROUP(config, categoryId, groupId, office);
        });
    }

    @SuppressWarnings("checkstyle:AbbreviationAsWordInName")
    public static void call_DELETE_TS_GROUP_CASCADE(Configuration configuration, String P_TS_CATEGORY_ID, String P_TS_GROUP_ID, String P_CASCADE, String P_DB_OFFICE_ID) {
        DELETE_TS_GROUP_CASCADE p = new DELETE_TS_GROUP_CASCADE();  // This is our own routine, not codegen.
        p.setP_TS_CATEGORY_ID(P_TS_CATEGORY_ID);
        p.setP_TS_GROUP_ID(P_TS_GROUP_ID);
        p.setP_CASCADE(P_CASCADE);
        p.setP_DB_OFFICE_ID(P_DB_OFFICE_ID);
        p.execute(configuration);
    }


    public void create(TimeSeriesGroup group, boolean failIfExists) {
        connection(dsl, c -> {
            Configuration configuration = getDslContext(c,group.getOfficeId()).configuration();
            String categoryId = group.getTimeSeriesCategory().getId();
            CWMS_TS_PACKAGE.call_STORE_TS_GROUP(configuration, categoryId,
                group.getId(), group.getDescription(), formatBool(failIfExists),
                "T", group.getSharedAliasId(),
                group.getSharedRefTsId(), group.getOfficeId());
            assignTs(configuration,group, group.getOfficeId());
        });
    }

    private void assignTs(Configuration configuration,TimeSeriesGroup group, String office) {
        List<AssignedTimeSeries> assignedTimeSeries = group.getAssignedTimeSeries();
        if (assignedTimeSeries != null) {
            List<TS_ALIAS_T> collect = assignedTimeSeries.stream()
                .map(TimeSeriesGroupDao::convertToTsAliasType)
                .collect(toList());
            TS_ALIAS_TAB_T assignedLocs = new TS_ALIAS_TAB_T(collect);
            CWMS_TS_PACKAGE.call_ASSIGN_TS_GROUPS(configuration, group.getTimeSeriesCategory().getId(),
                group.getId(), assignedLocs, office);
        }
    }

    public void assignTs(TimeSeriesGroup group, String office) {
        connection(dsl, c -> assignTs(getDslContext(c, office).configuration(),group, office));
    }

    private static TS_ALIAS_T convertToTsAliasType(AssignedTimeSeries assignedTimeSeries) {
        BigDecimal attribute = toBigDecimal(assignedTimeSeries.getAttribute());
        return new TS_ALIAS_T(assignedTimeSeries.getTimeseriesId(), attribute,
            assignedTimeSeries.getAliasId(), assignedTimeSeries.getRefTsId());
    }

    public void renameTimeSeriesGroup(String oldGroupId, TimeSeriesGroup group) {
        connection(dsl, c ->
            CWMS_TS_PACKAGE.call_RENAME_TS_GROUP(
                getDslContext(c, group.getOfficeId()).configuration(),
                group.getTimeSeriesCategory().getId(), oldGroupId, group.getId(),
                group.getOfficeId())
        );
    }

    public void unassignAllTs(TimeSeriesGroup group, String officeId) {
        connection(dsl, c ->
            CWMS_TS_PACKAGE.call_UNASSIGN_TS_GROUP(
                getDslContext(c,officeId).configuration(),
                group.getTimeSeriesCategory().getId(), group.getId(),
                null, "T", group.getOfficeId())
        );
    }
}
