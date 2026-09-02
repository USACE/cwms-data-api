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
import static org.jooq.impl.DSL.asterisk;
import static org.jooq.impl.DSL.count;

import com.google.common.flogger.FluentLogger;
import cwms.cda.data.dao.timeseriesgroup.DELETE_TS_GROUP_CASCADE;
import cwms.cda.data.dto.AssignedTimeSeries;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.TimeSeriesCategory;
import cwms.cda.data.dto.timeseriesgroup.TimeSeriesGroup;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.jetbrains.annotations.NotNull;
import org.jooq.Condition;
import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.jooq.Record1;
import org.jooq.Record5;
import org.jooq.Record8;
import org.jooq.Record9;
import org.jooq.RecordMapper;
import org.jooq.Result;
import org.jooq.SelectConditionStep;
import org.jooq.SelectSeekStep4;
import org.jooq.conf.ParamType;
import org.jooq.impl.DSL;
import usace.cwms.db.jooq.codegen.packages.CWMS_ENV_PACKAGE;
import usace.cwms.db.jooq.codegen.packages.CWMS_TS_PACKAGE;
import usace.cwms.db.jooq.codegen.tables.AV_TS_CAT_GRP;
import usace.cwms.db.jooq.codegen.tables.AV_TS_GRP_ASSGN;
import usace.cwms.db.jooq.codegen_latest.udt.records.TS_ALIAS_T;
import usace.cwms.db.jooq.codegen_latest.udt.records.TS_ALIAS_TAB_T;


public class TimeSeriesGroupDao extends JooqDao<TimeSeriesGroup> {
    private static final FluentLogger logger = FluentLogger.forEnclosingClass();
    public static final String CWMS = "CWMS";

    private enum DeleteTsGroupCascadeMode {
        UNKNOWN,
        USE_CASCADE_ROUTINE,
        USE_UNASSIGN
    }

    private static volatile DeleteTsGroupCascadeMode deleteTsGroupCascadeMode = DeleteTsGroupCascadeMode.UNKNOWN;

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

        return getTimeSeriesGroupsWhere(dsl, whereCond, tsOfficeId, groupOfficeId, categoryOfficeId);
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
            return getTimeSeriesGroupsWhere(dsl, whereCond, tsOfficeId, groupOfficeId, categoryOfficeId);
        } else {
            return getTimeSeriesGroupsWithoutAssigned(whereCond, groupOfficeId, categoryOfficeId);
        }

    }


    public List<TimeSeriesGroup> getTimeSeriesGroups(DSLContext dslContext, String tsOfficeId, String groupOfficeId, String categoryOfficeId,
                                                     String categoryId, String groupId) {
        return getTimeSeriesGroupsWhere(dslContext, buildWhereCondition(categoryId, groupId), tsOfficeId,
                groupOfficeId, categoryOfficeId);
    }


    public TimeSeriesGroup getTimeSeriesGroup(String tsOfficeId, String groupOfficeId, String categoryOfficeId,
                                              String categoryId, String groupId) {
        return getTimeSeriesGroup(dsl, tsOfficeId, groupOfficeId, categoryOfficeId, categoryId, groupId);
    }

    /**
     *
     * @param dslContext The context to be used to avoid creating a new connection.
     * @param tsOfficeId The office id.
     * @param groupOfficeId The group office id.
     * @param categoryOfficeId The category office id.
     * @param categoryId The category id.
     * @param groupId The group id.
     * @return retrieved TimeSeriesGroup.
     */
    public TimeSeriesGroup getTimeSeriesGroup(DSLContext dslContext, String tsOfficeId, String groupOfficeId, String categoryOfficeId,
                                              String categoryId, String groupId) {
        List<TimeSeriesGroup> timeSeriesGroups = getTimeSeriesGroups(dslContext, tsOfficeId, groupOfficeId, categoryOfficeId, categoryId, groupId);
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

    /**
     *
     * @param dslContext Jooq Context to be used
     * @param whereCond Additional whereCondition that will be added to the query.
     * @param tsOfficeId If provided, the assigned time series that are retrieved will be restricted to this office.
     * @param groupOfficeId If provided, the retrieved groups must be in this office
     * @param categoryOfficeId If provided, the retrieve groups must be in categories that are in this office
     * @return retrieved TimeSeriesGroups.
     */
    @NotNull
    private List<TimeSeriesGroup> getTimeSeriesGroupsWhere(DSLContext dslContext, Condition whereCond, String tsOfficeId, String groupOfficeId,
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
                List<AssignedTimeSeries>>, String, String, String, String> query = dslContext
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
                        dslContext
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
    private List<TimeSeriesGroup> getTimeSeriesGroupsWithoutAssigned(Condition whereCond, String groupOfficeId, String categoryOfficeId) {
        AV_TS_CAT_GRP catGrp = AV_TS_CAT_GRP.AV_TS_CAT_GRP;

        Condition whereCondGrpCat = DSL.noCondition();
        if (categoryOfficeId != null) {
            whereCondGrpCat = whereCondGrpCat.and(catGrp.CAT_DB_OFFICE_ID.eq(categoryOfficeId.toUpperCase()));
        }
        if (groupOfficeId != null) {
            whereCondGrpCat = whereCondGrpCat.and(catGrp.GRP_DB_OFFICE_ID.eq(groupOfficeId.toUpperCase()));
        }

        SelectConditionStep<Record8<String, String, String, String, String, String, String, String>> query = dsl.select(
                catGrp.CAT_DB_OFFICE_ID,
                        catGrp.TS_CATEGORY_ID,
                        catGrp.TS_CATEGORY_DESC,
                        catGrp.GRP_DB_OFFICE_ID,
                        catGrp.TS_GROUP_ID,
                        catGrp.TS_GROUP_DESC,
                        catGrp.SHARED_TS_ALIAS_ID,
                        catGrp.SHARED_REF_TS_ID)
                .from(catGrp)
                .where(whereCond)
                .and(whereCondGrpCat);

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
            DSLContext dslContext = getDslContext(conn, office);
            // If caller didn't ask for cascade behavior, we can always use the legacy routine.
            if (!cascade) {
                CWMS_TS_PACKAGE.call_DELETE_TS_GROUP(dslContext.configuration(), categoryId, groupId, office);
                return;
            }

            // Cascade requested:
            //  1) Prefer DELETE_TS_GROUP_CASCADE when it exists and binds successfully.
            //  2) If it does not exist / doesn't bind, fall back to the legacy method (unassign + delete).
            DeleteTsGroupCascadeMode mode = deleteTsGroupCascadeMode;

            if (mode == DeleteTsGroupCascadeMode.USE_CASCADE_ROUTINE) {
                call_DELETE_TS_GROUP_CASCADE(dslContext.configuration(), categoryId, groupId, formatBool(true), office);
                return;
            }

            if (mode == DeleteTsGroupCascadeMode.USE_UNASSIGN) {
                deleteViaUnassign(dslContext, categoryId, groupId, office, true);
                return;
            }

            // UNKNOWN: just try it; harmless if multiple threads probe simultaneously.
            try {
                call_DELETE_TS_GROUP_CASCADE(dslContext.configuration(), categoryId, groupId, formatBool(true), office);
                deleteTsGroupCascadeMode = DeleteTsGroupCascadeMode.USE_CASCADE_ROUTINE;
            } catch (RuntimeException e) {
                if (isMissingOrBindFailure(e)) {
                    // No reason to log the whole exception here. It was either missing or bind
                    // and we think we have an alternative.
                    logger.atFine().log("DELETE_TS_GROUP_CASCADE is not available. Falling back to iterative cascade delete.");
                    deleteTsGroupCascadeMode = DeleteTsGroupCascadeMode.USE_UNASSIGN;
                    deleteViaUnassign(dslContext, categoryId, groupId, office, true);
                } else {
                    throw e;
                }
            }
        });
    }

    /**
     *
     * @param dslContext a jooq context that already has the approriate office set in the session.
     * @param categoryId
     * @param groupId
     * @param office
     * @param cascade
     */
    private void deleteViaUnassign(DSLContext dslContext, String categoryId, String groupId, String office, boolean cascade) {

        dslContext.transaction((Configuration config) -> {

            if (cascade) {
                unassignAll(config, categoryId, groupId, office);
            }
            CWMS_TS_PACKAGE.call_DELETE_TS_GROUP(config, categoryId, groupId, office);
        });
    }


    public void unassignTsIds(String categoryId, String groupId, String office, List<CwmsId> tsIds) {
        if (tsIds == null || tsIds.isEmpty()) {
            throw new IllegalArgumentException("At least one time series id must be provided to unassign.");
        }

        connection(dsl, conn -> {
            DSLContext dslContext = getDslContext(conn, office);
            dslContext.transaction((Configuration config) -> {
                for (CwmsId tsId : tsIds) {
                    CWMS_TS_PACKAGE.call_UNASSIGN_TS_GROUP(config, categoryId, groupId, tsId.getName(), "F", office);
                }
            });
        });
    }

    public void unassignAll(String categoryId, String groupId, String office) {
        dsl.transaction((Configuration config) ->
            unassignAll(config, categoryId, groupId, office)
        );
    }

    // This may not be that useful in practice.  Typically groups either below to an office like SPK or to CWMS.
    // Offices can assign ts to the CWMS group and they should be able to unassign their own ts from a CWMS group
    // but SPK users shouldn't be able to unassign assignments that belong to other offices (SWT) and they
    // shouldn't be able to remove CWMS assignments.   SPK users also shouldn't be able to delete CWMS groups.
    //
    // This method is currently used when cascade delete isn't available in the pl/sql and the user wants
    // to unassign all assignments for a group so that the group will be empty and can then be deleted.
    // In practice, CWMS groups can't be deleted, even if they were empty.  So this would only be helpful
    // for office specific groups, in which case users only need to unassign for a single office (their own).
    private void unassignAll(Configuration config, String categoryId, String groupId, String office) {
        DSLContext context = config.dsl();

        // Find all the offices with an assignment in the group.
        List<String> assignmentOffices = getAssignmentOffices(context, categoryId, groupId, office);
        logger.atInfo().log("For o:%s c:%s g:%s found assignments in offices:%s", office, categoryId, groupId, assignmentOffices);
        if (!assignmentOffices.isEmpty()) {
            for (String assignmentOffice : assignmentOffices) {
                unassignForOffice(config, categoryId, groupId, office, assignmentOffice);
            }
        }
    }

    public void unassignForOffice( String categoryId, String groupId, String office, String assignmentOffice) {
        connection(dsl, conn -> {
            DSLContext dslContext = getDslContext(conn, assignmentOffice);
            unassignForOffice(dslContext.configuration(), categoryId, groupId, office, assignmentOffice);
        });
    }

    public static void unassignForOffice(Configuration config, String categoryId, String groupId,
            String office, String assignmentOffice) {
        if (office != null && !"CWMS".equals(office)) {
            CWMS_ENV_PACKAGE.call_SET_SESSION_OFFICE_ID(config, assignmentOffice);
        }
        CWMS_TS_PACKAGE.call_UNASSIGN_TS_GROUP(config,
                categoryId, groupId,
                null, "T", assignmentOffice);
    }

    private List<String> getAssignmentOffices(DSLContext context, String categoryId, String groupId, String office) {
        List<String> retval = new ArrayList<>();

        // retrieve with a null tsOfficeId so that we get ALL ts assignments.
        TimeSeriesGroup group = getTimeSeriesGroup(context,null, office, null, categoryId, groupId);
        if (group != null) {

            Set<String> assignmentOffices = new LinkedHashSet<>();
            if (group.getAssignedTimeSeries() != null) {
                for (AssignedTimeSeries ats : group.getAssignedTimeSeries()) {
                    assignmentOffices.add(ats.getOfficeId());
                }
            }

            boolean hadCwms = assignmentOffices.remove("CWMS");
            retval.addAll(assignmentOffices);
            if (hadCwms) {
                assignmentOffices.add("CWMS"); // want it last
            }
        }
        return retval;
    }

    @SuppressWarnings("checkstyle:AbbreviationAsWordInName")
    public static void call_DELETE_TS_GROUP_CASCADE(Configuration configuration, String tsCategoryId, String tsGroupId, String cascade, String dbOfficeId) {
        DELETE_TS_GROUP_CASCADE p = new DELETE_TS_GROUP_CASCADE();  // This is our own routine, not codegen.
        p.setP_TS_CATEGORY_ID(tsCategoryId);
        p.setP_TS_GROUP_ID(tsGroupId);
        p.setP_CASCADE(cascade);
        p.setP_DB_OFFICE_ID(dbOfficeId);
        p.execute(configuration);
    }

    public List<CwmsId> create(TimeSeriesGroup group, boolean failIfExists, boolean ignoreNulls) {
        return create(group, failIfExists, ignoreNulls, false);
    }

    public List<CwmsId> create(TimeSeriesGroup group, boolean failIfExists, boolean ignoreNulls, boolean ignoreMissing) {
        return connectionResult(dsl, c -> {
            Configuration configuration = getDslContext(c, group.getOfficeId()).configuration();
            String categoryId = group.getTimeSeriesCategory().getId();
            DSLContext dslContext = getDslContext(c, group.getOfficeId());
            return dslContext.transactionResult((Configuration trx) -> {
                CWMS_TS_PACKAGE.call_STORE_TS_GROUP(configuration, categoryId,
                    group.getId(), group.getDescription(), formatBool(failIfExists),
                    formatBool(ignoreNulls), group.getSharedAliasId(),
                    group.getSharedRefTsId(), group.getOfficeId());
                return assignTs(configuration, group, group.getOfficeId(), ignoreNulls, ignoreMissing);
            });
        });
    }

    private List<CwmsId> assignTs(Configuration configuration, TimeSeriesGroup group,
            String office, boolean ignoreNulls) {
        return assignTs(configuration, group, office, ignoreNulls, false);
    }

    private List<CwmsId> assignTs(Configuration configuration, TimeSeriesGroup group,
            String office, boolean ignoreNulls, boolean ignoreMissing) {
        List<AssignedTimeSeries> assignedTimeSeries = group.getAssignedTimeSeries();
        List<CwmsId> missingTimeSeries = new ArrayList<>();

        if (!ignoreNulls && (assignedTimeSeries == null || assignedTimeSeries.isEmpty())) {
            CWMS_TS_PACKAGE.call_UNASSIGN_TS_GROUP(configuration,
                    group.getTimeSeriesCategory().getId(), group.getId(),
                    null, "T", group.getOfficeId());

        } else {
            if (assignedTimeSeries != null) {
                if (supportsMissing(configuration)) {
                    List<TS_ALIAS_T> collect = assignedTimeSeries.stream()
                        .map(TimeSeriesGroupDao::convertToTsAliasType)
                        .collect(toList());
                    TS_ALIAS_TAB_T assignedLocs = new TS_ALIAS_TAB_T(collect);
                    TS_ALIAS_TAB_T missingTs =
                        usace.cwms.db.jooq.codegen_latest.packages.CWMS_TS_PACKAGE.call_ASSIGN_TS_GROUPS_SUPPORT_MISSING(
                            configuration,
                            group.getTimeSeriesCategory().getId(), group.getId(), assignedLocs, office,
                            formatBool(ignoreMissing));
                    if (!missingTs.isEmpty()) {
                        for (TS_ALIAS_T missing : missingTs) {
                            missingTimeSeries.add(CwmsId.buildCwmsId(office, missing.getTS_ID()));
                        }
                    }
                } else {
                    List<usace.cwms.db.jooq.codegen.udt.records.TS_ALIAS_T> collect = assignedTimeSeries.stream()
                        .map(TimeSeriesGroupDao::convertToLegacyTsAliasType)
                        .collect(toList());
                    usace.cwms.db.jooq.codegen.udt.records.TS_ALIAS_TAB_T assignedLocs
                        = new usace.cwms.db.jooq.codegen.udt.records.TS_ALIAS_TAB_T(collect);
                    CWMS_TS_PACKAGE.call_ASSIGN_TS_GROUPS(configuration, group.getTimeSeriesCategory().getId(),
                        group.getId(), assignedLocs, office);
                }
            }
        }
        return missingTimeSeries;
    }

    private boolean supportsMissing(Configuration config) {
        Result<Record1<Integer>> support = config.dsl()
            .select(count(asterisk()))
            .from("all_procedures")
            .where("procedure_name = 'ASSIGN_TS_GROUPS_SUPPORT_MISSING'")
            .fetch();
        int supportCount = support.get(0).value1();
        return supportCount > 0;
    }

    public List<CwmsId> assignTs(TimeSeriesGroup group, String office, boolean ignoreMissing) {
        return connectionResult(dsl, c -> assignTs(getDslContext(c, office).configuration(),group, office, true, ignoreMissing));
    }

    public void assignTs(TimeSeriesGroup group, String office) {
        connection(dsl, c -> assignTs(getDslContext(c, office).configuration(),group, office, true));
    }

    private static TS_ALIAS_T convertToTsAliasType(AssignedTimeSeries assignedTimeSeries) {
        BigDecimal attribute = toBigDecimal(assignedTimeSeries.getAttribute());
        return new TS_ALIAS_T(assignedTimeSeries.getTimeseriesId(), attribute,
            assignedTimeSeries.getAliasId(), assignedTimeSeries.getRefTsId());
    }

    @Deprecated
    private static usace.cwms.db.jooq.codegen.udt.records.TS_ALIAS_T convertToLegacyTsAliasType(AssignedTimeSeries assignedTimeSeries) {
        BigDecimal attribute = toBigDecimal(assignedTimeSeries.getAttribute());
        return new usace.cwms.db.jooq.codegen.udt.records.TS_ALIAS_T(assignedTimeSeries.getTimeseriesId(), attribute,
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
            // For Default/Default if officeId is 'CWMS' this seems to not unassign
            // the assigned timeseries where the office id of the timeseries is SPK.
            // Is this a bug?
            {
                DSLContext dslContext = getDslContext(c, officeId);

                // UNASSIGN_TS_GROUP apparently only unassigns the assignments that are in the
                // P_DB_OFFICE_ID  ( last parameter)
                CWMS_TS_PACKAGE.call_UNASSIGN_TS_GROUP(
                    dslContext.configuration(),
                    group.getTimeSeriesCategory().getId(), group.getId(),
                    null, "T", group.getOfficeId());
            }
        );
    }
}
