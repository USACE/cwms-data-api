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

import cwms.cda.data.dto.AssignedTimeSeries;
import cwms.cda.data.dto.TimeSeriesCategory;
import cwms.cda.data.dto.TimeSeriesGroup;
import kotlin.Pair;
import org.jetbrains.annotations.NotNull;
import org.jooq.*;
import org.jooq.conf.ParamType;
import org.jooq.impl.DSL;
import usace.cwms.db.jooq.codegen.packages.CWMS_TS_PACKAGE;
import usace.cwms.db.jooq.codegen.tables.AV_TS_CAT_GRP;
import usace.cwms.db.jooq.codegen.tables.AV_TS_GRP_ASSGN;
import usace.cwms.db.jooq.codegen.udt.records.TS_ALIAS_T;
import usace.cwms.db.jooq.codegen.udt.records.TS_ALIAS_TAB_T;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import static java.util.stream.Collectors.toList;

public class TimeSeriesGroupDao extends JooqDao<TimeSeriesGroup> {
    private static final Logger logger = Logger.getLogger(TimeSeriesGroupDao.class.getName());
    public static final String CWMS = "CWMS";

    public TimeSeriesGroupDao(DSLContext dsl) {
        super(dsl);
    }

    public List<TimeSeriesGroup> getTimeSeriesGroups() {
        return getTimeSeriesGroups(null);
    }

    public List<TimeSeriesGroup> getTimeSeriesGroups(String officeId) {
        Condition whereCond = DSL.noCondition();
        if (officeId != null) {
            whereCond = AV_TS_CAT_GRP.AV_TS_CAT_GRP.GRP_DB_OFFICE_ID.eq(officeId);
        }

        return getTimeSeriesGroupsWhere(whereCond);
    }

    public List<TimeSeriesGroup> getTimeSeriesGroups(String officeId, boolean includeAssigned, String tsCategoryLike, String tsGroupLike) {

        Condition whereCond = DSL.noCondition();
        if (officeId != null) {
            whereCond = whereCond.and(AV_TS_CAT_GRP.AV_TS_CAT_GRP.GRP_DB_OFFICE_ID.eq(officeId));
        }

        if (tsCategoryLike != null) {
            whereCond = whereCond.and(JooqDao.caseInsensitiveLikeRegex(AV_TS_CAT_GRP.AV_TS_CAT_GRP.TS_CATEGORY_ID, tsCategoryLike));
        }

        if (tsGroupLike != null) {
            whereCond = whereCond.and(JooqDao.caseInsensitiveLikeRegex(AV_TS_CAT_GRP.AV_TS_CAT_GRP.TS_GROUP_ID, tsGroupLike));
        } else {
            whereCond = whereCond.and(AV_TS_CAT_GRP.AV_TS_CAT_GRP.TS_GROUP_ID.isNotNull());
        }

        if(includeAssigned){
            return getTimeSeriesGroupsWhere(whereCond);
        } else {
            return getTimeSeriesGroupsWithoutAssigned(whereCond);
        }

    }


    public List<TimeSeriesGroup> getTimeSeriesGroups(String officeId, String categoryId, String groupId) {
        return getTimeSeriesGroupsWhere(buildWhereCondition(officeId, categoryId, groupId));
    }

    @NotNull
    private List<TimeSeriesGroup> getTimeSeriesGroupsWhere(Condition whereCond) {
        List<TimeSeriesGroup> retval = new ArrayList<>();
        AV_TS_CAT_GRP catGrp = AV_TS_CAT_GRP.AV_TS_CAT_GRP;
        AV_TS_GRP_ASSGN grpAssgn = AV_TS_GRP_ASSGN.AV_TS_GRP_ASSGN;

        final RecordMapper<Record, Pair<TimeSeriesGroup, AssignedTimeSeries>> mapper =
                queryRecord -> {
                    TimeSeriesGroup group = buildTimeSeriesGroup(queryRecord);
                    AssignedTimeSeries loc = buildAssignedTimeSeries(queryRecord);

                    return new Pair<>(group, loc);
                };

        SelectOnConditionStep<?> selectOn = dsl.select(catGrp.CAT_DB_OFFICE_ID,
                        catGrp.TS_CATEGORY_ID, catGrp.TS_CATEGORY_DESC, catGrp.GRP_DB_OFFICE_ID,
                        catGrp.TS_GROUP_ID, catGrp.TS_GROUP_DESC, catGrp.SHARED_TS_ALIAS_ID,
                        catGrp.SHARED_REF_TS_ID, grpAssgn.CATEGORY_ID, grpAssgn.DB_OFFICE_ID,
                        grpAssgn.GROUP_ID, grpAssgn.TS_ID, grpAssgn.TS_CODE, grpAssgn.ATTRIBUTE,
                        grpAssgn.ALIAS_ID, grpAssgn.REF_TS_ID)
                .from(catGrp).leftJoin(grpAssgn)
                .on(catGrp.TS_CATEGORY_ID.eq(grpAssgn.CATEGORY_ID)
                        .and(catGrp.TS_GROUP_ID.eq(grpAssgn.GROUP_ID)));

        SelectOrderByStep<?> select = selectOn;
        if (whereCond != null) {
            select = selectOn.where(whereCond);
        }

        final SelectSeekStep1<?, BigDecimal> query = select.orderBy(grpAssgn.ATTRIBUTE);

        logger.fine(() -> query.getSQL(ParamType.INLINED));

        List<Pair<TimeSeriesGroup, AssignedTimeSeries>> assignments =
                query.fetch(mapper);

        Map<TimeSeriesGroup, List<AssignedTimeSeries>> map = new LinkedHashMap<>();
        for (Pair<TimeSeriesGroup, AssignedTimeSeries> pair : assignments) {
            List<AssignedTimeSeries> list = map.computeIfAbsent(pair.component1(),
                    k -> new ArrayList<>());
            AssignedTimeSeries assignedTimeSeries = pair.component2();
            if (assignedTimeSeries != null) {
                list.add(assignedTimeSeries);
            }
        }

        for (final Map.Entry<TimeSeriesGroup, List<AssignedTimeSeries>> entry : map.entrySet()) {
            List<AssignedTimeSeries> assigned = entry.getValue();
            retval.add(new TimeSeriesGroup(entry.getKey(), assigned));
        }
        return retval;
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

        logger.fine(() -> query.getSQL(ParamType.INLINED));

        return query.fetch((RecordMapper<Record, TimeSeriesGroup>) this::buildTimeSeriesGroup);
    }

    private AssignedTimeSeries buildAssignedTimeSeries(Record queryRecord) {
        AssignedTimeSeries retval = null;

        String officeId = queryRecord.get(AV_TS_GRP_ASSGN.AV_TS_GRP_ASSGN.DB_OFFICE_ID);
        String timeseriesId = queryRecord.get(AV_TS_GRP_ASSGN.AV_TS_GRP_ASSGN.TS_ID);
        BigDecimal tsCode = queryRecord.get(AV_TS_GRP_ASSGN.AV_TS_GRP_ASSGN.TS_CODE);

        if (timeseriesId != null && tsCode != null) {
            String aliasId = queryRecord.get(AV_TS_GRP_ASSGN.AV_TS_GRP_ASSGN.ALIAS_ID);
            String refTsId = queryRecord.get(AV_TS_GRP_ASSGN.AV_TS_GRP_ASSGN.REF_TS_ID);
            BigDecimal attrBD = queryRecord.get(AV_TS_GRP_ASSGN.AV_TS_GRP_ASSGN.ATTRIBUTE);

            Integer attr = null;
            if (attrBD != null) {
                attr = attrBD.intValue();
            }
            retval = new AssignedTimeSeries(officeId, timeseriesId, tsCode, aliasId, refTsId, attr);
        }

        return retval;
    }

    private TimeSeriesGroup buildTimeSeriesGroup(Record queryRecord) {
        TimeSeriesCategory cat = buildTimeSeriesCategory(queryRecord);

        String grpOfficeId = queryRecord.get(AV_TS_CAT_GRP.AV_TS_CAT_GRP.GRP_DB_OFFICE_ID);
        String grpId = queryRecord.get(AV_TS_CAT_GRP.AV_TS_CAT_GRP.TS_GROUP_ID);
        String grpDesc = queryRecord.get(AV_TS_CAT_GRP.AV_TS_CAT_GRP.TS_GROUP_DESC);
        String sharedAliasId = queryRecord.get(AV_TS_CAT_GRP.AV_TS_CAT_GRP.SHARED_TS_ALIAS_ID);
        String sharedRefTsId = queryRecord.get(AV_TS_CAT_GRP.AV_TS_CAT_GRP.SHARED_REF_TS_ID);

        return new TimeSeriesGroup(cat, grpOfficeId, grpId, grpDesc, sharedAliasId, sharedRefTsId);
    }

    @NotNull
    private TimeSeriesCategory buildTimeSeriesCategory(Record queryRecord) {
        String catOfficeId = queryRecord.get(AV_TS_CAT_GRP.AV_TS_CAT_GRP.CAT_DB_OFFICE_ID);
        String catId = queryRecord.get(AV_TS_CAT_GRP.AV_TS_CAT_GRP.TS_CATEGORY_ID);
        String catDesc = queryRecord.get(AV_TS_CAT_GRP.AV_TS_CAT_GRP.TS_CATEGORY_DESC);
        return new TimeSeriesCategory(catOfficeId, catId, catDesc);
    }


    private Condition buildWhereCondition(String groupOfficeId, String categoryId, String groupId) {
        AV_TS_CAT_GRP atcg = AV_TS_CAT_GRP.AV_TS_CAT_GRP;
        Condition whereCondition = DSL.noCondition();

        if (groupOfficeId != null && !groupOfficeId.isEmpty()) {
            //We do not need to filter on the category office id since category ids
            //are unique unlike group ids.
            //We also don't want to filter on assigned time series' offices since that could leave
            //incomplete groups. If that is ever needed, it should be a separate context variable.
            whereCondition = whereCondition.and(atcg.GRP_DB_OFFICE_ID.eq(groupOfficeId));
        }

        if (categoryId != null && !categoryId.isEmpty()) {
            whereCondition = whereCondition.and(atcg.TS_CATEGORY_ID.eq(categoryId));
        }

        if (groupId != null && !groupId.isEmpty()) {
            whereCondition = whereCondition.and(atcg.TS_GROUP_ID.eq(groupId));
        }
        return whereCondition;
    }


    public void delete(String categoryId, String groupId, String office) {
        connection(dsl, c ->
            CWMS_TS_PACKAGE.call_DELETE_TS_GROUP(
                getDslContext(c,office).configuration(), categoryId, groupId, office
            )
        );
    }

    public void create(TimeSeriesGroup group, boolean failIfExists) {
        connection(dsl, c-> {
            Configuration configuration = getDslContext(c,group.getOfficeId()).configuration();
            String categoryId = group.getTimeSeriesCategory().getId();
            CWMS_TS_PACKAGE.call_STORE_TS_GROUP(configuration, categoryId,
            group.getId(), group.getDescription(), formatBool(failIfExists),
            "T", group.getSharedAliasId(),
            group.getSharedRefTsId(), group.getOfficeId());
            assignTs(configuration,group);
        });
        
    }

    private void assignTs(Configuration configuration,TimeSeriesGroup group) {
        List<AssignedTimeSeries> assignedTimeSeries = group.getAssignedTimeSeries();
        if(assignedTimeSeries != null)
        {
            List<TS_ALIAS_T> collect = assignedTimeSeries.stream()
                .map(TimeSeriesGroupDao::convertToTsAliasType)
                .collect(toList());
            TS_ALIAS_TAB_T assignedLocs = new TS_ALIAS_TAB_T(collect);
            CWMS_TS_PACKAGE.call_ASSIGN_TS_GROUPS(configuration, group.getTimeSeriesCategory().getId(),
                group.getId(), assignedLocs, group.getOfficeId());
        }
    }

    public void assignTs(TimeSeriesGroup group) {
        connection(dsl, c->assignTs(getDslContext(c,group.getOfficeId()).configuration(),group));
    }

    private static TS_ALIAS_T convertToTsAliasType(AssignedTimeSeries assignedTimeSeries) {
        BigDecimal attribute = toBigDecimal(assignedTimeSeries.getAttribute());
        return new TS_ALIAS_T(assignedTimeSeries.getTimeseriesId(), attribute,
            assignedTimeSeries.getAliasId(), assignedTimeSeries.getRefTsId());
    }

    public void renameTimeSeriesGroup(String oldGroupId, TimeSeriesGroup group) {
        connection(dsl, c->
            CWMS_TS_PACKAGE.call_RENAME_TS_GROUP(
                getDslContext(c,group.getOfficeId()).configuration(), 
                group.getTimeSeriesCategory().getId(), oldGroupId, group.getId(),
                group.getOfficeId())
        );
    }

    public void unassignAllTs(TimeSeriesGroup group) {
        connection(dsl, c ->
            CWMS_TS_PACKAGE.call_UNASSIGN_TS_GROUP(
                getDslContext(c,group.getOfficeId()).configuration(), 
                group.getTimeSeriesCategory().getId(), group.getId(),
                null, "T", group.getOfficeId())
        );
    }


}
