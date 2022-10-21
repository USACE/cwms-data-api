package cwms.radar.data.dao;

import cwms.radar.data.dto.AssignedTimeSeries;
import cwms.radar.data.dto.TimeSeriesCategory;
import cwms.radar.data.dto.TimeSeriesGroup;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import kotlin.Pair;
import org.jetbrains.annotations.NotNull;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.RecordMapper;
import org.jooq.SelectOnConditionStep;
import org.jooq.SelectOrderByStep;
import org.jooq.SelectSeekStep1;
import org.jooq.conf.ParamType;
import usace.cwms.db.jooq.codegen.tables.AV_TS_CAT_GRP;
import usace.cwms.db.jooq.codegen.tables.AV_TS_GRP_ASSGN;

public class TimeSeriesGroupDao extends JooqDao<TimeSeriesGroup> {
    private static final Logger logger = Logger.getLogger(TimeSeriesGroupDao.class.getName());

    public TimeSeriesGroupDao(DSLContext dsl) {
        super(dsl);
    }

    public List<TimeSeriesGroup> getTimeSeriesGroups() {
        return getTimeSeriesGroups(null);
    }

    public List<TimeSeriesGroup> getTimeSeriesGroups(String officeId) {
        Condition whereCond = null;
        if (officeId != null) {
            whereCond = AV_TS_CAT_GRP.AV_TS_CAT_GRP.GRP_DB_OFFICE_ID.eq(officeId);
        }

        return getTimeSeriesGroupsWhere(whereCond);
    }

    public List<TimeSeriesGroup> getTimeSeriesGroups(String officeId, String categoryId,
                                                     String groupId) {
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
                .from(catGrp).leftOuterJoin(grpAssgn)
                .on(catGrp.GRP_DB_OFFICE_ID.eq(grpAssgn.DB_OFFICE_ID)
                        .and(catGrp.TS_CATEGORY_ID.eq(grpAssgn.CATEGORY_ID))
                        .and(catGrp.TS_GROUP_ID.eq(grpAssgn.GROUP_ID)));

        SelectOrderByStep<?> select = selectOn;
        if (whereCond != null) {
            select = selectOn.where(whereCond);
        }

        SelectSeekStep1<?, BigDecimal> query = select.orderBy(grpAssgn.ATTRIBUTE);

        //logger.info(() -> query.getSQL(ParamType.INLINED));

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

    private AssignedTimeSeries buildAssignedTimeSeries(Record queryRecord) {
        AssignedTimeSeries retval = null;

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
            retval = new AssignedTimeSeries(timeseriesId, tsCode, aliasId, refTsId, attr);
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


    private Condition buildWhereCondition(String officeId, String categoryId, String groupId) {
        Condition whereCondition = null;
        if (officeId != null && !officeId.isEmpty()) {
            whereCondition = and(whereCondition,
                    AV_TS_CAT_GRP.AV_TS_CAT_GRP.GRP_DB_OFFICE_ID.eq(officeId));
        }

        if (categoryId != null && !categoryId.isEmpty()) {
            whereCondition = and(whereCondition,
                    AV_TS_CAT_GRP.AV_TS_CAT_GRP.TS_CATEGORY_ID.eq(categoryId));
        }

        if (groupId != null && !groupId.isEmpty()) {
            whereCondition = and(whereCondition,
                    AV_TS_CAT_GRP.AV_TS_CAT_GRP.TS_GROUP_ID.eq(groupId));
        }
        return whereCondition;
    }

    private Condition and(Condition whereCondition, Condition cond) {
        Condition retval;
        if (whereCondition == null) {
            retval = cond;
        } else {
            retval = whereCondition.and(cond);
        }
        return retval;
    }


}
