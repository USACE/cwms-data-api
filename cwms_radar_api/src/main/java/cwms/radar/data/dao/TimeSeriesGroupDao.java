package cwms.radar.data.dao;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import cwms.radar.data.dto.AssignedTimeSeries;
import cwms.radar.data.dto.TimeSeriesCategory;
import cwms.radar.data.dto.TimeSeriesGroup;
import kotlin.Pair;
import org.jetbrains.annotations.NotNull;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Record16;
import org.jooq.RecordMapper;
import org.jooq.SelectConditionStep;
import org.jooq.SelectOnConditionStep;
import org.jooq.SelectOrderByStep;

import usace.cwms.db.jooq.codegen.tables.AV_TS_CAT_GRP;
import usace.cwms.db.jooq.codegen.tables.AV_TS_GRP_ASSGN;

public class TimeSeriesGroupDao extends JooqDao<TimeSeriesGroup>
{
	public TimeSeriesGroupDao(DSLContext dsl)
	{
		super(dsl);
	}

	public List<TimeSeriesGroup> getTimeSeriesGroups()
	{
		return getTimeSeriesGroups(null);
	}

	public List<TimeSeriesGroup> getTimeSeriesGroups(String officeId)
	{
		Condition whereCond = null;
		if(officeId != null)
		{
			whereCond = AV_TS_GRP_ASSGN.AV_TS_GRP_ASSGN.DB_OFFICE_ID.eq(officeId);
		}

		return getTimeSeriesGroupsWhere(whereCond);
	}

	@NotNull
	private List<TimeSeriesGroup> getTimeSeriesGroupsWhere(Condition whereCond)
	{
		List<TimeSeriesGroup> retval = new ArrayList<>();
		AV_TS_CAT_GRP catGrp = AV_TS_CAT_GRP.AV_TS_CAT_GRP;
		AV_TS_GRP_ASSGN grpAssgn = AV_TS_GRP_ASSGN.AV_TS_GRP_ASSGN;

		final RecordMapper<Record, Pair<TimeSeriesGroup, AssignedTimeSeries>> mapper = record15 -> {
			TimeSeriesGroup group = buildTimeSeriesGroup(record15);
			AssignedTimeSeries loc = buildAssignedTimeSeries(record15);

			return new Pair<>(group, loc);
		};

		SelectOnConditionStep<Record16<String, String, String, String, String, String, String, String, String, String, String, String, BigDecimal, BigDecimal, String, String>>
				selectOn = dsl.select(
				catGrp.CAT_DB_OFFICE_ID, catGrp.TS_CATEGORY_ID, catGrp.TS_CATEGORY_DESC, catGrp.GRP_DB_OFFICE_ID,
				catGrp.TS_GROUP_ID, catGrp.TS_GROUP_DESC, catGrp.SHARED_TS_ALIAS_ID, catGrp.SHARED_REF_TS_ID,
				grpAssgn.CATEGORY_ID, grpAssgn.DB_OFFICE_ID, grpAssgn.GROUP_ID, grpAssgn.TS_ID,
				grpAssgn.TS_CODE,grpAssgn.ATTRIBUTE,
				grpAssgn.ALIAS_ID, grpAssgn.REF_TS_ID)
				.from(catGrp)
				.leftOuterJoin(grpAssgn)
				.on(
				catGrp.TS_CATEGORY_ID.eq(grpAssgn.CATEGORY_ID)
						.and(catGrp.TS_GROUP_ID.eq(grpAssgn.GROUP_ID))
						.and(catGrp.GRP_DB_OFFICE_ID.eq(grpAssgn.DB_OFFICE_ID)));


		SelectOrderByStep<Record16<String, String, String, String, String, String, String, String, String, String, String, String, BigDecimal, BigDecimal, String, String>> select = selectOn;
		if(whereCond != null)
		{
			SelectConditionStep<Record16<String, String, String, String, String, String, String, String, String, String, String, String, BigDecimal, BigDecimal, String, String>> selectWhere = selectOn.where(
					whereCond);
			select = selectWhere;
		}

		List<Pair<TimeSeriesGroup, AssignedTimeSeries>> assignments = select
				.orderBy(grpAssgn.ATTRIBUTE).fetch(mapper);

		Map<TimeSeriesGroup, List<AssignedTimeSeries>> map = new LinkedHashMap<>();
		for(Pair<TimeSeriesGroup, AssignedTimeSeries> pair : assignments){
			List<AssignedTimeSeries> list = map.computeIfAbsent(pair.component1(), k -> new ArrayList<>());
			AssignedTimeSeries assignedTimeSeries = pair.component2();
			if(assignedTimeSeries != null)
			{
				list.add(assignedTimeSeries);
			}
		}

		for(final Map.Entry<TimeSeriesGroup, List<AssignedTimeSeries>> entry : map.entrySet())
		{
			List<AssignedTimeSeries> assigned = entry.getValue();

			retval.add(new TimeSeriesGroup(entry.getKey(), assigned));
		}
		return retval;
	}

	private AssignedTimeSeries buildAssignedTimeSeries(Record record15)
	{
		AssignedTimeSeries retval = null;

		String timeseriesId = record15.get(AV_TS_GRP_ASSGN.AV_TS_GRP_ASSGN.TS_ID);
		BigDecimal tsCode = record15.get(AV_TS_GRP_ASSGN.AV_TS_GRP_ASSGN.TS_CODE);

		if(timeseriesId != null && tsCode != null){
			String aliasId = record15.get(AV_TS_GRP_ASSGN.AV_TS_GRP_ASSGN.ALIAS_ID);
			String refTsId = record15.get(AV_TS_GRP_ASSGN.AV_TS_GRP_ASSGN.REF_TS_ID);
			BigDecimal attrBD = record15.get(AV_TS_GRP_ASSGN.AV_TS_GRP_ASSGN.ATTRIBUTE);

			Integer attr = null;
			if(attrBD != null){
				attr = attrBD.intValue();
			}
			retval = new AssignedTimeSeries(timeseriesId, tsCode, aliasId, refTsId, attr);
		}

		return retval;
	}

	private TimeSeriesGroup buildTimeSeriesGroup(Record record15)
	{
		TimeSeriesCategory cat = buildTimeSeriesCategory(record15);

		String grpOfficeId = record15.get(AV_TS_CAT_GRP.AV_TS_CAT_GRP.GRP_DB_OFFICE_ID);
		String grpId = record15.get(AV_TS_CAT_GRP.AV_TS_CAT_GRP.TS_GROUP_ID);
		String grpDesc = record15.get(AV_TS_CAT_GRP.AV_TS_CAT_GRP.TS_GROUP_DESC);
		String sharedAliasId = record15.get(AV_TS_CAT_GRP.AV_TS_CAT_GRP.SHARED_TS_ALIAS_ID);
		String sharedRefTsId = record15.get(AV_TS_CAT_GRP.AV_TS_CAT_GRP.SHARED_REF_TS_ID);

		return new TimeSeriesGroup(cat, grpOfficeId, grpId,	 grpDesc,
				sharedAliasId, sharedRefTsId);
	}

	@NotNull
	private TimeSeriesCategory buildTimeSeriesCategory(Record record15)
	{
		String catOfficeId = record15.get(AV_TS_CAT_GRP.AV_TS_CAT_GRP.CAT_DB_OFFICE_ID);
		String catId = record15.get(AV_TS_CAT_GRP.AV_TS_CAT_GRP.TS_CATEGORY_ID);
		String catDesc = record15.get(AV_TS_CAT_GRP.AV_TS_CAT_GRP.TS_CATEGORY_DESC);
		TimeSeriesCategory cat = new TimeSeriesCategory(catOfficeId, catId, catDesc);
		return cat;
	}

	public List<TimeSeriesGroup> getTimeSeriesGroups(String officeId, String categoryId, String groupId)
	{
		return getTimeSeriesGroupsWhere(buildWhereCondition(officeId, categoryId, groupId));
	}

	private Condition buildWhereCondition(String officeId, String categoryId, String groupId)
	{
		Condition whereCondition = null;
		if ( officeId != null && !officeId.isEmpty())
		{
			whereCondition = and(whereCondition, AV_TS_CAT_GRP.AV_TS_CAT_GRP.GRP_DB_OFFICE_ID.eq(officeId));
		}

		if ( categoryId != null && !categoryId.isEmpty())
		{
			whereCondition = and(whereCondition, AV_TS_CAT_GRP.AV_TS_CAT_GRP.TS_CATEGORY_ID.eq(categoryId));
		}

		if ( groupId != null && !groupId.isEmpty())
		{
			whereCondition = and(whereCondition, AV_TS_CAT_GRP.AV_TS_CAT_GRP.TS_GROUP_ID.eq(groupId));
		}
		return whereCondition;
	}

	private Condition and(Condition whereCondition, Condition cond)
	{
		Condition retval = null;
		if(whereCondition == null){
			retval = cond;
		} else {
			retval = whereCondition.and(cond);
		}
		return retval;
	}



}
