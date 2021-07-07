package cwms.radar.data.dao;

import java.util.List;

import cwms.radar.data.dto.TimeSeriesGroup;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Select;
import org.jooq.SelectJoinStep;
import org.jooq.SelectWhereStep;
import org.jooq.TableField;

import usace.cwms.db.jooq.codegen.tables.AV_TS_CAT_GRP;

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
		AV_TS_CAT_GRP table = AV_TS_CAT_GRP.AV_TS_CAT_GRP;

		org.jooq.TableField [] columns = new TableField[]{
				table.CAT_DB_OFFICE_ID, table.TS_CATEGORY_ID,
				table.TS_CATEGORY_DESC, table.GRP_DB_OFFICE_ID, table.TS_GROUP_ID,
				table.TS_GROUP_DESC, table.SHARED_TS_ALIAS_ID, table.SHARED_REF_TS_ID
		};

		SelectJoinStep<Record> step
				= dsl.selectDistinct(columns)
				.from(table);

		Select select = step;

		if ( officeId != null && !officeId.isEmpty())
		{
			select = step.where(table.GRP_DB_OFFICE_ID.eq(officeId));
		}

		return select
				//  .orderBy(AV_TS_CAT_GRP.AV_TS_CAT_GRP.TS_GROUP_ATTRIBUTE)
				.fetch().into(TimeSeriesGroup.class);
	}

	public List<TimeSeriesGroup> getTimeSeriesGroups(String officeId, String categoryId, String groupId)
	{
		AV_TS_CAT_GRP table = AV_TS_CAT_GRP.AV_TS_CAT_GRP;

		org.jooq.TableField [] columns = new TableField[]{
				table.CAT_DB_OFFICE_ID, table.TS_CATEGORY_ID,
				table.TS_CATEGORY_DESC, table.GRP_DB_OFFICE_ID, table.TS_GROUP_ID,
				table.TS_GROUP_DESC, table.SHARED_TS_ALIAS_ID, table.SHARED_REF_TS_ID
		};

		SelectWhereStep<Record> step = dsl.selectDistinct(columns).from(table);

		Select select = step;

		Condition whereCondition = buildWhereCondition(officeId, categoryId, groupId, table);

		if(whereCondition != null)
		{
			select = step.where(whereCondition);
		}

		return select.fetch().into(TimeSeriesGroup.class);
	}

	private Condition buildWhereCondition(String officeId, String categoryId, String groupId, AV_TS_CAT_GRP table)
	{
		Condition whereCondition = null;
		if ( officeId != null && !officeId.isEmpty())
		{
			whereCondition = and(whereCondition, table.GRP_DB_OFFICE_ID.eq(officeId));
		}

		if ( categoryId != null && !categoryId.isEmpty())
		{
			whereCondition = and(whereCondition, table.TS_CATEGORY_ID.eq(categoryId));
		}

		if ( groupId != null && !groupId.isEmpty())
		{
			whereCondition = and(whereCondition, table.TS_GROUP_ID.eq(groupId));
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
