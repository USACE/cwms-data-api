package cwms.radar.data.dao;

import java.util.List;
import java.util.Optional;

import cwms.radar.data.dto.TimeSeriesCategory;
import org.jooq.DSLContext;
import org.jooq.Record3;
import org.jooq.Select;
import org.jooq.SelectWhereStep;

import usace.cwms.db.jooq.codegen.tables.AV_TS_CAT_GRP;

public class TimeSeriesCategoryDao extends JooqDao<TimeSeriesCategory>
{

	public TimeSeriesCategoryDao(DSLContext dsl)
	{
		super(dsl);
	}

	public Optional<TimeSeriesCategory> getTimeSeriesCategory(String officeId, String categoryId)
	{
		AV_TS_CAT_GRP view = AV_TS_CAT_GRP.AV_TS_CAT_GRP;

		Record3<String, String, String> fetchOne = dsl.selectDistinct(view.CAT_DB_OFFICE_ID, view.TS_CATEGORY_ID,
				view.TS_CATEGORY_DESC)
				.from(view)
				.where(view.CAT_DB_OFFICE_ID.eq(officeId))
				.and(view.TS_CATEGORY_ID.eq(categoryId))
				.fetchOne();

		return fetchOne != null ?
			Optional.of(fetchOne.into(TimeSeriesCategory.class)) : Optional.empty();
	}

	public List<TimeSeriesCategory> getTimeSeriesCategories(String officeId)
	{
		AV_TS_CAT_GRP table = AV_TS_CAT_GRP.AV_TS_CAT_GRP;

		SelectWhereStep<Record3<String, String, String>> step = dsl.selectDistinct(
				table.CAT_DB_OFFICE_ID, table.TS_CATEGORY_ID,table.TS_CATEGORY_DESC)
				.from(table);
		Select select;
		if ( officeId != null && !officeId.isEmpty())
		{
			select = step.where(table.CAT_DB_OFFICE_ID.eq(officeId));
		}else {
			 select = step;
		}

		return select.fetch().into(TimeSeriesCategory.class);
	}

	public List<TimeSeriesCategory> getTimeSeriesCategories()
	{
		return getTimeSeriesCategories(null);
	}

}
