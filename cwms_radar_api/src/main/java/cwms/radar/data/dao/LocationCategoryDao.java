package cwms.radar.data.dao;

import java.util.List;

import cwms.radar.data.dto.LocationCategory;
import org.jooq.DSLContext;

import usace.cwms.db.jooq.codegen.tables.AV_LOC_CAT_GRP;

public class LocationCategoryDao extends JooqDao<LocationCategory>
{
	public LocationCategoryDao(DSLContext dsl)
	{
		super(dsl);
	}

	public List<LocationCategory> getLocationCategories()
	{
		AV_LOC_CAT_GRP table = AV_LOC_CAT_GRP.AV_LOC_CAT_GRP;

		return dsl.selectDistinct(
				table.CAT_DB_OFFICE_ID,
				table.LOC_CATEGORY_ID,
				table.LOC_CATEGORY_DESC)
				.from(table)
				.fetch().into(LocationCategory.class);
	}

	public List<LocationCategory> getLocationCategories(String officeId)
	{
		if(officeId == null || officeId.isEmpty()){
			return getLocationCategories();
		}
		AV_LOC_CAT_GRP table = AV_LOC_CAT_GRP.AV_LOC_CAT_GRP;

		return dsl.select(table.CAT_DB_OFFICE_ID,
				table.LOC_CATEGORY_ID, table.LOC_CATEGORY_DESC)
				.from(table)
				.where(table.CAT_DB_OFFICE_ID.eq(officeId))
				.fetch().into(LocationCategory.class);
	}

	public LocationCategory getLocationCategory(String officeId, String categoryId)
	{
		AV_LOC_CAT_GRP table = AV_LOC_CAT_GRP.AV_LOC_CAT_GRP;

		return dsl.select(table.CAT_DB_OFFICE_ID,
				table.LOC_CATEGORY_ID, table.LOC_CATEGORY_DESC)
				.from(table)
				.where(table.CAT_DB_OFFICE_ID.eq(officeId)
						.and(table.LOC_CATEGORY_ID.eq(categoryId)))
				.fetchOne().into(LocationCategory.class);
	}
}
