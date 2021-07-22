package cwms.radar.data.dao;

import java.util.List;
import java.util.Optional;

import cwms.radar.data.dto.LocationCategory;
import org.jooq.DSLContext;
import org.jooq.Record3;

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

	public Optional<LocationCategory> getLocationCategory(String officeId, String categoryId)
	{
		AV_LOC_CAT_GRP table = AV_LOC_CAT_GRP.AV_LOC_CAT_GRP;

		 Record3<String, String, String> fetchOne = dsl.select(table.CAT_DB_OFFICE_ID,
				table.LOC_CATEGORY_ID, table.LOC_CATEGORY_DESC)
				.from(table)
				.where(table.CAT_DB_OFFICE_ID.eq(officeId)
						.and(table.LOC_CATEGORY_ID.eq(categoryId)))
				.fetchOne();
		return fetchOne != null ?
			Optional.of(fetchOne.into(LocationCategory.class)) : Optional.empty();
	}
}
