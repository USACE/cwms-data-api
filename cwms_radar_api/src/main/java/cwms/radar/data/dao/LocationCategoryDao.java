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

package cwms.radar.data.dao;

import cwms.radar.data.dto.LocationCategory;
import java.util.List;
import java.util.Optional;
import org.jooq.DSLContext;
import org.jooq.Record3;
import usace.cwms.db.dao.util.OracleTypeMap;
import usace.cwms.db.jooq.codegen.packages.CWMS_LOC_PACKAGE;
import usace.cwms.db.jooq.codegen.tables.AV_LOC_CAT_GRP;

public final class LocationCategoryDao extends JooqDao<LocationCategory>
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

		return dsl.selectDistinct(table.CAT_DB_OFFICE_ID,
				table.LOC_CATEGORY_ID, table.LOC_CATEGORY_DESC)
				.from(table)
				.where(table.CAT_DB_OFFICE_ID.eq(officeId))
				.fetch().into(LocationCategory.class);
	}

	public Optional<LocationCategory> getLocationCategory(String officeId, String categoryId)
	{
		AV_LOC_CAT_GRP table = AV_LOC_CAT_GRP.AV_LOC_CAT_GRP;

		 Record3<String, String, String> fetchOne = dsl.selectDistinct(table.CAT_DB_OFFICE_ID,
				table.LOC_CATEGORY_ID, table.LOC_CATEGORY_DESC)
				.from(table)
				.where(table.CAT_DB_OFFICE_ID.eq(officeId)
						.and(table.LOC_CATEGORY_ID.eq(categoryId)))
				.fetchOne();
		return fetchOne != null ?
			Optional.of(fetchOne.into(LocationCategory.class)) : Optional.empty();
	}

	public void delete(String categoryId, boolean cascade, String office) {
		String cascadeParam = OracleTypeMap.formatBool(cascade);
		CWMS_LOC_PACKAGE.call_DELETE_LOC_CAT(dsl.configuration(), categoryId, cascadeParam, office);
	}

	public void create(LocationCategory category) {
		CWMS_LOC_PACKAGE.call_CREATE_LOC_CATEGORY(dsl.configuration(), category.getId(), category.getDescription(),
			category.getOfficeId());
	}

	public void update(String oldCategoryId, String newCategoryId, String office) {
		CWMS_LOC_PACKAGE.call_RENAME_LOC_CATEGORY(dsl.configuration(), oldCategoryId, newCategoryId, null, "T", office);
	}
}
