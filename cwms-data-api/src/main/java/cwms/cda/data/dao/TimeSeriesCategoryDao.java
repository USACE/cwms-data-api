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

import cwms.cda.data.dto.TimeSeriesCategory;
import java.util.List;
import java.util.Optional;
import org.jooq.DSLContext;
import org.jooq.Record3;
import org.jooq.Select;
import org.jooq.SelectWhereStep;
import usace.cwms.db.dao.util.OracleTypeMap;
import usace.cwms.db.jooq.codegen.packages.CWMS_TS_PACKAGE;
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

	public void delete(String categoryId, boolean cascadeDelete, String office) {
		this.setOffice(office);
		dsl.connection((c)-> 
			CWMS_TS_PACKAGE.call_DELETE_TS_CATEGORY(
				getDslContext(c,office).configuration(), categoryId,
				OracleTypeMap.formatBool(cascadeDelete), office)
		);
	}

	public void create(TimeSeriesCategory category, boolean failIfExists) {
		this.setOffice(category);
		dsl.connection((c) -> 
			CWMS_TS_PACKAGE.call_STORE_TS_CATEGORY(
				getDslContext(c,category.getOfficeId()).configuration(), category.getId(), category.getDescription(),
				OracleTypeMap.formatBool(failIfExists), "T", category.getOfficeId())
		);
	}
}
