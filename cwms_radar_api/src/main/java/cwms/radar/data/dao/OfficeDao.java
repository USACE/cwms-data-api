package cwms.radar.data.dao;

import java.util.List;

import cwms.radar.data.dto.Office;
import org.jooq.DSLContext;

import usace.cwms.db.jooq.codegen.tables.AV_OFFICE;

public class OfficeDao extends JooqDao<Office>
{
	public OfficeDao(DSLContext dsl)
	{
		super(dsl);
	}


	public List<Office> getOffices() {
		List<Office> retval;
		AV_OFFICE view = AV_OFFICE.AV_OFFICE;
		// The .as snippets lets it map directly into the Office ctor fields.
		retval = dsl.select(view.OFFICE_ID.as("name"), view.LONG_NAME, view.OFFICE_TYPE.as("type"),
				view.REPORT_TO_OFFICE_ID.as("reportsTo")).from(view).fetch().into(
				Office.class);

		return retval;
	}

	public Office getOfficeById(String officeId) {
		AV_OFFICE view = AV_OFFICE.AV_OFFICE;
		// The .as snippets lets it map directly into the Office ctor fields.
		return dsl.select(view.OFFICE_ID.as("name"), view.LONG_NAME, view.OFFICE_TYPE.as("type"),
				view.REPORT_TO_OFFICE_ID.as("reportsTo")).from(view).where(view.OFFICE_ID.eq(officeId)).fetchOne().into(
				Office.class);
	}


}
