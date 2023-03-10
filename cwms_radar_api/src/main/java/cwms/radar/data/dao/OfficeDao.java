package cwms.radar.data.dao;

import java.util.List;
import java.util.Optional;

import cwms.radar.data.dto.Office;
import org.jooq.DSLContext;
import org.jooq.Record4;
import org.jooq.Table;

import usace.cwms.db.jooq.codegen.tables.AV_OFFICE;
import usace.cwms.db.jooq.codegen.tables.AV_LOC2;

public class OfficeDao extends JooqDao<Office>
{
	public OfficeDao(DSLContext dsl)
	{
        super(dsl);
    }

    public List<Office> getOffices(Boolean hasData) {
        List<Office> retval;
        AV_OFFICE view = AV_OFFICE.AV_OFFICE;

        if (hasData) {
            // #321 - Return offices with operation data only i.e. hasData
            AV_LOC2 locView = AV_LOC2.AV_LOC2;
            Table<?> locViewDistinct = dsl.selectDistinct(locView.DB_OFFICE_ID)
                    .from(locView)
                    .where(locView.LOCATION_CODE.ne(0L).and(locView.DB_OFFICE_ID.ne("CWMS")))
                    .asTable("locViewDistinct");
            retval = dsl.select(view.OFFICE_ID.as("name"), view.LONG_NAME, view.OFFICE_TYPE.as("type"),
                    view.REPORT_TO_OFFICE_ID.as("reportsTo"))
                    .from(view)
                    .join(locViewDistinct).on(view.OFFICE_ID.eq(locViewDistinct.field(locView.DB_OFFICE_ID)))
                    .fetch()
                    .into(Office.class);
        } else {
            // The .as snippets lets it map directly into the Office ctor fields.
            retval = dsl.select(view.OFFICE_ID.as("name"), view.LONG_NAME, view.OFFICE_TYPE.as("type"),
                    view.REPORT_TO_OFFICE_ID.as("reportsTo")).from(view).fetch().into(
                            Office.class);
        }

        return retval;
    }

    public Optional<Office> getOfficeById(String officeId) {
        AV_OFFICE view = AV_OFFICE.AV_OFFICE;
        // The .as snippets lets it map directly into the Office ctor fields.
		Record4<String, String, String, String> fetchOne = dsl.select(view.OFFICE_ID.as("name"), view.LONG_NAME, view.OFFICE_TYPE.as("type"),
				view.REPORT_TO_OFFICE_ID.as("reportsTo")).from(view).where(view.OFFICE_ID.eq(officeId)).fetchOne();
		return fetchOne != null ?
				Optional.of(fetchOne.into(Office.class)) : Optional.empty();
    }


}
