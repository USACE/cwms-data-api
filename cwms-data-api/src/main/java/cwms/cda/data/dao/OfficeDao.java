package cwms.cda.data.dao;

import static org.jooq.impl.DSL.upper;

import cwms.cda.data.dto.Office;
import java.util.List;
import java.util.Optional;
import org.jooq.DSLContext;
import org.jooq.Record4;
import org.jooq.Table;
import usace.cwms.db.jooq.codegen.tables.AV_LOC2;
import usace.cwms.db.jooq.codegen.tables.AV_OFFICE;

public class OfficeDao extends JooqDao<Office> {
    public OfficeDao(DSLContext dsl) {
        super(dsl);
    }

    /**
     * Returns all offices in CDA.
     * * @param hasData specifies whether the office has data in CDA or not (all
     * offices)
     *
     * @return a list of offices
     *
     * @see List
     */
    public List<Office> getOffices(Boolean hasData) {
        List<Office> retVal;
        AV_OFFICE view = AV_OFFICE.AV_OFFICE;

        if (hasData) {
            // Return offices present in the loc2 table
            AV_LOC2 locView = AV_LOC2.AV_LOC2;
            Table<?> locViewDistinct = dsl.selectDistinct(locView.DB_OFFICE_ID)
                    .from(locView)
                    .where(locView.LOCATION_CODE.ne(0L).and(locView.DB_OFFICE_ID.ne("CWMS")))
                    .asTable("locViewDistinct");
            retVal = dsl.select(view.OFFICE_ID.as("name"),
                    view.LONG_NAME, view.OFFICE_TYPE.as("type"),
                    view.REPORT_TO_OFFICE_ID.as("reportsTo"))
                    .from(view)
                    .join(locViewDistinct)
                    .on(view.OFFICE_ID.eq(locViewDistinct.field(locView.DB_OFFICE_ID)))
                    .fetch()
                    .into(Office.class);
        } else {
            // The .as snippets lets it map directly into the Office ctor fields.
            retVal = dsl.select(view.OFFICE_ID.as("name"),
                    view.LONG_NAME,
                    view.OFFICE_TYPE.as("type"),
                    view.REPORT_TO_OFFICE_ID.as("reportsTo"))
                    .from(view)
                    .fetch()
                    .into(Office.class);
        }

        return retVal;
    }

    /**
     * Returns a specific office from CDA given an office ID (3-4 capital letters).
     * * @param officeId Case insensitive (3-4 letter) ID of a district
     *
     * @return a single office and its metadata
     *
     * @see Optional
     */
    public Optional<Office> getOfficeById(String officeId) {
        AV_OFFICE view = AV_OFFICE.AV_OFFICE;
        // The .as snippets lets it map directly into the Office ctor fields.
        Record4<String, String, String, String> fetchOne = dsl.select(view.OFFICE_ID.as("name"),
                view.LONG_NAME,
                view.OFFICE_TYPE.as("type"),
                view.REPORT_TO_OFFICE_ID.as("reportsTo"))
                .from(view)
                .where(upper(view.OFFICE_ID).eq(upper(officeId)))
                .fetchOne();
        return fetchOne != null
                ? Optional.of(fetchOne.into(Office.class))
                : Optional.empty();
    }

}
