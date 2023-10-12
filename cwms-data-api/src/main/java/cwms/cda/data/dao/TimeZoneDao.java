package cwms.cda.data.dao;

import org.jooq.DSLContext;
import usace.cwms.db.jooq.codegen.packages.CWMS_CAT_PACKAGE;

public class TimeZoneDao extends JooqDao<String> {

    public TimeZoneDao(DSLContext dsl) {
        super(dsl);
    }

    public String getTimeZones(String format) {
        return CWMS_CAT_PACKAGE.call_RETRIEVE_TIME_ZONES_F(dsl.configuration(), format);
    }
}
