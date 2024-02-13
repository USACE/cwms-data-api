package cwms.cda.data.dao;

import org.jooq.DSLContext;
import usace.cwms.db.jooq.codegen.packages.CWMS_CAT_PACKAGE;

public class UnitsDao extends JooqDao<String> {

    public UnitsDao(DSLContext dsl) {
        super(dsl);
    }

    public String getUnits(String format) {
        return CWMS_CAT_PACKAGE.call_RETRIEVE_UNITS_F(dsl.configuration(), format);
    }
}
