package cwms.radar.data.dao;

import org.jooq.DSLContext;
import usace.cwms.db.jooq.codegen.packages.CWMS_CAT_PACKAGE;

public class ParameterDao extends JooqDao {
    public ParameterDao(DSLContext dsl) {
        super(dsl);
    }


    public String getParameters(String format) {
        return CWMS_CAT_PACKAGE.call_RETRIEVE_PARAMETERS_F(dsl.configuration(), format);
    }


}
