package cwms.cda.data.dao;

import cwms.cda.data.dto.Unit;
import org.jooq.DSLContext;
import usace.cwms.db.jooq.codegen.packages.CWMS_CAT_PACKAGE;

import java.util.ArrayList;
import java.util.List;

public class UnitsDao extends JooqDao<String> {

    public UnitsDao(DSLContext dsl) {
        super(dsl);
    }

    public String getUnits(String format) {
        return CWMS_CAT_PACKAGE.call_RETRIEVE_UNITS_F(dsl.configuration(), format);
    }

    public List<Unit> getUnits()
    {
        //Todo
        return new ArrayList<>();
    }
}
