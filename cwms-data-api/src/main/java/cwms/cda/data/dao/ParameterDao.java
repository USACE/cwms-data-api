package cwms.cda.data.dao;

import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.impl.DSL;
import usace.cwms.db.jooq.codegen.packages.CWMS_CAT_PACKAGE;
import usace.cwms.db.jooq.codegen.packages.cwms_cat.CAT_BASE_PARAMETER;
import usace.cwms.db.jooq.codegen.tables.AV_UNIT;

import java.util.stream.Collectors;

public class ParameterDao extends JooqDao<ParameterDao> {

    public ParameterDao(DSLContext dsl) {
        super(dsl);
    }

    public String getParameters(String format) {
        return CWMS_CAT_PACKAGE.call_RETRIEVE_PARAMETERS_F(dsl.configuration(), format);
    }

    public String getParameters()
    {
        return CWMS_CAT_PACKAGE.call_CAT_PARAMETER(dsl.configuration(), null)
                               .stream()
                               .map(Record::intoStream)
                               .map(str -> str.map(String.class::cast)
                                              .collect(Collectors.joining(", ")))
                               .collect(Collectors.joining("\n"));
    }
}
