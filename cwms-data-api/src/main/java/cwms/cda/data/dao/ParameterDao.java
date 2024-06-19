package cwms.cda.data.dao;

import cwms.cda.data.dto.Parameter;
import cwms.cda.data.dto.Parameters;
import org.jooq.DSLContext;
import org.jooq.Record;
import usace.cwms.db.jooq.codegen.packages.CWMS_CAT_PACKAGE;

import java.util.stream.Collectors;

public class ParameterDao extends JooqDao<ParameterDao> {

    public ParameterDao(DSLContext dsl) {
        super(dsl);
    }

    public String getParameters(String format) {
        return CWMS_CAT_PACKAGE.call_RETRIEVE_PARAMETERS_F(dsl.configuration(), format);
    }

    public Parameters getParametersV2(String office)
    {
        return new Parameters(office, CWMS_CAT_PACKAGE.call_CAT_PARAMETER(dsl.configuration(), office)
                                                      .stream()
                                                      .map(this::buildParameter)
                                                      .collect(Collectors.toList()));
    }

    private Parameter buildParameter(Record record)
    {
        String param = record.get("PARAMETER_ID", String.class);
        String baseParam = record.get("BASE_PARAMETER_ID", String.class);
        String subParam = record.get("SUB_PARAMETER_ID", String.class);
        String subParamDesc = record.get("SUB_PARAMETER_DESC", String.class);
        String dbOfficeId = record.get("DB_OFFICE_ID", String.class);
        String dbUnitId = record.get("DB_UNIT_ID", String.class);
        String unitLongName = record.get("UNIT_LONG_NAME", String.class);
        String unitDesc = record.get("UNIT_DESCRIPTION", String.class);
        return new Parameter(param, baseParam, subParam, subParamDesc, dbOfficeId, dbUnitId, unitLongName, unitDesc);
    }
}
