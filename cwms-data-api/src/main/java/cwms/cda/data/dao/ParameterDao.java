package cwms.cda.data.dao;

import cwms.cda.data.dto.Parameter;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import cwms.cda.data.dto.ParameterLegacy;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import cwms.cda.formatters.FormattingException;
import mil.army.usace.hec.metadata.UnitUtil;
import org.jooq.DSLContext;
import org.jooq.Record;
import usace.cwms.db.jooq.codegen.packages.CWMS_CAT_PACKAGE;

import java.io.IOException;
import java.util.List;
import cwms.cda.formatters.json.JsonV2;

import static java.util.stream.Collectors.toList;

public class ParameterDao extends JooqDao<ParameterDao> {

    private static final String LEGACY_JSON_FIELD_NAME = "parameters";

    public ParameterDao(DSLContext dsl) {
        super(dsl);
    }

    public String getParameters(String format) {
        String retVal = CWMS_CAT_PACKAGE.call_RETRIEVE_PARAMETERS_F(dsl.configuration(), format);
        if (Formats.JSON_LEGACY.equals(format)) {
            retVal = fixDefaultUnits(format, retVal);
        }
        return retVal;
    }

    public List<Parameter> getParametersV2(String office)
    {
        return CWMS_CAT_PACKAGE.call_CAT_PARAMETER(dsl.configuration(), office)
                                                      .stream()
                                                      .map(this::buildParameter)
                                                      .collect(toList());
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

    private String fixDefaultUnits(String format, String retVal) {
        try {
            ObjectMapper mapper = JsonV2.buildObjectMapper();
            JsonNode root = mapper.readTree(retVal);
            JsonNode wrapper = root.path(LEGACY_JSON_FIELD_NAME);
            JsonNode paramsNode = wrapper.path(LEGACY_JSON_FIELD_NAME);
            ContentType contentType = new ContentType(format);
            List<ParameterLegacy> params = Formats.parseContentList(contentType, paramsNode.toString(), ParameterLegacy.class);
            params = params.stream()
                    .map(this::fixDefaultUnits)
                    .collect(toList());
            ArrayNode newArray = mapper.valueToTree(params);
            ((ObjectNode) wrapper).set(LEGACY_JSON_FIELD_NAME, newArray);
            retVal = mapper.writeValueAsString(root);
        } catch (IOException e) {
            throw new FormattingException("Error processing legacy JSON: " + e.getMessage(), e);
        }
        return retVal;
    }

    private ParameterLegacy fixDefaultUnits(ParameterLegacy parameterLegacy) {
        var param = mil.army.usace.hec.metadata.Parameter.getParameterForUnitsString(parameterLegacy.getDefaultSiUnit());
        String siUnits = param.getUnitsStringForSystem(UnitUtil.SI_ID);
        String enUnits = param.getUnitsStringForSystem(UnitUtil.ENGLISH_ID);
        parameterLegacy = new ParameterLegacy.Builder()
            .from(parameterLegacy)
            .withDefaultSiUnit(siUnits)
            .withDefaultEnglishUnit(enUnits)
            .build();
        return parameterLegacy;
    }
}
