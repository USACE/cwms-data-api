package cwms.cda.data.dao;

import cwms.cda.data.dto.Unit;
import org.jooq.DSLContext;
import org.jooq.Record;
import usace.cwms.db.jooq.codegen.packages.CWMS_CAT_PACKAGE;
import usace.cwms.db.jooq.codegen.tables.AV_UNIT;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.jooq.impl.DSL.table;

public class UnitsDao extends JooqDao<String> {

    public UnitsDao(DSLContext dsl) {
        super(dsl);
    }

    public String getUnits(String format) {
        return CWMS_CAT_PACKAGE.call_RETRIEVE_UNITS_F(dsl.configuration(), format);
    }

    public List<Unit> getUnits()
    {
        Map<Long, List<String>> unitIdToAliasMap = dsl.select()
                                                      .from(table("CWMS_20.AT_UNIT_ALIAS"))
                                                      .fetch()
                                                      .stream()
                                                      .collect(Collectors.groupingBy(rec -> rec.get("UNIT_CODE", Long.class), Collectors.mapping(rec -> rec.get("ALIAS_ID", String.class), Collectors.toList())));
        return dsl.select()
                  .from(AV_UNIT.AV_UNIT)
                  .fetch()
                  .stream()
                  .map(rec -> buildUnit(rec, unitIdToAliasMap))
                  .collect(Collectors.toList());
    }

    private Unit buildUnit(Record rec, Map<Long, List<String>> unitIdToAliasMap)
    {
        AV_UNIT view = AV_UNIT.AV_UNIT;
        String unitId = rec.get(view.UNIT_ID);
        String longName = rec.get(view.LONG_NAME);
        String unitSystem = rec.get(view.UNIT_SYSTEM);
        String description = rec.get(view.DESCRIPTION);
        String abstractParameter = rec.get(view.ABSTRACT_PARAM_ID);
        List<String> aliases = unitIdToAliasMap.getOrDefault(rec.get(view.UNIT_CODE), new ArrayList<>());
        return new Unit(unitId, longName, abstractParameter, description, unitSystem, aliases);
    }
}
