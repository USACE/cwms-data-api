package cwms.cda.data.dao;

import cwms.cda.data.dto.TimeZoneId;
import cwms.cda.data.dto.TimeZoneIds;
import org.jooq.DSLContext;
import org.jooq.Record1;
import usace.cwms.db.jooq.codegen.packages.CWMS_CAT_PACKAGE;
import usace.cwms.db.jooq.codegen.tables.MV_TIME_ZONE;

import java.util.stream.Collectors;

public class TimeZoneDao extends JooqDao<String> {

    public TimeZoneDao(DSLContext dsl) {
        super(dsl);
    }

    public String getTimeZones(String format) {
        return CWMS_CAT_PACKAGE.call_RETRIEVE_TIME_ZONES_F(dsl.configuration(), format);
    }

    public TimeZoneIds getTimeZones()
    {
        return new TimeZoneIds(dsl.select(MV_TIME_ZONE.MV_TIME_ZONE.TIME_ZONE_NAME)
                                .from(MV_TIME_ZONE.MV_TIME_ZONE)
                                .stream()
                                .map(Record1::component1)
                                .map(TimeZoneId::new)
                                .collect(Collectors.toList()));
    }
}
