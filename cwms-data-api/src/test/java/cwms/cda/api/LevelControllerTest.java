package cwms.cda.api;

import org.junit.jupiter.api.Test;

import cwms.cda.data.dto.LocationLevel;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.xml.adapters.ZonedDateTimeAdapter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class LevelControllerTest extends ControllerTest
{
    private static final String OFFICE_ID = "LRL";
    @Test
    void testDeserializeSeasonalLevelXml() throws Exception
    {
        ZonedDateTimeAdapter dateTimeAdapter = new ZonedDateTimeAdapter();
        String xml = loadResourceAsString("cwms/cda/api/levels_seasonal_create.xml");
        assertNotNull(xml);
        LocationLevel level = Formats.parseContent(Formats.parseHeader(Formats.XML, LocationLevel.class), xml, LocationLevel.class);
        assertNotNull(level);
        assertEquals("LOC_TEST.Elev.Inst.0.Bottom of Inlet", level.getLocationLevelId());
        assertEquals(OFFICE_ID, level.getOfficeId());
        assertEquals("ft", level.getLevelUnitsId());
        assertEquals(dateTimeAdapter.unmarshal("2008-12-03T10:15:30+01:00[Z]").toInstant(), level.getLevelDate().toInstant());
        assertEquals(10.0, level.getSeasonalValues().get(0).getValue());
    }

    @Test
    void testDeserializeSeasonalLevelJSON() throws Exception
    {
        ZonedDateTimeAdapter dateTimeAdapter = new ZonedDateTimeAdapter();
        String json = loadResourceAsString("cwms/cda/api/levels_seasonal_create.json");
        assertNotNull(json);
        LocationLevel level = Formats.parseContent(Formats.parseHeader(Formats.JSONV1, LocationLevel.class), json, LocationLevel.class);
        assertNotNull(level);
        assertEquals("LOC_TEST.Elev.Inst.0.Bottom of Inlet", level.getLocationLevelId());
        assertEquals(OFFICE_ID, level.getOfficeId());
        assertEquals("ft", level.getLevelUnitsId());
        assertEquals(dateTimeAdapter.unmarshal("2008-12-03T10:15:30+01:00[Z]"), level.getLevelDate());
        assertEquals(10.0, level.getSeasonalValues().get(0).getValue());
    }

    @Test
    void testDeserializeConstantLevelXml() throws Exception
    {
        ZonedDateTimeAdapter dateTimeAdapter = new ZonedDateTimeAdapter();
        String xml = loadResourceAsString("cwms/cda/api/levels_constant_create.xml");
        assertNotNull(xml);
        LocationLevel level = Formats.parseContent(Formats.parseHeader(Formats.XML), xml, LocationLevel.class);
        assertNotNull(level);
        assertEquals("LOC_TEST.Elev.Inst.0.Bottom of Inlet", level.getLocationLevelId());
        assertEquals(OFFICE_ID, level.getOfficeId());
        assertEquals("ft", level.getLevelUnitsId());
        assertEquals(dateTimeAdapter.unmarshal("2008-12-03T10:15:30+01:00[Z]").toInstant(), level.getLevelDate().toInstant());
        assertEquals(10.0, level.getConstantValue());
    }

    @Test
    void testDeserializeConstantLevelJSON() throws Exception
    {
        ZonedDateTimeAdapter dateTimeAdapter = new ZonedDateTimeAdapter();
        String json = loadResourceAsString("cwms/cda/api/levels_constant_create.json");
        assertNotNull(json);
        LocationLevel level = Formats.parseContent(Formats.parseHeader(Formats.JSONV1, LocationLevel.class), json, LocationLevel.class);
        assertNotNull(level);
        assertEquals("LOC_TEST.Elev.Inst.0.Bottom of Inlet", level.getLocationLevelId());
        assertEquals(OFFICE_ID, level.getOfficeId());
        assertEquals("ft", level.getLevelUnitsId());
        assertEquals(dateTimeAdapter.unmarshal("2008-12-03T10:15:30+01:00[Z]"), level.getLevelDate());
        assertEquals(10.0, level.getConstantValue());
    }

    @Test
    void testDeserializeTimeSeriesLevelXml() throws Exception
    {
        ZonedDateTimeAdapter dateTimeAdapter = new ZonedDateTimeAdapter();
        String xml = loadResourceAsString("cwms/cda/api/levels_timeseries_create.xml");
        assertNotNull(xml);
        LocationLevel level = Formats.parseContent(Formats.parseHeader(Formats.XML), xml, LocationLevel.class);
        assertNotNull(level);
        assertEquals("LOC_TEST.Elev.Inst.0.Bottom of Inlet", level.getLocationLevelId());
        assertEquals(OFFICE_ID, level.getOfficeId());
        assertEquals("ft", level.getLevelUnitsId());
        assertEquals(dateTimeAdapter.unmarshal("2008-12-03T10:15:30+01:00[Z]").toInstant(), level.getLevelDate().toInstant());
        assertEquals("RYAN3.Stage.Inst.5Minutes.0.ZSTORE_TS_TEST630", level.getSeasonalTimeSeriesId());
    }

    @Test
    void testDeserializeTimeSeriesLevelJSON() throws Exception
    {
        ZonedDateTimeAdapter dateTimeAdapter = new ZonedDateTimeAdapter();
        String json = loadResourceAsString("cwms/cda/api/levels_timeseries_create.json");
        assertNotNull(json);
        LocationLevel level = Formats.parseContent(Formats.parseHeader(Formats.JSONV1, LocationLevel.class), json, LocationLevel.class);
        assertNotNull(level);
        assertEquals("LOC_TEST.Elev.Inst.0.Bottom of Inlet", level.getLocationLevelId());
        assertEquals(OFFICE_ID, level.getOfficeId());
        assertEquals("ft", level.getLevelUnitsId());
        assertEquals(dateTimeAdapter.unmarshal("2008-12-03T10:15:30+01:00[Z]"), level.getLevelDate());
        assertEquals("RYAN3.Stage.Inst.5Minutes.0.ZSTORE_TS_TEST630", level.getSeasonalTimeSeriesId());
    }
}
