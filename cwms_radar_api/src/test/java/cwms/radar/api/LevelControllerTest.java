package cwms.radar.api;

import cwms.radar.data.dto.LocationLevel;
import cwms.radar.formatters.Formats;
import cwms.radar.formatters.xml.adapters.ZonedDateTimeAdapter;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class LevelControllerTest extends ControllerTest
{
    private static final String OFFICE_ID = "LRL";
    @Test
    void testDeserializeSeasonalLevelXml() throws Exception
    {
        ZonedDateTimeAdapter dateTimeAdapter = new ZonedDateTimeAdapter();
        String xml = loadResourceAsString("cwms/radar/api/levels_seasonal_create.xml");
        assertNotNull(xml);
        LocationLevel level = LevelsController.deserializeLocationLevel(xml, Formats.XML, OFFICE_ID);
        assertNotNull(level);
        assertEquals("LOC_TEST.Elev.Inst.0.Bottom of Inlet", level.getLocationId());
        assertEquals(OFFICE_ID, level.getOfficeId());
        assertEquals("ft", level.getLevelUnitsId());
        assertEquals(dateTimeAdapter.unmarshal("2008-12-03T10:15:30+01:00[UTC]"), level.getLevelDate());
        assertEquals(10.0, level.getSeasonalValues().get(0).getValue());
    }

    @Test
    void testDeserializeSeasonalLevelJSON() throws Exception
    {
        ZonedDateTimeAdapter dateTimeAdapter = new ZonedDateTimeAdapter();
        String json = loadResourceAsString("cwms/radar/api/levels_seasonal_create.json");
        assertNotNull(json);
        LocationLevel level = LevelsController.deserializeLocationLevel(json, Formats.JSON, OFFICE_ID);
        assertNotNull(level);
        assertEquals("LOC_TEST.Elev.Inst.0.Bottom of Inlet", level.getLocationId());
        assertEquals(OFFICE_ID, level.getOfficeId());
        assertEquals("ft", level.getLevelUnitsId());
        assertEquals(dateTimeAdapter.unmarshal("2008-12-03T10:15:30+01:00[UTC]"), level.getLevelDate());
        assertEquals(10.0, level.getSeasonalValues().get(0).getValue());
    }

    @Test
    void testDeserializeConstantLevelXml() throws Exception
    {
        ZonedDateTimeAdapter dateTimeAdapter = new ZonedDateTimeAdapter();
        String xml = loadResourceAsString("cwms/radar/api/levels_constant_create.xml");
        assertNotNull(xml);
        LocationLevel level = LevelsController.deserializeLocationLevel(xml, Formats.XML, OFFICE_ID);
        assertNotNull(level);
        assertEquals("LOC_TEST.Elev.Inst.0.Bottom of Inlet", level.getLocationId());
        assertEquals(OFFICE_ID, level.getOfficeId());
        assertEquals("ft", level.getLevelUnitsId());
        assertEquals(dateTimeAdapter.unmarshal("2008-12-03T10:15:30+01:00[UTC]"), level.getLevelDate());
        assertEquals(10.0, level.getSiParameterUnitsConstantValue());
    }

    @Test
    void testDeserializeConstantLevelJSON() throws Exception
    {
        ZonedDateTimeAdapter dateTimeAdapter = new ZonedDateTimeAdapter();
        String json = loadResourceAsString("cwms/radar/api/levels_constant_create.json");
        assertNotNull(json);
        LocationLevel level = LevelsController.deserializeLocationLevel(json, Formats.JSON, OFFICE_ID);
        assertNotNull(level);
        assertEquals("LOC_TEST.Elev.Inst.0.Bottom of Inlet", level.getLocationId());
        assertEquals(OFFICE_ID, level.getOfficeId());
        assertEquals("ft", level.getLevelUnitsId());
        assertEquals(dateTimeAdapter.unmarshal("2008-12-03T10:15:30+01:00[UTC]"), level.getLevelDate());
        assertEquals(10.0, level.getSiParameterUnitsConstantValue());
    }

    @Test
    void testDeserializeTimeSeriesLevelXml() throws Exception
    {
        ZonedDateTimeAdapter dateTimeAdapter = new ZonedDateTimeAdapter();
        String xml = loadResourceAsString("cwms/radar/api/levels_timeseries_create.xml");
        assertNotNull(xml);
        LocationLevel level = LevelsController.deserializeLocationLevel(xml, Formats.XML, OFFICE_ID);
        assertNotNull(level);
        assertEquals("LOC_TEST.Elev.Inst.0.Bottom of Inlet", level.getLocationId());
        assertEquals(OFFICE_ID, level.getOfficeId());
        assertEquals("ft", level.getLevelUnitsId());
        assertEquals(dateTimeAdapter.unmarshal("2008-12-03T10:15:30+01:00[UTC]"), level.getLevelDate());
        assertEquals("RYAN3.Stage.Inst.5Minutes.0.ZSTORE_TS_TEST630", level.getSeasonalTimeSeriesId());
    }

    @Test
    void testDeserializeTimeSeriesLevelJSON() throws Exception
    {
        ZonedDateTimeAdapter dateTimeAdapter = new ZonedDateTimeAdapter();
        String json = loadResourceAsString("cwms/radar/api/levels_timeseries_create.json");
        assertNotNull(json);
        LocationLevel level = LevelsController.deserializeLocationLevel(json, Formats.JSON, OFFICE_ID);
        assertNotNull(level);
        assertEquals("LOC_TEST.Elev.Inst.0.Bottom of Inlet", level.getLocationId());
        assertEquals(OFFICE_ID, level.getOfficeId());
        assertEquals("ft", level.getLevelUnitsId());
        assertEquals(dateTimeAdapter.unmarshal("2008-12-03T10:15:30+01:00[UTC]"), level.getLevelDate());
        assertEquals("RYAN3.Stage.Inst.5Minutes.0.ZSTORE_TS_TEST630", level.getSeasonalTimeSeriesId());
    }
}
