package cwms.cda.api;

import cwms.cda.data.dto.locationlevel.ConstantLocationLevel;
import cwms.cda.data.dto.locationlevel.SeasonalLocationLevel;
import cwms.cda.data.dto.locationlevel.TimeSeriesLocationLevel;
import org.junit.jupiter.api.Test;

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
        SeasonalLocationLevel level = Formats.parseContent(Formats.parseHeader(Formats.XML, SeasonalLocationLevel.class), xml, SeasonalLocationLevel.class);
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
        SeasonalLocationLevel level = Formats.parseContent(Formats.parseHeader(Formats.JSONV1, SeasonalLocationLevel.class), json, SeasonalLocationLevel.class);
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
        ConstantLocationLevel level = Formats.parseContent(Formats.parseHeader(Formats.XML, ConstantLocationLevel.class), xml, ConstantLocationLevel.class);
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
        ConstantLocationLevel level = Formats.parseContent(Formats.parseHeader(Formats.JSONV1, ConstantLocationLevel.class), json, ConstantLocationLevel.class);
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
        TimeSeriesLocationLevel level = Formats.parseContent(Formats.parseHeader(Formats.XML, TimeSeriesLocationLevel.class),
            xml, TimeSeriesLocationLevel.class);
        assertNotNull(level);
        assertEquals("LOC_TEST.Elev.Inst.0.Bottom of Inlet", level.getLocationLevelId());
        assertEquals(OFFICE_ID, level.getOfficeId());
        assertEquals("ft", level.getLevelUnitsId());
        assertEquals(dateTimeAdapter.unmarshal("2008-12-03T10:15:30+01:00[Z]").toInstant(),
            level.getLevelDate().toInstant());
        assertEquals("RYAN3.Stage.Inst.5Minutes.0.ZSTORE_TS_TEST630", level.getSeasonalTimeSeriesId());
    }

    @Test
    void testDeserializeTimeSeriesLevelJSON() throws Exception
    {
        ZonedDateTimeAdapter dateTimeAdapter = new ZonedDateTimeAdapter();
        String json = loadResourceAsString("cwms/cda/api/levels_timeseries_create.json");
        assertNotNull(json);
        TimeSeriesLocationLevel level = Formats.parseContent(Formats.parseHeader(Formats.JSONV1, TimeSeriesLocationLevel.class), json, TimeSeriesLocationLevel.class);
        assertNotNull(level);
        assertEquals("LOC_TEST.Elev.Inst.0.Bottom of Inlet", level.getLocationLevelId());
        assertEquals(OFFICE_ID, level.getOfficeId());
        assertEquals("ft", level.getLevelUnitsId());
        assertEquals(dateTimeAdapter.unmarshal("2008-12-03T10:15:30+01:00[Z]"), level.getLevelDate());
        assertEquals("RYAN3.Stage.Inst.5Minutes.0.ZSTORE_TS_TEST630", level.getSeasonalTimeSeriesId());
    }
}
