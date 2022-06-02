package cwms.radar.data.dao;

import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.introspect.BeanPropertyDefinition;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import cwms.radar.api.enums.Nation;
import cwms.radar.api.enums.Unit;
import cwms.radar.api.enums.UnitSystem;
import cwms.radar.data.dto.Location;
import cwms.radar.data.dto.LocationLevel;
import cwms.radar.data.dto.SeasonalValueBean;
import cwms.radar.formatters.Formats;
import cwms.radar.formatters.xml.adapters.ZonedDateTimeAdapter;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class LocationLevelsDaoTest extends DaoTest
{

    private static final String OFFICE_ID = "LRL";

    @Disabled
    void testStore() throws Exception
    {
        LocationLevel levelToStore = null;
        Location location = null;
        try {
            levelToStore = buildExampleLevel("TEST_LOC");
            LocationsDao locationsDao = new LocationsDaoImpl(getDslContext(getConnection(), OFFICE_ID));
            location = buildTestLocation("TEST_LOC");
            locationsDao.storeLocation(location);
            LocationLevelsDao levelsDao = new LocationLevelsDaoImpl(getDslContext(getConnection(), OFFICE_ID));
            levelsDao.storeLocationLevel(levelToStore, ZoneId.of("UTC"));
            LocationLevel retrievedLevel = levelsDao.retrieveLocationLevel(levelToStore.getLocationLevelId(), UnitSystem.EN.getValue(), levelToStore.getLevelDate(), "LRL");
            assertNotNull(retrievedLevel);
            assertEquals(levelToStore.getLocationLevelId(), retrievedLevel.getLocationLevelId());
            assertEquals(levelToStore.getLevelDate(), retrievedLevel.getLevelDate());
        }
        finally {
            if(levelToStore != null)
            {
                deleteLevel(levelToStore);
            }
        }
    }

    @Disabled
    void testDeleteLocationLevel() throws Exception
    {
        LocationLevelsDao levelsDao = new LocationLevelsDaoImpl(getDslContext(getConnection(), "LRL"));
        LocationLevel levelToStore = buildExampleLevel("TEST_LOC5");
        LocationsDao locationsDao = new LocationsDaoImpl(getDslContext(getConnection(), OFFICE_ID));
        Location location = buildTestLocation("TEST_LOC5");
        locationsDao.storeLocation(location);
        levelsDao.storeLocationLevel(levelToStore, ZoneId.of("UTC"));
        LocationLevel retrievedLevel = levelsDao.retrieveLocationLevel(levelToStore.getLocationLevelId(), UnitSystem.EN.getValue(), levelToStore.getLevelDate(), OFFICE_ID);
        assertNotNull(retrievedLevel);
        levelsDao.deleteLocationLevel(levelToStore.getLocationLevelId(), levelToStore.getLevelDate(), OFFICE_ID, true);
        assertThrows(IOException.class, () -> levelsDao.retrieveLocationLevel(levelToStore.getLocationLevelId(), UnitSystem.EN.getValue(), levelToStore.getLevelDate(), OFFICE_ID));
    }

    @Disabled
    void testUpdate() throws Exception
    {
        LocationLevel updatedLocationLevel = null;
        LocationLevel existingLocationLevel = null;
        LocationLevel levelToStore = buildExampleLevel("TEST_LOC6");
        LocationsDao locationsDao = new LocationsDaoImpl(getDslContext(getConnection(), OFFICE_ID));
        LocationLevelsDao levelsDao = new LocationLevelsDaoImpl(getDslContext(getConnection(), OFFICE_ID));
        Location location = buildTestLocation("TEST_LOC6");
        try {
            locationsDao.storeLocation(location);
            levelsDao.storeLocationLevel(levelToStore, ZoneId.of("UTC"));
            String body = getRenamedExampleJSON();
            String format = Formats.JSON;

            LocationLevel levelFromBody = deserializeLocationLevel(body, Formats.JSON, OFFICE_ID);
            existingLocationLevel = levelsDao.retrieveLocationLevel(levelToStore.getLocationLevelId(), UnitSystem.EN.getValue(), levelFromBody.getLevelDate(), OFFICE_ID);
            existingLocationLevel = updatedClearedFields(body, format, existingLocationLevel);
            //only store (update) if level does exist
            updatedLocationLevel = getUpdatedLocationLevel(existingLocationLevel, levelFromBody);
            updatedLocationLevel = new LocationLevel.Builder(updatedLocationLevel).withLevelDate(levelFromBody.getLevelDate()).build();
            if (!updatedLocationLevel.getLocationLevelId().equalsIgnoreCase(existingLocationLevel.getLocationLevelId())) //if name changed then delete location with old name
            {
                levelsDao.renameLocationLevel(levelToStore.getLocationLevelId(), updatedLocationLevel);
            } else {
                levelsDao.storeLocationLevel(updatedLocationLevel, updatedLocationLevel.getLevelDate().getZone());
            }
            LocationLevel retrievedLevel = levelsDao.retrieveLocationLevel(updatedLocationLevel.getLocationLevelId(), UnitSystem.EN.getValue(), updatedLocationLevel.getLevelDate(), OFFICE_ID);
            assertNotNull(retrievedLevel);
        }
        finally {
            if(updatedLocationLevel != null)
            {
                deleteLevel(updatedLocationLevel);
            }
        }
    }

    private LocationLevel deserializeLocationLevel(String body, String format, String office) throws IOException
    {
        ObjectMapper om = getObjectMapperForFormat(format);
        LocationLevel retVal;
        try
        {
            retVal = om.readValue(body, LocationLevel.class);
            retVal = new LocationLevel.Builder(retVal).withOfficeId(office).build();
        }
        catch(Exception e)
        {
            throw new IOException("Failed to deserialize level");
        }
        return retVal;
    }

    private static ObjectMapper getObjectMapperForFormat(String format)
    {
        ObjectMapper om = new ObjectMapper();
        if((Formats.XML).equals(format))
        {
           // om = new XmlMapper();
        }
        om.registerModule(new JavaTimeModule());
        return om;
    }


    String getExampleJSON()
    {
        return "{ \"seasonal-values\": [ { \"value\": 10, \"offset-minutes\": 0, \"offset-months\": 0 }],"+
                "\"level-units-id\": \"ft\","+
                "\"level-date\": \"1999-12-05T10:15:30+01:00[UTC]\","+
                "\"level-comment\": \"for testing purposes\","+
                "\"interval-origin\": \"1999-12-05T10:15:30+01:00[UTC]\","+
                "\"interval-months\": 1,"+
                "\"interpolate-string\": \"false\","+
                "\"location-level-id\": \"TEST_LOC.Elev.Inst.0.Bottom of Inlet\","+
                "\"office-id\": \"LRL\""+
                "}";
    }

    String getRenamedExampleJSON()
    {
        return "{ \"seasonal-values\": [ { \"value\": 10, \"offset-minutes\": 0, \"offset-months\": 0 }],"+
                "\"level-units-id\": \"ft\","+
                "\"level-date\": \"1999-12-05T10:15:30+01:00[UTC]\","+
                "\"level-comment\": \"for testing purposes\","+
                "\"interval-origin\": \"1999-12-05T10:15:30+01:00[UTC]\","+
                "\"interval-months\": 1,"+
                "\"interpolate-string\": \"false\","+
                "\"location-level-id\": \"TEST_LOC6.Elev.Inst.0.Top of Inlet\","+
                "\"office-id\": \"LRL\""+
                "}";
    }

    private String getExampleUpdatedJSON()
    {
        return "{ \"seasonal-values\": [ { \"value\": 10, \"offset-minutes\": 0, \"offset-months\": 0 }],"+
                "\"level-units-id\": \"ft\","+
                "\"level-date\": \"1999-12-05T10:15:30+01:00[UTC]\","+
                "\"level-comment\": \"for testing purposes updated\","+
                "\"interval-origin\": \"1999-12-05T10:15:30+01:00[UTC]\","+
                "\"interval-months\": 1,"+
                "\"interpolate-string\": \"false\","+
                "\"location-level-id\": \"TEST_LOC.Elev.Inst.0.Bottom of Inlet\","+
                "\"office-id\": \"LRL\""+
                "}";
    }

    private LocationLevel updatedClearedFields(String body, String format, LocationLevel existingLocation) throws IOException
    {
        ObjectMapper om = getObjectMapperForFormat(format);
        JsonNode root = om.readTree(body);
        JavaType javaType = om.getTypeFactory().constructType(LocationLevel.class);
        BeanDescription beanDescription = om.getSerializationConfig().introspect(javaType);
        List<BeanPropertyDefinition> properties = beanDescription.findProperties();
        LocationLevel retVal = new LocationLevel.Builder(existingLocation).build();
        try
        {
            for (BeanPropertyDefinition propertyDefinition : properties) {
                String propertyName = propertyDefinition.getName();
                JsonNode propertyValue = root.findValue(propertyName);
                if (propertyValue != null && "".equals(propertyValue.textValue())) {
                    retVal = new LocationLevel.Builder(retVal).withProperty(propertyName, null).build();
                }
            }
        }
        catch (NullPointerException e) //gets thrown if required field is null
        {
            throw new IOException(e.getMessage());
        }
        return retVal;
    }

    private LocationLevel getUpdatedLocationLevel(LocationLevel existinglocation, LocationLevel updatedLevel)
    {
        String seasonalTimeSeriesId = (updatedLevel.getSeasonalTimeSeriesId() == null ? existinglocation.getSeasonalTimeSeriesId() : updatedLevel.getSeasonalTimeSeriesId());
        List<SeasonalValueBean> seasonalValues = (updatedLevel.getSeasonalValues() == null ? existinglocation.getSeasonalValues() : updatedLevel.getSeasonalValues());
        String specifiedLevelId = (updatedLevel.getSpecifiedLevelId() == null ? existinglocation.getSpecifiedLevelId() : updatedLevel.getSpecifiedLevelId());
        String parameterTypeId = (updatedLevel.getParameterTypeId() == null ? existinglocation.getParameterTypeId() : updatedLevel.getParameterTypeId());
        String parameterId = (updatedLevel.getParameterId() == null ? existinglocation.getParameterId() : updatedLevel.getParameterId());
        Double siParameterUnitsConstantValue = (updatedLevel.getConstantValue() == null ? existinglocation.getConstantValue() : updatedLevel.getConstantValue());
        String levelUnitsId = (updatedLevel.getLevelUnitsId() == null ? existinglocation.getLevelUnitsId() : updatedLevel.getLevelUnitsId());
        ZonedDateTime levelDate = (updatedLevel.getLevelDate() == null ? existinglocation.getLevelDate() : updatedLevel.getLevelDate());
        String levelComment = (updatedLevel.getLevelComment() == null ? existinglocation.getLevelComment() : updatedLevel.getLevelComment());
        ZonedDateTime intervalOrigin = (updatedLevel.getIntervalOrigin() == null ? existinglocation.getIntervalOrigin() : updatedLevel.getIntervalOrigin());
        Integer intervalMinutes = (updatedLevel.getIntervalMinutes() == null ? existinglocation.getIntervalMinutes() : updatedLevel.getIntervalMinutes());
        Integer intervalMonths = (updatedLevel.getIntervalMonths() == null ? existinglocation.getIntervalMonths() : updatedLevel.getIntervalMonths());
        String interpolateString = (updatedLevel.getInterpolateString() == null ? existinglocation.getInterpolateString() : updatedLevel.getInterpolateString());
        String durationId = (updatedLevel.getDurationId() == null ? existinglocation.getDurationId() : updatedLevel.getDurationId());
        BigDecimal attributeValue = (updatedLevel.getAttributeValue() == null ? existinglocation.getAttributeValue() : updatedLevel.getAttributeValue());
        String attributeUnitsId = (updatedLevel.getAttributeUnitsId() == null ? existinglocation.getAttributeUnitsId() : updatedLevel.getAttributeUnitsId());
        String attributeParameterTypeId = (updatedLevel.getAttributeParameterTypeId() == null ? existinglocation.getAttributeParameterTypeId() : updatedLevel.getAttributeParameterTypeId());
        String attributeParameterId = (updatedLevel.getAttributeParameterId() == null ? existinglocation.getAttributeParameterId() : updatedLevel.getAttributeParameterId());
        String attributeDurationId = (updatedLevel.getAttributeDurationId() == null ? existinglocation.getAttributeDurationId() : updatedLevel.getAttributeDurationId());
        String attributeComment = (updatedLevel.getAttributeComment() == null ? existinglocation.getAttributeComment() : updatedLevel.getAttributeComment());
        String locationId = (updatedLevel.getLocationLevelId() == null ? existinglocation.getLocationLevelId() : updatedLevel.getLocationLevelId());
        String officeId = (updatedLevel.getOfficeId() == null ? existinglocation.getOfficeId() : updatedLevel.getOfficeId());
        if(existinglocation.getIntervalMonths() != null && existinglocation.getIntervalMonths() > 0)
        {
            intervalMinutes = null;
        }
        else if(existinglocation.getIntervalMinutes() != null && existinglocation.getIntervalMinutes() > 0)
        {
            intervalMonths = null;
        }
        if(existinglocation.getAttributeValue() == null)
        {
            attributeUnitsId = null;
        }
        if(!existinglocation.getSeasonalValues().isEmpty())
        {
            siParameterUnitsConstantValue = null;
            seasonalTimeSeriesId = null;
        }
        else if(existinglocation.getSeasonalTimeSeriesId() != null && !existinglocation.getSeasonalTimeSeriesId().isEmpty())
        {
            siParameterUnitsConstantValue = null;
            seasonalValues = null;
        }
        return new LocationLevel.Builder(locationId, levelDate)
                .withSeasonalValues(seasonalValues)
                .withSeasonalTimeSeriesId(seasonalTimeSeriesId)
                .withSpecifiedLevelId(specifiedLevelId)
                .withParameterTypeId(parameterTypeId)
                .withParameterId(parameterId)
                .withConstantValue(siParameterUnitsConstantValue)
                .withLevelUnitsId(levelUnitsId)
                .withLevelComment(levelComment)
                .withIntervalOrigin(intervalOrigin)
                .withIntervalMinutes(intervalMinutes)
                .withIntervalMonths(intervalMonths)
                .withInterpolateString(interpolateString)
                .withDurationId(durationId)
                .withAttributeValue(attributeValue)
                .withAttributeUnitsId(attributeUnitsId)
                .withAttributeParameterTypeId(attributeParameterTypeId)
                .withAttributeParameterId(attributeParameterId)
                .withAttributeDurationId(attributeDurationId)
                .withAttributeComment(attributeComment)
                .withOfficeId(officeId)
                .build();
    }

    private void deleteLevel(LocationLevel level) throws Exception
    {
        LocationLevelsDao levelsDao = new LocationLevelsDaoImpl(getDslContext(getConnection(), "LRL"));
        levelsDao.deleteLocationLevel(level.getLocationLevelId(), level.getLevelDate(), level.getOfficeId(), true);
    }

    private void deleteLocation(Location location) throws Exception
    {
        LocationsDao locationsDao = new LocationsDaoImpl(getDslContext(getConnection(), "LRL"));
        locationsDao.deleteLocation(location.getName(), location.getOfficeId());
    }

    LocationLevel buildExampleLevel(String locationName) throws Exception
    {
        String dateString = "1999-12-05T10:15:30+01:00[UTC]";
        List<SeasonalValueBean> seasonalValues = new ArrayList<>();
        SeasonalValueBean seasonalVal = new SeasonalValueBean.Builder(10.0)
                .withOffsetMinutes(BigInteger.valueOf(0))
                .withOffsetMonths(0)
                .build();
        seasonalValues.add(0, seasonalVal);
        ZonedDateTimeAdapter zonedDateTimeAdapter = new ZonedDateTimeAdapter();
        ZonedDateTime unmarshalledDateTime = zonedDateTimeAdapter.unmarshal(dateString);
        LocationLevel retval = new LocationLevel.Builder(locationName + ".Elev.Inst.0.Bottom of Inlet", unmarshalledDateTime)
                .withOfficeId(OFFICE_ID)
                .withLevelComment("For testing")
                .withLevelUnitsId(Unit.FEET.getValue())
                .withSeasonalValues(seasonalValues)
                .withIntervalMonths(1)
                .build();
        return retval;
    }

    private Location buildTestLocation(String name) {
        return new Location.Builder(name, "SITE", ZoneId.of("UTC"), 50.0, 50.0, "NVGD29", OFFICE_ID)
                .withElevation(10.0)
                .withCountyName("Sacramento")
                .withNation(Nation.US)
                .withActive(true)
                .withStateInitial("CA")
                .withBoundingOfficeId(OFFICE_ID)
                .withLongName("TEST_LOCATION")
                .withPublishedLatitude(50.0)
                .withPublishedLongitude(50.0)
                .withDescription("for testing")
                .build();
    }
}
