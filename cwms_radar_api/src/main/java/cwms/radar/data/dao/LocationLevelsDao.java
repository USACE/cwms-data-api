package cwms.radar.data.dao;

import cwms.radar.data.dto.LocationLevel;

import java.io.IOException;
import java.time.ZoneId;
import java.time.ZonedDateTime;

public interface LocationLevelsDao
{
    void deleteLocationLevel(String locationLevelName, ZonedDateTime date, String officeId, Boolean cascadeDelete) throws IOException;
    void storeLocationLevel(LocationLevel level, ZoneId zoneId) throws IOException;
    void renameLocationLevel(String oldLocationLevelName, LocationLevel renamedLocationLevel) throws IOException;
    LocationLevel retrieveLocationLevel(String locationLevelName, String unitSystem, ZonedDateTime effectiveDate, String officeId) throws IOException;
}
