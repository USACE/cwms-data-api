package cwms.radar.data.dao;

import cwms.radar.data.dto.LocationLevel;
import usace.cwms.db.dao.ifc.level.LocationLevelPojo;

import java.io.IOException;
import java.time.ZoneId;

public interface LocationLevelsDao
{
    void deleteLocationLevel(String locationLevelName, String officeId) throws IOException;
    void storeLocationLevel(LocationLevel level) throws IOException;
    void renameLocationLevel(String oldLocationLevelName, LocationLevel renamedLocationLevel) throws IOException;
    LocationLevel retrieveLocationLevel(String locationLevelName, String unitSystem, ZoneId timeZoneId, String officeId) throws IOException;
}
