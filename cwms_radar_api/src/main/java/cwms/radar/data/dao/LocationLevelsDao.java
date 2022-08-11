package cwms.radar.data.dao;

import cwms.radar.data.dto.LocationLevel;
import cwms.radar.data.dto.LocationLevels;
import java.time.ZoneId;
import java.time.ZonedDateTime;

public interface LocationLevelsDao {
    void deleteLocationLevel(String locationLevelName, ZonedDateTime date, String officeId,
                             Boolean cascadeDelete);

    void storeLocationLevel(LocationLevel level, ZoneId zoneId);

    void renameLocationLevel(String oldLocationLevelName, LocationLevel renamedLocationLevel);

    LocationLevel retrieveLocationLevel(String locationLevelName, String unitSystem,
                                        ZonedDateTime effectiveDate, String officeId);

    String getLocationLevels(String format, String names, String office, String unit,
                             String datum, String begin,
                             String end, String timezone);

    LocationLevels getLocationLevels(String cursor, int pageSize,
                                     String names, String office, String unit, String datum,
                                     ZonedDateTime beginZdt, ZonedDateTime endZdt);
}
