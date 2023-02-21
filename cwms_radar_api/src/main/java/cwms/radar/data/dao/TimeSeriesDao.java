package cwms.radar.data.dao;

import cwms.radar.data.dto.Catalog;
import cwms.radar.data.dto.RecentValue;
import cwms.radar.data.dto.TimeSeries;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;

public interface TimeSeriesDao {
    enum DeleteMethod {
        DELETE_ALL, DELETE_KEY, DELETE_DATA
    }

    Timestamp NON_VERSIONED = null;

    Catalog getTimeSeriesCatalog(String cursor, int pageSize, String office);

    Catalog getTimeSeriesCatalog(String cursor, int pageSize, String office, String idLike,
                                 String locCategoryLike, String locGroupLike,
                                 String tsCategoryLike, String tsGroupLike);

    void create(TimeSeries input);

    void create(TimeSeries input, int utcOffsetMinutes, int intervalForward, int intervalBackward,
                boolean versionedFlag, boolean activeFlag, Timestamp versionDate,
                boolean createAsLrts, StoreRule replaceAll, boolean overrideProtection);

    void store(TimeSeries timeSeries, Timestamp versionDate, boolean createAsLrts,
               StoreRule replaceAll, boolean overrideProtection);

    void deleteAll(String office, String tsId);

    void deleteData(String office, String tsId);

    void deleteKey(String office, String tsId);

    TimeSeries getTimeseries(String cursor, int pageSize, String names, String office,
                             String unit, String datum, ZonedDateTime begin, ZonedDateTime end,
                             ZoneId timezone);

    String getTimeseries(String format, String names, String office, String unit, String datum,
                         ZonedDateTime begin, ZonedDateTime end, ZoneId timezone);


    List<RecentValue> findRecentsInRange(String office, String categoryId, String groupId,
                                         Timestamp pastLimit, Timestamp futureLimit);

    List<RecentValue> findMostRecentsInRange(List<String> tsIds, Timestamp pastLimit, Timestamp futureLimit);

}
