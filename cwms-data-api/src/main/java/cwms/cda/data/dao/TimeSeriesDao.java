package cwms.cda.data.dao;

import cwms.cda.data.dto.Catalog;
import cwms.cda.data.dto.RecentValue;
import cwms.cda.data.dto.TimeSeries;

import java.sql.Time;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;

public interface TimeSeriesDao {

    Timestamp NON_VERSIONED = null;

    Catalog getTimeSeriesCatalog(String cursor, int pageSize, String office);

    Catalog getTimeSeriesCatalog(String cursor, int pageSize, String office, String idLike,
                                 String locCategoryLike, String locGroupLike,
                                 String tsCategoryLike, String tsGroupLike, String boundingOfficeLike);

    void create(TimeSeries input);

    void create(TimeSeries input,
                Timestamp versionDate,
                boolean createAsLrts, StoreRule replaceAll, boolean overrideProtection);

    void store(TimeSeries timeSeries, Timestamp versionDate);
    void store(TimeSeries timeSeries, Timestamp versionDate, boolean createAsLrts,
               StoreRule replaceAll, boolean overrideProtection);

    void delete(String officeId, String tsId, TimeSeriesDeleteOptions options);

    TimeSeries getTimeseries(String cursor, int pageSize, String names, String office,
                             String unit, String datum, ZonedDateTime begin, ZonedDateTime end,
                             ZoneId timezone, Timestamp versionDate);

    String getTimeseries(String format, String names, String office, String unit, String datum,
                         ZonedDateTime begin, ZonedDateTime end, ZoneId timezone);


    List<RecentValue> findRecentsInRange(String office, String categoryId, String groupId,
                                         Timestamp pastLimit, Timestamp futureLimit);

    List<RecentValue> findMostRecentsInRange(List<String> tsIds, Timestamp pastLimit, Timestamp futureLimit);

}
