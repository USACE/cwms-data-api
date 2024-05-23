package cwms.cda.data.dao;

import cwms.cda.api.enums.UnitSystem;
import cwms.cda.data.dto.Catalog;
import cwms.cda.data.dto.RecentValue;
import cwms.cda.data.dto.TimeSeries;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;

public interface TimeSeriesDao {

    Catalog getTimeSeriesCatalog(String page, int pageSize, CatalogRequestParameters inputParams);

    void create(TimeSeries input);

    void create(TimeSeries input,
                boolean createAsLrts, StoreRule replaceAll, boolean overrideProtection);

    void store(TimeSeries timeSeries, Timestamp versionDate);

    void store(TimeSeries timeSeries, boolean createAsLrts,
               StoreRule replaceAll, boolean overrideProtection);

    void delete(String officeId, String tsId, TimeSeriesDeleteOptions options);

    TimeSeries getTimeseries(String cursor, int pageSize, String names, String office,
                             String unit, ZonedDateTime begin, ZonedDateTime end,
                             ZonedDateTime versionDate, boolean trim);

    String getTimeseries(String format, String names, String office, String unit, String datum,
                         ZonedDateTime begin, ZonedDateTime end, ZoneId timezone);

    List<RecentValue> findRecentsInRange(String office, String categoryId, String groupId,
                                         Timestamp pastLimit, Timestamp futureLimit, UnitSystem unitSystem);

    List<RecentValue> findMostRecentsInRange(List<String> tsIds, Timestamp pastLimit,
                                             Timestamp futureLimit, UnitSystem unitSystem);

}
