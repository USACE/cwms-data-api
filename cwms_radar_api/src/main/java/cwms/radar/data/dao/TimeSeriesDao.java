package cwms.radar.data.dao;

import java.sql.Timestamp;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Optional;

import cwms.radar.data.dto.Catalog;
import cwms.radar.data.dto.RecentValue;
import cwms.radar.data.dto.TimeSeries;

public interface TimeSeriesDao
{
	Timestamp NON_VERSIONED = null;
	Catalog getTimeSeriesCatalog(String cursor, int pageSize, Optional<String> office);
	Catalog getTimeSeriesCatalog(String cursor, int pageSize, Optional<String> office, String idLike, String locCategoryLike, String locGroupLike, String tsCategoryLike, String tsGroupLike);

	void create(TimeSeries timeSeries);
	void store(TimeSeries timeSeries, Timestamp versionDate);

	void delete(String office, String tsId);

	TimeSeries getTimeseries(String cursor, int pageSize, String names, String office, String unit, String datum, ZonedDateTime begin, ZonedDateTime end, ZoneId timezone);
	String getTimeseries(String format, String names, String office, String unit, String datum, ZonedDateTime begin, ZonedDateTime end, ZoneId timezone);



	List<RecentValue> findRecentsInRange(String office, String categoryId, String groupId, Timestamp pastLimit, Timestamp futureLimit);
	List<RecentValue> findMostRecentsInRange(List<String> tsIds, Timestamp pastLimit, Timestamp futureLimit);

}
