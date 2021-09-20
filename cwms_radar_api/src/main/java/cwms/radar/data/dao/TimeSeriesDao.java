package cwms.radar.data.dao;

import java.sql.Timestamp;
import java.util.List;
import java.util.Optional;

import cwms.radar.data.dto.Catalog;
import cwms.radar.data.dto.RecentValue;
import cwms.radar.data.dto.TimeSeries;

public interface TimeSeriesDao
{
	Catalog getTimeSeriesCatalog(String cursor, int pageSize, Optional<String> office);

	void create(TimeSeries timeSeries);
	void store(TimeSeries timeSeries);

	void delete(String office, String tsId);

	TimeSeries getTimeseries(String cursor, int pageSize, String names, String office, String unit, String datum, String begin, String end, String timezone);
	String getTimeseries(String s, String names, String office, String unit, String datum, String begin, String end, String timezone);


	List<RecentValue> findRecentsInRange(String office, String categoryId, String groupId, Timestamp pastLimit, Timestamp futureLimit);
	List<RecentValue> findMostRecentsInRange(List<String> tsIds, Timestamp pastLimit, Timestamp futureLimit);
}
