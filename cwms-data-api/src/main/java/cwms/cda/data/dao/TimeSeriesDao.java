package cwms.cda.data.dao;

import cwms.cda.api.enums.UnitSystem;
import cwms.cda.data.dto.Catalog;
import cwms.cda.data.dto.RecentValue;
import cwms.cda.data.dto.TimeSeries;
import cwms.cda.data.dto.filteredtimeseries.FilteredTimeSeries;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;

public interface TimeSeriesDao {

    Catalog getTimeSeriesCatalog(String page, int pageSize, CatalogRequestParameters inputParams);

    void create(TimeSeries input);

    void create(TimeSeries input,
                boolean createAsLrts, StoreRule replaceAll, boolean overrideProtection, VerticalDatum vd);

    void store(TimeSeries timeSeries, Timestamp versionDate);

    void store(TimeSeries timeSeries, boolean createAsLrts,
               StoreRule replaceAll, boolean overrideProtection, VerticalDatum vd);

    void delete(String officeId, String tsId, TimeSeriesDeleteOptions options);

    /**
     *
     * @param cursor
     * @param pageSize
     * @param names
     * @param office
     * @param unit
     * @param begin
     * @param end
     * @param versionDate
     * @param trim
     * @param includeEntryDate
     * @return requested TimeSeries
     * @deprecated Use {@link #getTimeseries(String, int, TimeSeriesRequestParameters)}
     *             instead.  Create a {@link TimeSeriesRequestParameters} instance and
     *             call that overload.
     */
    @Deprecated
    TimeSeries getTimeseries(String cursor, int pageSize, String names, String office,
                             String unit, ZonedDateTime begin, ZonedDateTime end,
                             ZonedDateTime versionDate, boolean trim, boolean includeEntryDate);

    TimeSeries getTimeseries(String cursor, int pageSize, TimeSeriesRequestParameters requestParameters);
    FilteredTimeSeries getTimeseries(String page, int pageSize, TimeSeriesRequestParameters requestParameters, FilteredTimeSeriesParameters filterParams);

    String getTimeseries(String format, String names, String office, String unit, String datum,
                         ZonedDateTime begin, ZonedDateTime end, ZoneId timezone);

    List<RecentValue> findRecentsInRange(String office, String categoryId, String groupId,
                                         Timestamp pastLimit, Timestamp futureLimit, UnitSystem unitSystem);

    List<RecentValue> findMostRecentsInRange(String office, List<String> tsIds, Timestamp pastLimit,
                                             Timestamp futureLimit, UnitSystem unitSystem);
    /**
     * Streams the requested TimeSeries data in CSV format using a {@link StreamConsumer},
     * similar to the BlobDao streaming pattern. The DAO will invoke the provided consumer
     * with an InputStream that produces the CSV bytes on-the-fly, avoiding buffering the
     * entire dataset in memory.
     *
     * Notes:
     * - The consumer is responsible for copying the InputStream to the HTTP response and
     *   setting any additional headers as needed.
     * - The mediaType passed to the consumer will be {@code Formats.CSV}.
     * - The totalLength may be unknown and can be negative; callers should not rely on it.
     *
     * @param requestParameters request parameters describing the time-series retrieval
     * @param pageSize page size to use when paging results internally; non-positive values disable limiting
     * @param consumer a sink that will receive a streaming InputStream of CSV data
     */
    /**
     * Stream time-series in CSV using CsvV1 formatter.
     *
     * @param requestParameters parameters describing the request
     * @param consumer sink for the InputStream
     * @param includeMetadataAsColumns when true, include metadata as CSV columns
     * @param includeMetadataAsComments when true, include metadata as header comments
     */
    void streamRequestedTimeSeriesCsv(TimeSeriesRequestParameters requestParameters,
                                      StreamConsumer consumer,
                                      boolean includeMetadataAsColumns,
                                      boolean includeMetadataAsComments);

}
