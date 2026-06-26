package cwms.cda.data.dao;

import static com.google.common.flogger.LazyArgs.lazy;

import cwms.cda.api.errors.InvalidItemException;
import cwms.cda.data.dao.rsql.FieldResolver;
import cwms.cda.data.dao.rsql.MapFieldResolver;
import cwms.cda.data.dao.rsql.RSQLConditionBuilder;
import cwms.cda.data.dto.filteredtimeseries.FilteredTimeSeries;
import cwms.cda.data.dto.catalog.TimeSeriesAlias;
import cwms.cda.formatters.csv.CsvConfiguration;
import cwms.cda.helpers.DateUtils;

import java.io.IOException;
import java.sql.Connection;

import static org.jooq.impl.DSL.asterisk;
import static org.jooq.impl.DSL.countDistinct;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.max;
import static org.jooq.impl.DSL.name;
import static org.jooq.impl.DSL.noCondition;
import static org.jooq.impl.DSL.partitionBy;
import static org.jooq.impl.DSL.select;
import static org.jooq.impl.DSL.selectDistinct;
import static org.jooq.impl.DSL.selectOne;
import static org.jooq.impl.DSL.table;
import static usace.cwms.db.jooq.codegen.tables.AV_CWMS_TS_ID2.AV_CWMS_TS_ID2;
import static usace.cwms.db.jooq.codegen.tables.AV_TS_EXTENTS_UTC.AV_TS_EXTENTS_UTC;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheStats;
import com.google.common.flogger.FluentLogger;
import cwms.cda.api.enums.UnitSystem;
import cwms.cda.api.enums.VersionType;
import cwms.cda.data.dto.Catalog;
import cwms.cda.data.dto.CwmsDTOPaginated;
import cwms.cda.data.dto.RecentValue;
import cwms.cda.data.dto.TimeSeries;
import cwms.cda.data.dto.TimeSeriesExtents;
import cwms.cda.data.dto.Tsv;
import cwms.cda.data.dto.TsvDqu;
import cwms.cda.data.dto.TsvId;
import cwms.cda.data.dto.VerticalDatumInfo;
import cwms.cda.data.dto.catalog.CatalogEntry;
import cwms.cda.data.dto.catalog.TimeseriesCatalogEntry;
import cwms.cda.formatters.xml.XMLv1;
import cwms.cda.helpers.ZoneIdHelper;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import mil.army.usace.hec.metadata.Interval;
import mil.army.usace.hec.metadata.IntervalFactory;
import mil.army.usace.hec.metadata.IntervalOffset;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jooq.*;
import org.jooq.Record;
import org.jooq.conf.ParamType;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import usace.cwms.db.jooq.codegen.packages.CWMS_LOC_PACKAGE;
import usace.cwms.db.jooq.codegen.packages.CWMS_TS_PACKAGE;
import usace.cwms.db.jooq.codegen.packages.CWMS_UTIL_PACKAGE;
import usace.cwms.db.jooq.codegen.tables.AV_CWMS_TS_ID;
import usace.cwms.db.jooq.codegen.tables.AV_LOC;
import usace.cwms.db.jooq.codegen.tables.AV_LOC_GRP_ASSGN;
import usace.cwms.db.jooq.codegen.tables.AV_TSV;
import usace.cwms.db.jooq.codegen.tables.AV_TSV_DQU;
import usace.cwms.db.jooq.codegen.tables.AV_TS_GRP_ASSGN;
import usace.cwms.db.jooq.codegen.udt.records.DATE_TABLE_TYPE;
import usace.cwms.db.jooq.codegen.udt.records.DATE_RANGE_T;
import usace.cwms.db.jooq.codegen.udt.records.ZTSV_ARRAY;
import usace.cwms.db.jooq.codegen.udt.records.ZTSV_TYPE;

import cwms.cda.formatters.csv.CsvV1;
import cwms.cda.formatters.Formats;

public class TimeSeriesDaoImpl extends JooqDao<TimeSeries> implements TimeSeriesDao {
    private static final FluentLogger logger = FluentLogger.forEnclosingClass();

    /**
     * String constants for accessing alias tables and columns in TimeSeriesRecent querying
     */
    private static final String DATE_TIME = "DATE_TIME";
    private static final String VERSION_DATE = "VERSION_DATE";
    private static final String DATA_ENTRY_DATE = "DATA_ENTRY_DATE";
    private static final String QUALITY_CODE = "QUALITY_CODE";
    private static final String START_DATE = "START_DATE";
    private static final String END_DATE = "END_DATE";
    private static final String TS_CODE = "TS_CODE";
    private static final String VALUE = "VALUE";
    private static final String CWMS_TS_ID = "CWMS_TS_ID";
    private static final String TS_ID = "TS_ID";
    private static final String AT_TS_EXTENTS = "AT_TS_EXTENTS";
    private static final String VALUE_AT_MAX_DATE = "value_at_max_date";
    private static final String CWMS_20 = "CWMS_20";
    private static final String UNIT_ID = "UNIT_ID";
    private static final DateTimeFormatter ORACLE_DATE_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");

    public static final boolean OVERRIDE_PROTECTION = true;
    public static final int TS_ID_MISSING_CODE = 20001;
    public static final String MAX_DATE_TIME = "max_date_time";
    public static final String DEFAULT_UNITS = "def_units";
    public static final String PROP_BASE = "cwms.cda.data.dao.ts";
    private static final long TOTAL_QUERY_TIMEOUT_SECONDS = 30L;
    private static final ExecutorService TOTAL_QUERY_EXECUTOR = Executors.newCachedThreadPool(
            r -> {
                Thread thread = new Thread(r, "timeseries-total-query");
                thread.setDaemon(true);
                return thread;
            }
    );

    public static final String VERSIONED_NAME = "isVersioned";
    private static final long UTC_OFFSET_IRREGULAR = -2147483648L;
    private static final long UTC_OFFSET_UNDEFINED = 2147483647L;
    private static final String UTC = "UTC";

    /** To be able to use a named inner table (otherwise JOOQ creates a random alias which messes
     * with the planner) we need to use fixed names to be able to reference the required columns.
    ) */
    private static final AV_TS_GRP_ASSGN tsGroupView = AV_TS_GRP_ASSGN.AV_TS_GRP_ASSGN;
    private static final AV_LOC_GRP_ASSGN locGroupView = AV_LOC_GRP_ASSGN.AV_LOC_GRP_ASSGN;

    /**
     * This two tables share common names and are used in the same query, this
     * requires an "column as name" place holder to allow distinction.
     */
    private static final Field<String> tsGroupField = tsGroupView.GROUP_ID;
    private static final Field<String> tsCategoryField = tsGroupView.CATEGORY_ID;

    private static final Field<String> locGroupField = locGroupView.GROUP_ID;
    private static final Field<String> locCategoryField = locGroupView.CATEGORY_ID;

    private static final Cache<List<String>, Boolean> isVersionedCache = CacheBuilder.newBuilder()
            .maximumSize(Integer.getInteger(PROP_BASE + "." + VERSIONED_NAME
                    + ".maxSize", 32000))
            .expireAfterWrite(Integer.getInteger(PROP_BASE + "." + VERSIONED_NAME
                            + ".expireAfterSeconds", 600), TimeUnit.SECONDS)
            .recordStats()
            .build();
    private static final FieldMapping AV_CWMS_TS_ID2_FIELD_MAP = new CwmsTsId2FieldMapping();
    private static final FieldMapping AV_CWMS_TS_ID_FIELD_MAP = new CwmsTsIdFieldMapping();

    @NotNull
    private final Timer getRequestedTimeSeriesTotalQueryTimer;
    @NotNull
    private final Meter getRequestedTimeSeriesTotalQueryMeter;
    @NotNull
    private final Meter getRequestedTimeSeriesTotalQueryTimeoutMeter;
    @NotNull
    private final Meter getRequestedTimeSeriesTotalQueryErrorMeter;
    @NotNull
    private final Histogram getRequestedTimeSeriesResultsReturnedHistogram;
    @NotNull
    private final Histogram getRequestedTimeSeriesRequestWindowMillisHistogram;
    @NotNull
    private final MetricRegistry metrics;

    public TimeSeriesDaoImpl(DSLContext dsl, @NotNull MetricRegistry metrics) {
        super(dsl);
        this.metrics = metrics;

        String className = this.getClass().getName();
        CacheStats stats = isVersionedCache.stats();
        String hrName = MetricRegistry.name(className, VERSIONED_NAME, "hit-rate");
        if (metrics.getGauges().get(hrName) == null) {
            MetricRegistry.MetricSupplier<? extends Gauge> hr = () -> (Gauge<Double>) stats::hitRate;
            metrics.gauge(hrName, hr);
        }
        String mrName = MetricRegistry.name(className, VERSIONED_NAME, "miss-rate");
        if (metrics.getGauges().get(mrName) == null) {
            MetricRegistry.MetricSupplier<? extends Gauge> mr = () -> (Gauge<Double>) stats::missRate;
            metrics.gauge(mrName, mr);
        }

        getRequestedTimeSeriesTotalQueryTimer = metrics.timer(MetricRegistry.name(className,
                "getRequestedTimeSeries", "totalQuery", "time"));
        getRequestedTimeSeriesTotalQueryMeter = metrics.meter(MetricRegistry.name(className,
                "getRequestedTimeSeries", "totalQuery", "count"));
        getRequestedTimeSeriesTotalQueryTimeoutMeter = metrics.meter(MetricRegistry.name(className,
                "getRequestedTimeSeries", "totalQuery", "timeout"));
        getRequestedTimeSeriesTotalQueryErrorMeter = metrics.meter(MetricRegistry.name(className,
                "getRequestedTimeSeries", "totalQuery", "error"));
        getRequestedTimeSeriesResultsReturnedHistogram = metrics.histogram(MetricRegistry.name(className,
                "getRequestedTimeSeries", "results", "returned"));
        getRequestedTimeSeriesRequestWindowMillisHistogram = metrics.histogram(MetricRegistry.name(className,
                "getRequestedTimeSeries", "request", "windowMillis"));

    }


    public String getTimeseries(String format, String names, String office, String units,
                                String datum,
                                ZonedDateTime begin, ZonedDateTime end, ZoneId timezone) {
        return CWMS_TS_PACKAGE.call_RETRIEVE_TIME_SERIES_F(dsl.configuration(),
                names, format, units, datum,
                begin.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME),
                end.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME),
                timezone.getId(), office);
    }

    private ResultQuery<Record4<Timestamp, Double, BigDecimal, Timestamp>> buildTsvDquQuery(
            long tsCode, String officeId, String units,
            TimeSeriesRequestParameters requestParameters,
            boolean includeEntryDate) {
        ZonedDateTime beginTime = requestParameters.getBeginTime();
        ZonedDateTime endTime = requestParameters.getEndTime();
        ZonedDateTime versionDate = requestParameters.getVersionDate();

        Timestamp beginTimestamp = Timestamp.from(beginTime.toInstant());
        Timestamp endTimestamp = Timestamp.from(endTime.toInstant());
        String beginTimestampText = beginTimestamp.toLocalDateTime().format(ORACLE_DATE_FORMATTER);
        String endTimestampText = endTimestamp.toLocalDateTime().format(ORACLE_DATE_FORMATTER);

        AV_TSV_DQU view = AV_TSV_DQU.AV_TSV_DQU;
        Field<Timestamp> dateTimeField = field(name("CWMS_20", "AV_TSV_DQU", DATE_TIME), Timestamp.class);
        Field<Timestamp> versionDateField = field(name("CWMS_20", "AV_TSV_DQU", VERSION_DATE), Timestamp.class);
        Field<BigDecimal> qualityCode = view.QUALITY_CODE.cast(BigDecimal.class).as(QUALITY_CODE);
        Field<Double> value = view.VALUE.as(VALUE);

        Condition baseCondition = view.ALIASED_ITEM.isNull()
                .and(view.TS_CODE.eq(tsCode))
                .and(view.OFFICE_ID.eq(officeId))
                .and(view.UNIT_ID.equalIgnoreCase(units))
                .and(DSL.condition("{0} >= to_date({1}, 'yyyy-mm-dd\"T\"hh24:mi:ss')",
                        dateTimeField, DSL.val(beginTimestampText)))
                .and(DSL.condition("{0} <= to_date({1}, 'yyyy-mm-dd\"T\"hh24:mi:ss')",
                        dateTimeField, DSL.val(endTimestampText)))
                .and(view.START_DATE.isNull()
                        .or(DSL.condition("{0} <= to_date({1}, 'yyyy-mm-dd\"T\"hh24:mi:ss')",
                                view.START_DATE, DSL.val(endTimestampText))))
                .and(view.END_DATE.isNull()
                        .or(DSL.condition("{0} > to_date({1}, 'yyyy-mm-dd\"T\"hh24:mi:ss')",
                                view.END_DATE, DSL.val(beginTimestampText))));

        ResultQuery<Record4<Timestamp, Double, BigDecimal, Timestamp>> query;
        if (versionDate != null) {
            query = buildVersionedRowsQuery(
                    view,
                    dateTimeField,
                    versionDateField,
                    value,
                    qualityCode,
                    baseCondition,
                    versionDate,
                    includeEntryDate
            );
        } else {
            query = buildMaxVersionRowsQuery(
                    view,
                    dateTimeField,
                    versionDateField,
                    value,
                    qualityCode,
                    baseCondition,
                    includeEntryDate
            );
        }
        return query;
    }

    @Override
    public void streamRequestedTimeSeriesCsv(TimeSeriesRequestParameters requestParameters, StreamConsumer consumer,
                                             CsvConfiguration csvConfiguration, Integer dbFetchSize, Integer rowsPerBuffer) {

        boolean includeDataEntryDate = csvConfiguration.includeOptionalColumns();

        DirectReadMetadata metadata = fetchRequestedTimeSeriesMetadataRecord(requestParameters);
        if (metadata == null) {
            throw new DataAccessException("Unable to resolve time series metadata for " + requestParameters.getNames());
        }

        long tsCode = metadata.tsCode;
        String tsIdStr = metadata.tsId;
        String officeResolved = metadata.officeId;
        String resolvedUnits = metadata.units;

        ResultQuery<Record4<Timestamp, Double, BigDecimal, Timestamp>> query = buildTsvDquQuery(tsCode, officeResolved,
                resolvedUnits, requestParameters, includeDataEntryDate);

        logger.atFine().log("%s", lazy(query::getSQL));

        int effectiveFetchSize = (dbFetchSize != null && dbFetchSize > 0)
                ? dbFetchSize
                : 1000;

        if (rowsPerBuffer != null && rowsPerBuffer > 0 && effectiveFetchSize < rowsPerBuffer) {
            effectiveFetchSize = rowsPerBuffer;
        }

        query.fetchSize(effectiveFetchSize);

        ZonedDateTime versionDate = requestParameters.getVersionDate();
        Timestamp versionTs = versionDate != null ? Timestamp.from(versionDate.toInstant()) : null;

        try (Cursor<? extends Record4<Timestamp, Double, BigDecimal, Timestamp>> recCursor = query.fetchLazy()) {
            CsvV1 csvFormatter = new CsvV1();

            CsvOnDemandInputStream stream = new CsvOnDemandInputStream(
                    recCursor,
                    csvFormatter,
                    tsIdStr,
                    officeResolved,
                    resolvedUnits,
                    versionTs,
                    csvConfiguration,
                    rowsPerBuffer
            );

            try (stream) {
                consumer.accept(stream, Formats.CSV);
            } catch (IOException | SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Retrieves a TimeSeries from the database
     * @param page an opaque token used for paging
     * @param pageSize number of points to return in a page
     * @param names the timeseries id
     * @param office the office
     * @param units the units to return
     * @param beginTime the beginning of the time window
     * @param endTime the end of the time window
     * @param versionDate the requested version date or null
     * @param shouldTrim should the beginning and end of the returned timeseries be trimmed
     * @param includeEntryDate if the entry-date should be included in results
     * @return TimeSeries containing the requested data
     * @deprecated Use {@link #getTimeseries(String,int,TimeSeriesRequestParameters)}
     *             instead.  Create a {@link TimeSeriesRequestParameters} instance and
     *             call that overload.
     */
    @Override
    @Deprecated
    public TimeSeries getTimeseries(String page, int pageSize, String names, String office,
                                    String units,
                                    ZonedDateTime beginTime, ZonedDateTime endTime,
                                    ZonedDateTime versionDate, boolean shouldTrim, boolean includeEntryDate) {
        TimeSeriesRequestParameters requestParameters = new TimeSeriesRequestParameters.Builder()
                .withNames(names)
                .withOffice(office)
                .withUnits(units)
                .withBeginTime(beginTime)
                .withEndTime(endTime)
                .withVersionDate(versionDate)
                .withShouldTrim(shouldTrim)
                .withIncludeEntryDate(includeEntryDate)
                .build();
        return getTimeseries(page, pageSize, requestParameters);
    }

    @Override
    public TimeSeries getTimeseries(String page, int pageSize, TimeSeriesRequestParameters requestParameters) {
        return getRequestedTimeSeries(page, pageSize,requestParameters, null);
    }

    @Override
    public FilteredTimeSeries getTimeseries(String page, int pageSize, TimeSeriesRequestParameters requestParameters, FilteredTimeSeriesParameters filterParams) {
        TimeSeries ts =  getRequestedTimeSeries(page, pageSize, requestParameters, filterParams);
        FilteredTimeSeries fts = new FilteredTimeSeries(ts, filterParams);
        fts.clearTimeSeriesPagination();  // we are wrapping the ts, it doesn't need to serialize its own page, nextPage etc.
        return fts;
    }

    protected TimeSeries getRequestedTimeSeries(String page, int pageSize,
                                                @NotNull TimeSeriesRequestParameters requestParameters,
                                                @Nullable FilteredTimeSeriesParameters fp) {
        if (fp != null) {
            return getRequestedTimeSeriesLegacy(page, pageSize, requestParameters, fp);
        }
        return getRequestedTimeSeriesDirect(page, pageSize, requestParameters);
    }

    protected TimeSeries getRequestedTimeSeriesLegacy(String page, int pageSize,
                                                      @NotNull TimeSeriesRequestParameters requestParameters,
                                                      @Nullable FilteredTimeSeriesParameters fp) {

        String names = requestParameters.getNames();
        String office = requestParameters.getOffice();
        String units = requestParameters.getUnits();
        ZonedDateTime beginTime = requestParameters.getBeginTime();
        ZonedDateTime endTime = requestParameters.getEndTime();
        ZonedDateTime versionDate = requestParameters.getVersionDate();
        boolean shouldTrim = requestParameters.isShouldTrim();
        boolean includeEntryDate = requestParameters.isIncludeEntryDate();
        String cursor = null;
        Timestamp tsCursor = null;
        Integer total = null;

        validateEntryDateSupport(includeEntryDate);

        if (page != null && !page.isEmpty()) {
            final String[] parts = CwmsDTOPaginated.decodeCursor(page);

            logger.atFine().log("Decoded cursor");
            logger.atFinest().log("%s", lazy(() -> {
                StringBuilder sb = new StringBuilder();
                for (String p : parts) {
                    sb.append(p).append("\n");
                }
                return sb.toString();
            }));

            if (parts.length > 1) {
                cursor = parts[0];
                tsCursor = Timestamp.from(Instant.ofEpochMilli(Long.parseLong(parts[0])));

                if (parts.length > 2) {
                    total = Integer.parseInt(parts[1]);
                }

                // Use the pageSize from the original cursor, for consistent paging
                pageSize = Integer.parseInt(parts[parts.length - 1]);   // Last item is pageSize
            }
        }

        final String recordCursor = cursor;
        final int recordPageSize = pageSize;

        // Call some stored_procs to validate the user input and get the ts_code and tsid for the provided name.
        final Field<String> officeId = CWMS_UTIL_PACKAGE.call_GET_DB_OFFICE_ID(
                office != null ? DSL.val(office) : CWMS_UTIL_PACKAGE.call_USER_OFFICE_ID());
        final Field<String> tsId = CWMS_TS_PACKAGE.call_GET_TS_ID__2(DSL.val(names), officeId);
        final Field<BigDecimal> tsCode = CWMS_TS_PACKAGE.call_GET_TS_CODE__2(DSL.val(names), officeId);

        Table<Record3<BigDecimal, String, String>> validTs =
                select(tsCode.as("tscode"),
                        tsId.as("tsid"),
                        officeId.as("office_id")
                ).asTable("validts");
        // split the tsId into different parts and get the location and parameter parts
        Field<String> loc = CWMS_UTIL_PACKAGE.call_SPLIT_TEXT(
                validTs.field("tsid", String.class),
                DSL.val(BigInteger.valueOf(1L)), DSL.val("."),
                DSL.val(BigInteger.valueOf(6L)));
        Field<String> param = DSL.upper(CWMS_UTIL_PACKAGE.call_SPLIT_TEXT(
                validTs.field("tsid", String.class),
                DSL.val(BigInteger.valueOf(2L)), DSL.val("."),
                DSL.val(BigInteger.valueOf(6L))));

        // possibly call another procedure to get the units
        Field<String> unit = units.compareToIgnoreCase("SI") == 0
                ||
                units.compareToIgnoreCase("EN") == 0
                ?
                CWMS_UTIL_PACKAGE.call_GET_DEFAULT_UNITS(
                        CWMS_TS_PACKAGE.call_GET_BASE_PARAMETER_ID(tsCode),
                        DSL.val(units, String.class)
                )
                :
                DSL.val(units, String.class);

        // another call to get the interval
        Field<BigDecimal> ival = CWMS_TS_PACKAGE.call_GET_TS_INTERVAL__2(validTs.field("tsid", String.class));

        // put all those columns together as "valid"
        CommonTableExpression<Record7<BigDecimal, String, String, String, String, BigDecimal,
                String>> valid =
                name("valid").fields("tscode", "tsid", "office_id", "loc_part", "units",
                                "interval", "parm_part")
                        .as(
                                select(
                                        validTs.field("tscode", BigDecimal.class).as("tscode"),
                                        validTs.field("tsid", String.class).as("tsid"),
                                        validTs.field("office_id", String.class).as("office_id"),
                                        loc.as("loc_part"),
                                        unit.as("units"),
                                        ival.as("interval"),
                                        param.as("parm_part")
                                ).from(validTs)
                        );

        // Give the TVQ (time, value, quality) columns names
        Field<Timestamp> dateTimeCol = field(DATE_TIME, Timestamp.class).as(DATE_TIME);
        Field<Double> valueCol = field(VALUE, Double.class).as(VALUE);
        Field<Integer> qualityCol = field(QUALITY_CODE, Integer.class).as(QUALITY_CODE);
        Field<Timestamp> dataEntryDate = field(DATA_ENTRY_DATE, Timestamp.class).as("data_entry_date");

        Long beginTimeMilli = beginTime.toInstant().toEpochMilli();
        Long endTimeMilli = endTime.toInstant().toEpochMilli();

        getRequestedTimeSeriesRequestWindowMillisHistogram.update(Math.max(0L, endTimeMilli - beginTimeMilli));

        String trim = formatBool(shouldTrim);
        final String startInclusive = "T";
        final String endInclusive = "T";
        String previous = "F";
        String next = "F";
        Long versionDateMilli = null;
        String maxVersion = "F";

        if (versionDate != null) {
            versionDateMilli = versionDate.toInstant().toEpochMilli();
        } else {
            maxVersion = "T";
        }

        Field<String> tzName = AV_CWMS_TS_ID2.TIME_ZONE_ID;

        Condition filterConditions = noCondition();
        if (fp != null) {
            Map<String, Field<?>> nameToField = new LinkedHashMap<>();
            nameToField.put("value", valueCol);
            nameToField.put("date_time", dateTimeCol);
            nameToField.put("quality", qualityCol);
            nameToField.put("data_entry_date", dataEntryDate);
            FieldResolver resolver = new MapFieldResolver(nameToField);
            filterConditions = getFilterCondition(fp, resolver);
        }

        Future<Integer> totalQueryFuture = CompletableFuture.completedFuture(total);
        long totalQueryDeadlineNanos = Long.MAX_VALUE;
        if (total == null) {
            // If we don't know the total, fetch it from the database (only for first fetch).
            // Total is only an estimate, as it can change if fetching current data,
            // or the timeseries otherwise changes between queries.

            SelectConditionStep<Record3<Timestamp, Double, Integer>> retrieveSelectCount = select(
                    dateTimeCol, valueCol, qualityCol
            ).from(DSL.sql(
                    "table(cwms_20.cwms_ts.retrieve_ts_out_tab(?,?,"
                            + "cwms_20.cwms_util.to_timestamp(?),cwms_20.cwms_util.to_timestamp(?),"
                            + "'UTC',?,?,?,?,?," + getVersionPart(versionDate) + ",?,?) ) retrieveTsTotal",
                    tsId,
                    unit,
                    beginTimeMilli,
                    endTimeMilli,
                    trim, startInclusive, endInclusive, previous, next, versionDateMilli, maxVersion,
                    officeId
            ))
            .where(filterConditions);

            getRequestedTimeSeriesTotalQueryMeter.mark();
            totalQueryFuture = TOTAL_QUERY_EXECUTOR.submit(() -> {
                try (Timer.Context ignored = getRequestedTimeSeriesTotalQueryTimer.time()) {
                    return dsl.selectCount().from(DSL.table(retrieveSelectCount)).fetchOne(0, Integer.class);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            totalQueryDeadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(TOTAL_QUERY_TIMEOUT_SECONDS);
        }

        SelectJoinStep<?> metadataQuery =
                dsl.with(valid)
                        .select(
                                valid.field("tsid", String.class).as("NAME"),
                                valid.field("office_id", String.class).as("office_id"),
                                valid.field("units", String.class).as("units"),
                                valid.field("interval", BigDecimal.class).as("interval"),
                                valid.field("loc_part", String.class).as("loc_part"),
                                valid.field("parm_part", String.class).as("parm_part"),
                                AV_CWMS_TS_ID2.INTERVAL_UTC_OFFSET,
                                AV_CWMS_TS_ID2.TIME_ZONE_ID
                        )
                        .from(valid)
                        .leftOuterJoin(AV_CWMS_TS_ID2)
                        .on(
                                AV_CWMS_TS_ID2.DB_OFFICE_ID.eq(valid.field("office_id",
                                                String.class))
                                        .and(AV_CWMS_TS_ID2.TS_CODE.eq(valid.field("tscode",
                                                BigDecimal.class)))
                                        .and(AV_CWMS_TS_ID2.ALIASED_ITEM.isNull())
                        );

        logger.atFine().log("%s", lazy(() -> metadataQuery.getSQL(ParamType.INLINED)));


        Record tsMetadata = metadataQuery.fetchOne();

        if (pageSize == 0) {
            Integer resolvedTotal = resolveTotalQueryFuture(totalQueryFuture, totalQueryDeadlineNanos,
                    names, office, beginTime, endTime);
            return buildTimeSeriesFromMetadata(tsMetadata, resolvedTotal, names, office,
                    beginTime, endTime, units, versionDate, recordCursor, recordPageSize, tzName);
        }

        String retrievalMethod;
        if (includeEntryDate) {
            retrievalMethod = "cwms_20.cwms_ts.retrieve_ts_entry_out_tab";  // New method that supports entry date
        } else {
            retrievalMethod = "cwms_20.cwms_ts.retrieve_ts_out_tab";    // Legacy method without entry date
        }

        // Now we're going to call the retrieve_ts_entry_out_tab function to get the data and build an
        // internal table from it so we can manipulate it further
        // This code assumes the database timezone is in UTC (per Oracle recommendation)
        SQL retrieveSelectData = DSL.sql(
                "table(" + retrievalMethod + "(?,?,"
                        + "cwms_20.cwms_util.to_timestamp(?), cwms_20.cwms_util.to_timestamp(?), 'UTC',"
                        + "?,?,?,?,?,"
                        + getVersionPart(versionDate) + ",?,?) ) retrieveTs",
                tsId, unit,
                beginTimeMilli, endTimeMilli,  //tz hardcoded
                trim, startInclusive, endInclusive, previous, next,
                versionDateMilli, maxVersion, officeId);

        Field<BigDecimal> qualityNormCol = CWMS_TS_PACKAGE.call_NORMALIZE_QUALITY(
                DSL.nvl(qualityCol, DSL.inline(5))).as("QUALITY_NORM");

        TimeSeries retVal = null;
        if (pageSize != 0) {
            SelectConditionStep<Record4<Timestamp, Double, BigDecimal, Timestamp>> query2 = dsl.select(
                            dateTimeCol,
                            valueCol,
                            qualityNormCol,
                            dataEntryDate
                    )
                    .from(retrieveSelectData)
                    .where(filterConditions);

            SelectConditionStep<Record3<Timestamp, Double, BigDecimal>> query = dsl.select(
                            dateTimeCol,
                            valueCol,
                            qualityNormCol
                    )
                    .from(retrieveSelectData)
                    .where(dateTimeCol
                                    .greaterOrEqual(CWMS_UTIL_PACKAGE.call_TO_TIMESTAMP__2(
                                            DSL.nvl(DSL.val(tsCursor == null ? null :
                                                            tsCursor.toInstant().toEpochMilli()),
                                                    DSL.val(beginTime.toInstant().toEpochMilli())))))
                            .and(dateTimeCol
                                    .lessOrEqual(CWMS_UTIL_PACKAGE.call_TO_TIMESTAMP__2(
                                            DSL.val(endTime.toInstant().toEpochMilli())))
                            )
                            .and(filterConditions);

            if (pageSize > 0) {
                query.limit(DSL.val(pageSize + 1));
                if (includeEntryDate) {
                    query2.limit(DSL.val(pageSize + 1));
                }
            }

            // Retrieve all the points.
            List<TimeSeries.Record> retrievedPoints = new ArrayList<>();
            if (requestParameters.isIncludeEntryDate()) {
                logger.atFine().log("%s", lazy(() -> query2.getSQL(ParamType.INLINED)));
                try (Cursor<Record4<Timestamp, Double, BigDecimal, Timestamp>> recCursor = query2.fetchLazy()) {
                    for (Record tsRecord: recCursor) {
                        retrievedPoints.add(new TimeSeries.Record(
                                tsRecord.getValue(dateTimeCol),
                                tsRecord.getValue(valueCol),
                                tsRecord.getValue(qualityNormCol).intValue(),
                                tsRecord.getValue(dataEntryDate)));
                    }
                }
            } else {
                logger.atFine().log("%s", lazy(() -> query.getSQL(ParamType.INLINED)));
                try (Cursor<Record3<Timestamp, Double, BigDecimal>> recCursor = query.fetchLazy()) {
                    for (Record tsRecord: recCursor) {
                        retrievedPoints.add(new TimeSeries.Record(
                                tsRecord.getValue(dateTimeCol),
                                tsRecord.getValue(valueCol),
                                tsRecord.getValue(qualityNormCol).intValue(),
                                null));
                    }
                }
            }

            // Wait to resolve the total future until right before we need it.
            // This allows the data retrieval query to run in parallel with the total count query (if pool>1),
            // and allows maximum time for the total query to complete before we time it out.
            final Integer resolvedTotal = resolveTotalQueryFuture(totalQueryFuture, totalQueryDeadlineNanos,
                    names, office, beginTime, endTime);
            TimeSeries timeseries = buildTimeSeriesFromMetadata(tsMetadata, resolvedTotal, names, office,
                    beginTime, endTime, units, versionDate, recordCursor, recordPageSize, tzName);

            for (TimeSeries.Record point : retrievedPoints) {
                timeseries.addValue(point);
            }

            retVal = timeseries;

            getRequestedTimeSeriesResultsReturnedHistogram.update(timeseries.getValues().size());
        }

        if (retVal != null) {
            retVal.alignWindowToReturnedValues(shouldTrim);
        }

        return retVal;
    }

    @Nullable
    private Integer resolveTotalQueryFuture(Future<Integer> totalQueryFuture, long totalQueryDeadlineNanos,
                                            String names, String office,
                                            ZonedDateTime beginTime, ZonedDateTime endTime) {
        try {
            if (totalQueryDeadlineNanos == Long.MAX_VALUE) {
                return totalQueryFuture.get();
            }

            if (totalQueryFuture.isDone()) {
                return totalQueryFuture.get();
            }

            long remainingNanos = totalQueryDeadlineNanos - System.nanoTime();
            if (remainingNanos <= 0) {
                throw new TimeoutException("Total query deadline elapsed before resolution");
            }

            return totalQueryFuture.get(remainingNanos, TimeUnit.NANOSECONDS);
        } catch (TimeoutException e) {
            totalQueryFuture.cancel(true);
            getRequestedTimeSeriesTotalQueryTimeoutMeter.mark();
            logger.atWarning().withCause(e).log("Timed out retrieving total count for timeseries %s at office %s "
                            + "for window %s to %s after %d seconds; continuing with unknown total.",
                    names, office, beginTime, endTime, TOTAL_QUERY_TIMEOUT_SECONDS);
            return null;
        } catch (InterruptedException e) {
            totalQueryFuture.cancel(true);
            Thread.currentThread().interrupt();
            getRequestedTimeSeriesTotalQueryErrorMeter.mark();
            logger.atWarning().withCause(e).log("Interrupted retrieving total count for timeseries %s at office %s "
                            + "for window %s to %s; continuing with unknown total.",
                    names, office, beginTime, endTime);
            return null;
        } catch (ExecutionException e) {
            getRequestedTimeSeriesTotalQueryErrorMeter.mark();
            logger.atWarning().withCause(e.getCause()).log("Failed retrieving total count for timeseries %s at office %s "
                            + "for window %s to %s; continuing with unknown total.",
                    names, office, beginTime, endTime);
            return null;
        }
    }

    @NotNull
    private TimeSeries buildTimeSeriesFromMetadata(Record tsMetadata, @Nullable Integer resolvedTotal,
                                                   String names, String office,
                                                   ZonedDateTime beginTime, ZonedDateTime endTime,
                                                   String units, ZonedDateTime versionDate,
                                                   String recordCursor, int recordPageSize,
                                                   Field<String> tzName) {
        if (tsMetadata == null) {
            throw new DataAccessException("No metadata returned for requested timeseries.");
        }

        String parmPart = tsMetadata.getValue("parm_part", String.class);
        String locPart = tsMetadata.getValue("loc_part", String.class);

        // Fetch vertical datum info separately only when needed
        VerticalDatumInfo verticalDatumInfo = null;
        if (shouldFetchVerticalDatum(parmPart)) {
            verticalDatumInfo = fetchVerticalDatumInfoSeparately(locPart, units, office);
        }

        VersionType finalDateVersionType = getVersionType(dsl, names, office, versionDate != null);
        return new TimeSeries(recordCursor, recordPageSize, resolvedTotal,
                tsMetadata.getValue("NAME", String.class),
                tsMetadata.getValue("office_id", String.class),
                beginTime, endTime, tsMetadata.getValue("units", String.class),
                Duration.ofMinutes(tsMetadata.get("interval") == null ? 0 :
                        tsMetadata.getValue("interval", Long.class)),
                verticalDatumInfo,
                tsMetadata.getValue(AV_CWMS_TS_ID2.INTERVAL_UTC_OFFSET).longValue(),
                tsMetadata.getValue(tzName),
                versionDate, finalDateVersionType
        );
    }

    private TimeSeries getRequestedTimeSeriesDirect(String page, int pageSize,
                                                    @NotNull TimeSeriesRequestParameters requestParameters) {
        String names = requestParameters.getNames();
        String office = requestParameters.getOffice();
        String requestedUnits = requestParameters.getUnits();
        ZonedDateTime beginTime = requestParameters.getBeginTime();
        ZonedDateTime endTime = requestParameters.getEndTime();
        ZonedDateTime versionDate = requestParameters.getVersionDate();
        boolean includeEntryDate = requestParameters.isIncludeEntryDate();
        String cursor = null;
        Timestamp tsCursor = null;

        if (page != null && !page.isEmpty()) {
            final String[] parts = CwmsDTOPaginated.decodeCursor(page);

            logger.atFine().log("Decoded cursor");
            logger.atFinest().log("%s", lazy(() -> {
                StringBuilder sb = new StringBuilder();
                for (String p : parts) {
                    sb.append(p).append("\n");
                }
                return sb.toString();
            }));

            if (parts.length > 1) {
                cursor = parts[0];
                tsCursor = Timestamp.from(Instant.ofEpochMilli(Long.parseLong(parts[0])));
                pageSize = Integer.parseInt(parts[parts.length - 1]);
            }
        }

        DirectReadMetadata metadata = fetchRequestedTimeSeriesMetadataRecord(requestParameters);
        if (metadata == null) {
            throw new DataAccessException("Unable to resolve time series metadata for " + names);
        }
        long tsCode = metadata.tsCode;
        String tsId = metadata.tsId;
        String[] tsIdParts = splitTimeSeriesId(tsId);
        String metadataOfficeId = metadata.officeId;
        String metadataUnits = metadata.units;
        String nativeUnits = metadata.nativeUnits;
        String locPart = getTimeSeriesIdPart(tsIdParts, 0);
        String parmPart = getTimeSeriesIdPart(tsIdParts, 1);
        String intervalPart = getTimeSeriesIdPart(tsIdParts, 3);
        long intervalMinutes = metadata.intervalMinutes;
        long intervalOffset = metadata.intervalUtcOffset;
        String timeZoneId = metadata.timeZoneId;
        boolean isLrts = parseBool(CWMS_TS_PACKAGE.call_IS_LRTS__2(dsl.configuration(), tsCode));

        VerticalDatumInfo verticalDatumInfo = null;
        if (shouldFetchVerticalDatum(parmPart)) {
            verticalDatumInfo = fetchVerticalDatumInfoSeparately(locPart, requestedUnits, office);
        }
        validateRequestedUnits(nativeUnits, metadataUnits);

        VersionType finalDateVersionType = getDirectReadVersionType(
                metadata.versionFlag, versionDate != null);

        // Pagination happens after regular-interval gap rows are merged
        //  fetch the full raw window first
        List<TimeSeries.Record> rawRows = fetchRequestedTimeSeriesRows(tsCode, metadataOfficeId,
                metadataUnits, requestParameters, includeEntryDate);
        long effectiveIntervalOffset = intervalOffset;
        if (isRegularSeries(intervalMinutes, intervalOffset, intervalPart, isLrts)) {
            effectiveIntervalOffset = resolveIntervalOffset(intervalOffset, timeZoneId, intervalPart, isLrts, rawRows);
        }

        List<Timestamp> expectedTimes = fetchExpectedRegularTimes(intervalMinutes, effectiveIntervalOffset, timeZoneId,
                intervalPart, isLrts, requestParameters, rawRows);
        int total = countMergedRows(rawRows, expectedTimes);

        TimeSeries timeseries = new TimeSeries(
                cursor,
                pageSize,
                total,
                tsId,
                metadataOfficeId,
                beginTime,
                endTime,
                metadataUnits,
                resolveIntervalDuration(intervalMinutes, intervalOffset, intervalPart, isLrts),
                verticalDatumInfo,
                effectiveIntervalOffset,
                timeZoneId,
                versionDate,
                finalDateVersionType
        );

        if (pageSize == 0) {
            return timeseries;
        }

        populateTimeSeriesValues(timeseries, rawRows, expectedTimes, tsCursor, includeEntryDate);
        return timeseries.alignWindowToReturnedValues(requestParameters.isShouldTrim());
    }

    private DirectReadMetadata fetchRequestedTimeSeriesMetadataRecord(
            TimeSeriesRequestParameters requestParameters) {
        return fetchRequestedTimeSeriesMetadataRecord(dsl, requestParameters);
    }

    private DirectReadMetadata fetchRequestedTimeSeriesMetadataRecord(DSLContext metadataDsl,
                                                                     TimeSeriesRequestParameters requestParameters) {
        String names = requestParameters.getNames();
        String office = requestParameters.getOffice();
        String units = requestParameters.getUnits();

        final Field<String> officeId = CWMS_UTIL_PACKAGE.call_GET_DB_OFFICE_ID(
                office != null ? DSL.val(office) : CWMS_UTIL_PACKAGE.call_USER_OFFICE_ID());
        final Field<String> tsId = CWMS_TS_PACKAGE.call_GET_TS_ID__2(DSL.val(names), officeId);
        final Field<BigDecimal> tsCode = CWMS_TS_PACKAGE.call_GET_TS_CODE__2(DSL.val(names), officeId);

        validateUnits(units, tsCode, officeId, names);

        Table<Record3<BigDecimal, String, String>> validTs =
                select(tsCode.as("tscode"),
                        tsId.as("tsid"),
                        officeId.as("office_id"))
                        .asTable("validts");

        Field<String> unit = units.compareToIgnoreCase("SI") == 0
                || units.compareToIgnoreCase("EN") == 0
                ? CWMS_UTIL_PACKAGE.call_GET_DEFAULT_UNITS(
                        CWMS_TS_PACKAGE.call_GET_BASE_PARAMETER_ID(tsCode),
                        DSL.val(units, String.class))
                : DSL.val(units, String.class);

        Field<BigDecimal> interval = CWMS_TS_PACKAGE.call_GET_TS_INTERVAL__2(validTs.field("tsid", String.class));

        CommonTableExpression<?> valid =
                name("valid").fields("tscode", "tsid", "office_id", "units", "interval")
                        .as(
                                select(
                                        validTs.field("tscode", BigDecimal.class).as("tscode"),
                                        validTs.field("tsid", String.class).as("tsid"),
                                        validTs.field("office_id", String.class).as("office_id"),
                                        unit.as("units"),
                                        interval.as("interval"))
                                        .from(validTs));

        var tsIdView = AV_CWMS_TS_ID.AV_CWMS_TS_ID;

        SelectJoinStep<?> metadataQuery =
                metadataDsl.with(valid)
                        .select(
                                valid.field("tscode", BigDecimal.class).as("tscode"),
                                valid.field("tsid", String.class).as("tsid"),
                                valid.field("office_id", String.class).as("office_id"),
                                valid.field("units", String.class).as("units"),
                                tsIdView.UNIT_ID.as("native_units"),
                                valid.field("interval", BigDecimal.class).as("interval"),
                                tsIdView.INTERVAL_UTC_OFFSET.as("interval_utc_offset"),
                                tsIdView.TIME_ZONE_ID.as("time_zone_id"),
                                tsIdView.field("VERSION_FLAG", String.class).as("version_flag"))
                        .from(valid)
                        .leftOuterJoin(tsIdView)
                        .on(tsIdView.DB_OFFICE_ID.eq(valid.field("office_id", String.class))
                                .and(tsIdView.TS_CODE.eq(valid.field("tscode", BigDecimal.class))));

        logger.atFine().log("%s", lazy(() -> metadataQuery.getSQL(ParamType.INLINED)));

        return metadataQuery.fetchOne(record -> new DirectReadMetadata(
                record.getValue("tscode", BigDecimal.class).longValue(),
                record.getValue("tsid", String.class),
                record.getValue("office_id", String.class),
                record.getValue("units", String.class),
                record.getValue("native_units", String.class),
                record.getValue("interval", BigDecimal.class) == null
                        ? 0L
                        : record.getValue("interval", BigDecimal.class).longValue(),
                record.getValue("interval_utc_offset", Number.class) == null
                        ? UTC_OFFSET_IRREGULAR
                        : record.getValue("interval_utc_offset", Number.class).longValue(),
                record.getValue("time_zone_id", String.class) == null
                        ? UTC
                        : record.getValue("time_zone_id", String.class),
                record.getValue("version_flag", String.class)));
    }

    private void validateUnits(String units, Field<BigDecimal> tsCode, Field<String> officeId, String name) {
        if(!units.equalsIgnoreCase("SI") && !units.equalsIgnoreCase("EN")) {

            boolean tsExists = dsl.fetchExists(
                    selectOne()
                            .from(AV_TSV_DQU.AV_TSV_DQU)
                            .where(AV_TSV_DQU.AV_TSV_DQU.TS_CODE.eq(tsCode.cast(Long.class)))
                            .and(AV_TSV_DQU.AV_TSV_DQU.OFFICE_ID.eq(officeId))
            );
            if(tsExists) {
                boolean unitRequestedExists = dsl.fetchExists(
                        selectOne()
                                .from(AV_TSV_DQU.AV_TSV_DQU)
                                .where(AV_TSV_DQU.AV_TSV_DQU.TS_CODE.eq(tsCode.cast(Long.class)))
                                .and(AV_TSV_DQU.AV_TSV_DQU.OFFICE_ID.eq(officeId))
                                .and(AV_TSV_DQU.AV_TSV_DQU.UNIT_ID.equalIgnoreCase(units))
                );

                if (!unitRequestedExists) {
                    String msg = sanitizeOrNull(units + " is not a valid unit for time series " + name);
                    throw new InvalidItemException(msg, new IllegalArgumentException(msg));
                }
            }
        }
    }

    private List<TimeSeries.Record> fetchRequestedTimeSeriesRows(long tsCode, String officeId,
                                                                 String requestedUnits,
                                                                 TimeSeriesRequestParameters requestParameters,
                                                                 boolean includeEntryDate) {
        ResultQuery<Record4<Timestamp, Double, BigDecimal, Timestamp>> query = buildTsvDquQuery(tsCode, officeId,
                requestedUnits, requestParameters, includeEntryDate);

        logger.atFine().log("%s", lazy(() -> query.getSQL(ParamType.INLINED)));

        return query.fetch(record -> {
            Timestamp dateTime = record.getValue(0, Timestamp.class);
            Double dataValue = record.getValue(1, Double.class);
            int quality = normalizeQualityCode(record.getValue(2, BigDecimal.class));
            Timestamp dataEntryDate = record.getValue(3, Timestamp.class);
            if (dataEntryDate != null) {
                return new TimeSeries.Record(dateTime, dataValue, quality, dataEntryDate);
            }
            return new TimeSeries.Record(dateTime, dataValue, quality);
        });
    }

    private static int normalizeQualityCode(BigDecimal qualityCode) {
        long quality = qualityCode == null ? 5L : qualityCode.longValue();
        if (quality > Integer.MAX_VALUE) {
            quality -= 4_294_967_296L;
        }
        return (int) quality;
    }

    private ResultQuery<Record4<Timestamp, Double, BigDecimal, Timestamp>> buildVersionedRowsQuery(
            AV_TSV_DQU view,
            Field<Timestamp> dateTime,
            Field<Timestamp> versionDateField,
            Field<Double> value,
            Field<BigDecimal> qualityCode,
            Condition baseCondition,
            ZonedDateTime versionDate,
            boolean includeEntryDate) {
        Field<Timestamp> versionTimestamp = CWMS_UTIL_PACKAGE.call_TO_TIMESTAMP__2(
                DSL.val(versionDate.toInstant().toEpochMilli()));
        String versionTimestampText = Timestamp.from(versionDate.toInstant()).toLocalDateTime()
                .format(ORACLE_DATE_FORMATTER);
        Condition versionDateCondition = versionDateField.eq(versionTimestamp)
                .or(DSL.condition("{0} = to_date({1}, 'yyyy-mm-dd\"T\"hh24:mi:ss')",
                        versionDateField, DSL.val(versionTimestampText)));
        Field<Timestamp> dataEntryDateField = includeEntryDate
                ? view.DATA_ENTRY_DATE
                : DSL.castNull(Timestamp.class).as(DATA_ENTRY_DATE);

        return dsl.select(
                        dateTime,
                        value,
                        qualityCode,
                        dataEntryDateField)
                .from(view)
                .where(baseCondition.and(versionDateCondition))
                .orderBy(dateTime.asc());
    }

    private ResultQuery<Record4<Timestamp, Double, BigDecimal, Timestamp>> buildMaxVersionRowsQuery(
            AV_TSV_DQU view,
            Field<Timestamp> dateTime,
            Field<Timestamp> versionDateField,
            Field<Double> value,
            Field<BigDecimal> qualityCode,
            Condition baseCondition,
            boolean includeEntryDate) {
        var rankedRows = dsl.select(
                        dateTime.as(DATE_TIME),
                        value,
                        qualityCode,
                        includeEntryDate
                                ? view.DATA_ENTRY_DATE.as(DATA_ENTRY_DATE)
                                : DSL.castNull(Timestamp.class).as(DATA_ENTRY_DATE),
                        DSL.rowNumber()
                                .over(partitionBy(dateTime)
                                        .orderBy(versionDateField.desc(), view.DATA_ENTRY_DATE.desc()))
                                .as("version_rank"))
                .from(view)
                .where(baseCondition)
                .asTable("ranked_rows");

        Field<Timestamp> dateTimeCol = rankedRows.field(DATE_TIME, Timestamp.class);
        Field<Double> valueCol = rankedRows.field(VALUE, Double.class);
        Field<BigDecimal> qualityCol = rankedRows.field(QUALITY_CODE, BigDecimal.class);
        Field<Timestamp> dataEntryDateCol = rankedRows.field(DATA_ENTRY_DATE, Timestamp.class);
        Field<Integer> versionRankCol = rankedRows.field("version_rank", Integer.class);

        return dsl.select(dateTimeCol, valueCol, qualityCol, dataEntryDateCol)
                .from(rankedRows)
                .where(versionRankCol.eq(1))
                .orderBy(dateTimeCol.asc());
    }

    private void validateRequestedUnits(String nativeUnits, String requestedUnits) {
        if (nativeUnits != null && requestedUnits != null) {
            CWMS_UTIL_PACKAGE.call_CONVERT_UNITS(dsl.configuration(), 0.0D, nativeUnits, requestedUnits);
        }
    }

    private List<Timestamp> fetchExpectedRegularTimes(long intervalMinutes, long intervalOffset, String timeZoneId,
                                                      String intervalPart, boolean isLrts,
                                                      TimeSeriesRequestParameters requestParameters,
                                                      List<TimeSeries.Record> rawRows) {
        boolean shouldTrim = requestParameters.isShouldTrim();
        if (!isRegularSeries(intervalMinutes, intervalOffset, intervalPart, isLrts)) {
            return Collections.emptyList();
        }
        // Trimmed requests collapse to the observed data window
        //  there is nothing to expand if no rows matched
        if (rawRows.isEmpty() && shouldTrim) {
            return Collections.emptyList();
        }

        Timestamp rangeStart = shouldTrim
                ? rawRows.get(0).getDateTime()
                : Timestamp.from(requestParameters.getBeginTime().toInstant());
        Timestamp rangeEnd = shouldTrim
                ? rawRows.get(rawRows.size() - 1).getDateTime()
                : Timestamp.from(requestParameters.getEndTime().toInstant());

        Interval expectedInterval = resolveExpectedInterval(intervalPart);
        if (expectedInterval != null) {
            return buildExpectedRegularTimes(rangeStart, rangeEnd, intervalOffset, expectedInterval,
                    getExpectedTimeZone(timeZoneId, isLrts));
        }

        String intervalTimeZone = isLrts ? timeZoneId : UTC;
        DATE_RANGE_T dateRange = new DATE_RANGE_T(rangeStart, rangeEnd, UTC, "T", "T", null);
        DATE_TABLE_TYPE expectedTimeTable = CWMS_TS_PACKAGE.call_GET_REG_TS_TIMES_UTC_F(
                dsl.configuration(),
                dateRange,
                intervalPart,
                String.valueOf(intervalOffset),
                intervalTimeZone
        );

        List<Timestamp> retVal = new ArrayList<>();
        if (expectedTimeTable != null) {
            expectedTimeTable.forEach(timestamp -> {
                if (timestamp != null) {
                    retVal.add(normalizeOracleUtcTimestamp(timestamp));
                }
            });
        }
        return retVal;
    }

    private long resolveIntervalOffset(long intervalOffset, String timeZoneId,
                                       String intervalPart, boolean isLrts, List<TimeSeries.Record> rawRows) {
        if (intervalOffset != UTC_OFFSET_UNDEFINED && intervalOffset != UTC_OFFSET_IRREGULAR) {
            return intervalOffset;
        }
        if (rawRows.isEmpty()) {
            return 0L;
        }

        Interval expectedInterval = resolveExpectedInterval(intervalPart);
        if (expectedInterval != null) {
            try {
                Instant firstTime = rawRows.get(0).getDateTime().toInstant();
                Instant topOfInterval = expectedInterval.getTimeOnPreviousOrCurrentInterval(
                        firstTime,
                        IntervalOffset.zeroOffset(),
                        getExpectedTimeZone(timeZoneId, isLrts)
                );
                return TimeUnit.MILLISECONDS.toMinutes(firstTime.toEpochMilli() - topOfInterval.toEpochMilli());
            } catch (mil.army.usace.hec.metadata.DataSetIllegalArgumentException ex) {
                throw new IllegalArgumentException("Unable to resolve interval offset for " + intervalPart, ex);
            }
        }

        String intervalTimeZone = isLrts ? timeZoneId : UTC;
        Timestamp topOfInterval = normalizeOracleUtcTimestamp(CWMS_TS_PACKAGE.call_TOP_OF_INTERVAL_UTC(
                dsl.configuration(),
                rawRows.get(0).getDateTime(),
                intervalPart,
                intervalTimeZone,
                "F"
        ));
        return (rawRows.get(0).getDateTime().getTime() - topOfInterval.getTime()) / TimeUnit.MINUTES.toMillis(1);
    }

    private boolean isRegularSeries(long intervalMinutes, long intervalOffset, String intervalPart, boolean isLrts) {
        return intervalOffset != UTC_OFFSET_IRREGULAR
                && (intervalMinutes != 0L || (isLrts && isLocalRegularInterval(intervalPart)));
    }

    private Duration resolveIntervalDuration(long intervalMinutes, long intervalOffset,
                                             String intervalPart, boolean isLrts) {
        if (!isRegularSeries(intervalMinutes, intervalOffset, intervalPart, isLrts)) {
            return Duration.ZERO;
        }

        if (intervalMinutes != 0L) {
            return Duration.ofMinutes(intervalMinutes);
        }

        Interval interval = resolveExpectedInterval(intervalPart);
        if (interval != null) {
            return Duration.ofSeconds(interval.getSeconds());
        }

        return Duration.ZERO;
    }

    private int countMergedRows(List<TimeSeries.Record> rawRows, List<Timestamp> expectedTimes) {
        if (expectedTimes.isEmpty()) {
            return rawRows.size();
        }

        int total = 0;
        int rawIndex = 0;
        int expectedIndex = 0;
        while (rawIndex < rawRows.size() || expectedIndex < expectedTimes.size()) {
            Timestamp rawTime = rawIndex < rawRows.size() ? rawRows.get(rawIndex).getDateTime() : null;
            Timestamp expectedTime = expectedIndex < expectedTimes.size() ? expectedTimes.get(expectedIndex) : null;

            if (rawTime == null) {
                expectedIndex++;
            } else if (expectedTime == null) {
                rawIndex++;
            } else {
                int compare = compareTimestampOrder(expectedTime, rawTime);
                if (compare < 0) {
                    expectedIndex++;
                } else if (compare > 0) {
                    rawIndex++;
                } else {
                    expectedIndex++;
                    rawIndex++;
                }
            }
            total++;
        }
        return total;
    }

    private void populateTimeSeriesValues(TimeSeries timeseries,
                                          List<TimeSeries.Record> rawRows,
                                          List<Timestamp> expectedTimes,
                                          Timestamp tsCursor,
                                          boolean includeEntryDate) {
        int rawIndex = 0;
        int expectedIndex = 0;
        int collected = 0;
        int maxRecords = timeseries.getPageSize() > 0 ? timeseries.getPageSize() + 1 : Integer.MAX_VALUE;

        while ((rawIndex < rawRows.size() || expectedIndex < expectedTimes.size()) && collected < maxRecords) {
            TimeSeries.Record rawRow = rawIndex < rawRows.size() ? rawRows.get(rawIndex) : null;
            Timestamp expectedTime = expectedIndex < expectedTimes.size() ? expectedTimes.get(expectedIndex) : null;

            Timestamp candidateTime;
            TimeSeries.Record candidateRow = null;
            boolean syntheticRow = false;

            if (rawRow == null) {
                candidateTime = expectedTime;
                syntheticRow = true;
                expectedIndex++;
            } else if (expectedTime == null) {
                candidateTime = rawRow.getDateTime();
                candidateRow = rawRow;
                rawIndex++;
            } else {
                int compare = compareTimestampOrder(expectedTime, rawRow.getDateTime());
                if (compare < 0) {
                    candidateTime = expectedTime;
                    syntheticRow = true;
                    expectedIndex++;
                } else if (compare > 0) {
                    candidateTime = rawRow.getDateTime();
                    candidateRow = rawRow;
                    rawIndex++;
                } else {
                    candidateTime = rawRow.getDateTime();
                    candidateRow = rawRow;
                    rawIndex++;
                    expectedIndex++;
                }
            }

            if (tsCursor != null && compareTimestampOrder(candidateTime, tsCursor) < 0) {
                continue;
            }

            if (syntheticRow) {
                if (includeEntryDate) {
                    timeseries.addValue(candidateTime, null, 0, null);
                } else {
                    timeseries.addValue(candidateTime, null, 0);
                }
            } else if (includeEntryDate) {
                timeseries.addValue(candidateRow.getDateTime(), candidateRow.getValue(),
                        candidateRow.getQualityCode(), candidateRow.getDataEntryDate());
            } else {
                timeseries.addValue(candidateRow.getDateTime(), candidateRow.getValue(),
                        candidateRow.getQualityCode());
            }
            collected++;
        }
    }

    private int compareTimestampOrder(Timestamp left, Timestamp right) {
        return Long.compare(left.getTime(), right.getTime());
    }

    private Timestamp normalizeOracleUtcTimestamp(Timestamp timestamp) {
        LocalDateTime utcWallTime = timestamp.toLocalDateTime();
        return Timestamp.from(utcWallTime.toInstant(ZoneOffset.UTC));
    }

    @Nullable
    private Interval resolveExpectedInterval(String intervalPart) {
        if (intervalPart == null) {
            return null;
        }

        return IntervalFactory.findAny(IntervalFactory.equalsName(normalizeIntervalNameForNucleus(intervalPart)))
                .orElse(null);
    }

    private List<Timestamp> buildExpectedRegularTimes(Timestamp rangeStart,
                                                      Timestamp rangeEnd,
                                                      long offsetMinutes,
                                                      Interval interval,
                                                      ZoneId intervalTimeZone) {
        List<Timestamp> expectedTimes = new ArrayList<>();
        IntervalOffset intervalOffset = IntervalOffset.fromSeconds(Math.toIntExact(
                TimeUnit.MINUTES.toSeconds(offsetMinutes)));
        Instant endTime = rangeEnd.toInstant();

        try {
            Instant nextTime = interval.getTimeOnNextOrCurrentInterval(rangeStart.toInstant(), intervalOffset,
                    intervalTimeZone);
            while (!nextTime.isAfter(endTime)) {
                expectedTimes.add(Timestamp.from(nextTime));
                nextTime = interval.getNextIntervalTime(nextTime, intervalTimeZone);
            }
        } catch (mil.army.usace.hec.metadata.DataSetIllegalArgumentException ex) {
            throw new IllegalArgumentException("Unable to build expected times for " + interval.getInterval(), ex);
        }
        return expectedTimes;
    }

    private ZoneId getExpectedTimeZone(String timeZoneId, boolean isLrts) {
        if (!isLrts) {
            return ZoneOffset.UTC;
        }
        return ZoneIdHelper.parseZoneIdWithAliases(timeZoneId);
    }

    private String normalizeIntervalNameForNucleus(String intervalPart) {
        if (intervalPart.startsWith("~")) {
            return intervalPart;
        }
        if (intervalPart.length() > 5
                && intervalPart.regionMatches(true, intervalPart.length() - 5, "Local", 0, 5)) {
            return "~" + intervalPart.substring(0, intervalPart.length() - 5);
        }
        return intervalPart;
    }

    private boolean isLocalRegularInterval(String intervalPart) {
        if (intervalPart == null) {
            return false;
        }
        return normalizeIntervalNameForNucleus(intervalPart).startsWith("~");
    }

    private boolean shouldFetchVerticalDatum(String parmPart) {
        // Check if parameter requires vertical datum (e.g., "ELEV")
        if (parmPart == null) {
            return false;
        }
        String upperParm = parmPart.toUpperCase();
        return upperParm.equals("ELEV");
    }

    private VerticalDatumInfo fetchVerticalDatumInfoSeparately(String locPart, String units, String office) {

        return connectionResult(dsl, conn -> {
            String datumUnits = units;
            if ("SI".equalsIgnoreCase(datumUnits)) {
                datumUnits = "m";
            } else if ("EN".equalsIgnoreCase(datumUnits)) {
                datumUnits = "ft";
            }
            DSLContext dslContext = getDslContext(conn, office);
            String result = CWMS_LOC_PACKAGE.call_GET_VERTICAL_DATUM_INFO_F__2(dslContext.configuration(),
                    locPart, datumUnits, office);
            return parseVerticalDatumInfo(result);
        });
    }

    public void validateEntryDateSupport(boolean includeEntryDate) {
        if (includeEntryDate) {
            Record entryDateSupport = dsl.select(asterisk()).from(table("ALL_TYPES"))
                    .where(field("TYPE_NAME").eq("ZTSV_ENTRY_TYPE"))
                    .and(field("OWNER").eq("CWMS_20")).fetchOne();

            if (entryDateSupport == null) {
                throw new DataAccessException("Data entry date retrieval is not supported by this database");
            }
        }
    }

    private static String getVersionPart(ZonedDateTime versionDate) {
        if (versionDate != null) {
            return "cwms_20.cwms_util.to_timestamp(?)";
        }
        return "?";
    }

    private static VersionType getDirectReadVersionType(String versionFlag, boolean versionDateProvided) {
        if (versionDateProvided) {
            return VersionType.SINGLE_VERSION;
        }
        return parseBool(versionFlag) ? VersionType.MAX_AGGREGATE : VersionType.UNVERSIONED;
    }

    private static String[] splitTimeSeriesId(String tsId) {
        return tsId.split("\\.", 6);
    }

    private static String getTimeSeriesIdPart(String[] tsIdParts, int index) {
        return tsIdParts.length > index ? tsIdParts[index] : null;
    }

    public static String parseLocFromTimeSeriesId(String tsId) {
        return getTimeSeriesIdPart(splitTimeSeriesId(tsId), 0);
    }

    public static String getTimeZoneId(DSLContext dsl, String tsId, String officeId) {
        String locationId = TimeSeriesDaoImpl.parseLocFromTimeSeriesId(tsId);
        return CWMS_LOC_PACKAGE.call_GET_LOCAL_TIMEZONE__2(dsl.configuration(), locationId, officeId);
    }

    public static VersionType getVersionType(DSLContext dsl, String names, String office, boolean dateProvided) {
        VersionType dateVersionType;

        if (!dateProvided) {
            boolean isVersioned = isVersioned(dsl, names, office);

            if (isVersioned) {
                dateVersionType = VersionType.MAX_AGGREGATE;
            } else {
                dateVersionType = VersionType.UNVERSIONED;
            }

        } else {
            dateVersionType = VersionType.SINGLE_VERSION;
        }

        return dateVersionType;
    }

    private static boolean isVersioned(DSLContext dsl, String tsId, String office) {
        final List<String> cacheKey = Arrays.asList(office, tsId);

        Boolean cachedValue = isVersionedCache.getIfPresent(cacheKey);
        if (cachedValue == null) {
            cachedValue = parseBool(CWMS_TS_PACKAGE.call_IS_TSID_VERSIONED(dsl.configuration(), tsId, office));
            isVersionedCache.put(cacheKey, cachedValue);
        }
        return cachedValue;
    }

    // datumInfo comes back like:
    //        <vertical-datum-info office="LRL" unit="m">
    //          <location>Buckhorn</location>
    //          <native-datum>NGVD-29</native-datum>
    //          <elevation>230.7</elevation>
    //          <offset estimate="true">
    //            <to-datum>NAVD-88</to-datum>
    //            <value>-.1666</value>
    //          </offset>
    //        </vertical-datum-info>
    public static VerticalDatumInfo parseVerticalDatumInfo(String body) {
        VerticalDatumInfo retVal = null;
        if (body != null && !body.isEmpty()) {
             retVal = new XMLv1().parseContent(body, VerticalDatumInfo.class);
        }
        return retVal;
    }

    @Override
    public Catalog getTimeSeriesCatalog(String page, int pageSize, CatalogRequestParameters inputParams) {
        int total;
        String cursorTsId = "*";
        String cursorOffice = null;
        Catalog.CatalogPage catPage = null;
        FieldMapping cwmsTsIdFields = inputParams.includeAliases() ? AV_CWMS_TS_ID2_FIELD_MAP : AV_CWMS_TS_ID_FIELD_MAP;
        Table<?> table = inputParams.includeAliases() ? AV_CWMS_TS_ID2 : AV_CWMS_TS_ID.AV_CWMS_TS_ID;
        if (page == null || page.isEmpty()) {
            CommonTableExpression<?> limiter = buildWithClause(cwmsTsIdFields, inputParams, buildWhereConditions(inputParams),
                    new ArrayList<>(), pageSize, true);
            SelectJoinStep<Record1<Integer>> totalQuery = dsl.with(limiter)
                    .select(countDistinct(limiter.field(cwmsTsIdFields.getTsCode())))
                    .from(limiter);
            logger.atFine().log("%s", lazy(() -> totalQuery.getSQL(ParamType.INLINED)));
            total = totalQuery.fetchOne(0, int.class);
        } else {
            logger.atFine().log("getting non-default page");
            // Information provided by the page value overrides anything provided
            catPage = new Catalog.CatalogPage(page);
            total = catPage.getTotal();
            pageSize = catPage.getPageSize();
            cursorTsId = catPage.getCursorId();  // cursor cwms_id
            cursorOffice = catPage.getCurOffice();  // cursor office

            inputParams = CatalogRequestParameters.Builder.from(inputParams)
                    .withOffice(catPage.getSearchOffice())
                    .withIdLike(catPage.getIdLike())
                    .withLocCatLike(catPage.getLocCategoryLike())
                    .withLocGroupLike(catPage.getLocGroupLike())
                    .withTsCatLike(catPage.getTsCategoryLike())
                    .withTsGroupLike(catPage.getTsGroupLike())
                    .withBoundingOfficeLike(catPage.getBoundingOfficeLike())
                    .withIncludeExtents(catPage.isIncludeExtents())
                    .withIncludeExtents(catPage.isIncludeVersions())
                    .withExcludeEmpty(catPage.isExcludeEmpty())
                    .build();
        }
        final CatalogRequestParameters params = inputParams;

        List<Field<?>> pageEntryFields = new ArrayList<>(getCwmsTsIdFieldsToIncludeInQuery(cwmsTsIdFields));
        if (params.isIncludeExtents()) {
            pageEntryFields.addAll(getExtentsFields());
        }

        List<Condition> whereConditions = buildWhereConditions(params);
        List<Condition> pagingConditions = buildPagingConditions(cwmsTsIdFields, cursorOffice, cursorTsId);
        CommonTableExpression<?> limiter = buildWithClause(cwmsTsIdFields, params, whereConditions, pagingConditions, pageSize, false);
        Field<BigDecimal> limiterCode = limiter.field(cwmsTsIdFields.getTsCode());
        SelectOnConditionStep<?> tmpQuery = dsl.with(limiter)
                                        .select(pageEntryFields)
                                        .from(limiter)
                                        .join(table).on(limiterCode.eq(cwmsTsIdFields.getTsCode()));

        if (params.isIncludeExtents()) {

            tmpQuery = tmpQuery.leftOuterJoin(AV_TS_EXTENTS_UTC)
                                       .on(limiterCode
                                         .eq(AV_TS_EXTENTS_UTC.TS_CODE.coerce(limiterCode)));
            if (!params.isIncludeVersions()) {
                tmpQuery = tmpQuery.and(AV_TS_EXTENTS_UTC.VERSION_TIME.isNull());
            }
        }
        final SelectSeekStep2<?, String, String> overallQuery = tmpQuery.orderBy(cwmsTsIdFields.getDbOfficeId(),
                        cwmsTsIdFields.getCwmsTsId());
        logger.atFine().log("%s", lazy(() -> overallQuery.getSQL(ParamType.INLINED)));
        Result<?> result = overallQuery.fetch();

        Map<String, TimeseriesCatalogEntry.Builder> tsIdExtentMap = new LinkedHashMap<>();
        Map<String, Set<TimeSeriesAlias>> tsCodeAliasMap = new LinkedHashMap<>();
        Map<String, String> tsIdToCodeMap = new LinkedHashMap<>(); //this is only for non-aliases
        boolean includeAliases = params.includeAliases();
        result.forEach(row -> {
            String officeTsId = row.get(cwmsTsIdFields.getDbOfficeId())
                    + "/"
                    + row.get(cwmsTsIdFields.getCwmsTsId());
            if (!tsIdExtentMap.containsKey(officeTsId)) {
                TimeseriesCatalogEntry.Builder builder = new TimeseriesCatalogEntry.Builder()
                        .officeId(row.get(cwmsTsIdFields.getDbOfficeId()))
                        .cwmsTsId(row.get(cwmsTsIdFields.getCwmsTsId()))
                        .units(row.get(cwmsTsIdFields.getUnitId()))
                        .interval(row.get(cwmsTsIdFields.getIntervalId()))
                        .intervalOffset(row.get(cwmsTsIdFields.getIntervalUtcOffset()))
                        .versioned(parseBool(row.get(cwmsTsIdFields.getVerionFlag())));

                builder.timeZone(row.get("TIME_ZONE_ID", String.class));

                if (params.isIncludeExtents()) {
                    builder.withExtents(new ArrayList<>());
                }
                if (includeAliases) {
                    if (row.get(AV_CWMS_TS_ID2.ALIASED_ITEM) == null) {
                        tsIdExtentMap.put(officeTsId, builder); //only add non-aliases... aliases get added as a node to each entry later
                    }
                } else {
                    tsIdExtentMap.put(officeTsId, builder);
                }

            }
            if (includeAliases) {
                updateAliasMapping(tsCodeAliasMap, tsIdToCodeMap, row, officeTsId);
            }

            if (params.isIncludeExtents()) {
                TimeSeriesExtents extents = new TimeSeriesExtents.Builder()
                        .withEarliestTime(DateUtils.toZdt(row.get(AV_TS_EXTENTS_UTC.EARLIEST_TIME)))
                        .withLatestTime(DateUtils.toZdt(row.get(AV_TS_EXTENTS_UTC.LATEST_TIME)))
                        .withLastUpdate(DateUtils.toZdt(row.get(AV_TS_EXTENTS_UTC.LAST_UPDATE)))
                        .withVersionTime(DateUtils.toZdt(row.get(AV_TS_EXTENTS_UTC.VERSION_TIME)))
                        .build();
                TimeseriesCatalogEntry.Builder entryBuilder = tsIdExtentMap.get(officeTsId);
                if (entryBuilder != null) {
                    entryBuilder.withExtent(extents);
                }
            }
        });

        if (includeAliases) {
            addAliasesToBuilders(tsIdExtentMap, tsCodeAliasMap, tsIdToCodeMap);
        }

        List<? extends CatalogEntry> entries = tsIdExtentMap.values().stream()
                .map(TimeseriesCatalogEntry.Builder::build)
                .collect(Collectors.toList());

        return new Catalog(catPage != null ? catPage.toString() : null,
                total, pageSize, entries, params);
    }


    @NotNull
    private static Condition getFilterCondition( @Nullable FilteredTimeSeriesParameters ip, FieldResolver resolver) {
        Condition filterConditions = noCondition();
        if (ip != null) {
            String query = ip.getQuery();
            if (query != null) {
                RSQLConditionBuilder builder = RSQLConditionBuilder.create(resolver);
                Condition condition = builder.buildCondition(query);
                filterConditions = filterConditions.and(condition);
            }

        }
        return filterConditions;
    }


    private void addAliasesToBuilders(Map<String, TimeseriesCatalogEntry.Builder> tsIdExtentMap, Map<String, Set<TimeSeriesAlias>> tsCodeAliasMap, Map<String, String> tsIdToCodeMap) {
        for (Map.Entry<String, Set<TimeSeriesAlias>> entry : tsCodeAliasMap.entrySet()) {
            String tsCode = entry.getKey();
            Set<TimeSeriesAlias> aliases = entry.getValue();
            for (Map.Entry<String, String> e : tsIdToCodeMap.entrySet()) {
                String tsId = e.getKey();
                String code = e.getValue();
                if (code.equals(tsCode)) {
                    tsIdExtentMap.get(tsId).withAliases(aliases);
                    break;
                }
            }
        }
    }

    private void updateAliasMapping(Map<String, Set<TimeSeriesAlias>> tsCodeAliasMap, Map<String, String> tsIdToCodeMap, Record row, String officeTsId) {
        boolean isAlias = row.get(AV_CWMS_TS_ID2.ALIASED_ITEM) != null;
        String tsCode = row.get(AV_CWMS_TS_ID2.TS_CODE).toString();
        if (isAlias) {
            tsCodeAliasMap.computeIfAbsent(tsCode, k -> new HashSet<>())
                .add(new TimeSeriesAlias.Builder()
                    .withName(row.get(AV_CWMS_TS_ID2.TS_ALIAS_CATEGORY) + "-" + row.get(AV_CWMS_TS_ID2.TS_ALIAS_GROUP))
                    .withValue(row.get(AV_CWMS_TS_ID2.CWMS_TS_ID))
                    .build());
        } else {
            tsIdToCodeMap.put(officeTsId, tsCode);
        }
    }

    private static @NotNull List<Condition> buildPagingConditions(FieldMapping cwmsTsIdFields, String cursorOffice, String cursorTsId) {

        List<Condition> pagingConditions = new ArrayList<>();

        // Can't do the rownum thing here b/c we want global ordering, not ordering within the page.
        //pagingConditions.add(DSL.noCondition());

        if (cursorOffice != null) {
            Condition moreInSameOffice = cwmsTsIdFields.getDbOfficeId()
                    .eq(cursorOffice.toUpperCase())
                    .and(DSL.upper(cwmsTsIdFields.getCwmsTsId())
                            .greaterThan(cursorTsId));
            Condition nextOffice = cwmsTsIdFields.getDbOfficeId()
                    .greaterThan(cursorOffice.toUpperCase());
            pagingConditions.add(moreInSameOffice.or(nextOffice));
        }
        return pagingConditions;
    }

    private static @NotNull List<TableField<?,?>> getExtentsFields() {
        List<TableField<?,?>> extentsFields = new ArrayList<>();
        extentsFields.add(AV_TS_EXTENTS_UTC.VERSION_TIME);
        extentsFields.add(AV_TS_EXTENTS_UTC.EARLIEST_TIME);
        extentsFields.add(AV_TS_EXTENTS_UTC.LATEST_TIME);
        extentsFields.add(AV_TS_EXTENTS_UTC.LAST_UPDATE);
        return extentsFields;
    }

    private @NotNull List<Field<?>> getCwmsTsIdFieldsToIncludeInQuery(FieldMapping cwmsTsIdFields) {
        List<Field<?>> retVal = new ArrayList<>();
        retVal.add(cwmsTsIdFields.getDbOfficeId());
        retVal.add(cwmsTsIdFields.getCwmsTsId());
        retVal.add(cwmsTsIdFields.getUnitId());
        retVal.add(cwmsTsIdFields.getIntervalId());
        retVal.add(cwmsTsIdFields.getIntervalUtcOffset());
        retVal.add(cwmsTsIdFields.getTimeZoneId());
        retVal.add(cwmsTsIdFields.getVerionFlag());
        if (cwmsTsIdFields.includesAliases()) {
            retVal.add(AV_CWMS_TS_ID2.ALIASED_ITEM);
            retVal.add(AV_CWMS_TS_ID2.TS_CODE);
            retVal.add(AV_CWMS_TS_ID2.TS_ALIAS_CATEGORY);
            retVal.add(AV_CWMS_TS_ID2.TS_ALIAS_GROUP);
        }
        return retVal;
    }

    private @NotNull List<Condition> buildWhereConditions(CatalogRequestParameters params) {
        List<Condition> conditions = new ArrayList<>();
        conditions.addAll(buildCwmsTsIdConditions(params));
        conditions.addAll(buildLocGrpAssgnConditions(params));
        conditions.addAll(buildTsGrpAssgnConditions(params));
        conditions.addAll(buildLocConditions(params));
        conditions.addAll(buildExtentsConditions(params));
        return conditions;
    }

    private static @NotNull CommonTableExpression<?> buildWithClause(FieldMapping cwmsTsIdFields, CatalogRequestParameters params,
                                                                     List<Condition> whereConditions, List<Condition> pagingConditions, int pageSize, boolean forCount) {
        Table<?> fromTable = params.includeAliases() ? AV_CWMS_TS_ID2 : AV_CWMS_TS_ID.AV_CWMS_TS_ID;
        List<Field<?>> selectFields = new ArrayList<>();
        selectFields.add(fromTable.field(cwmsTsIdFields.getTsCode()));
        selectFields.add(fromTable.field(cwmsTsIdFields.getDbOfficeId()));

        selectFields.add(fromTable.field(cwmsTsIdFields.getCwmsTsId()));
        TableOnConditionStep<Record> on = null;
        Table<?> table = params.includeAliases() ? AV_CWMS_TS_ID2 : AV_CWMS_TS_ID.AV_CWMS_TS_ID;
        if (params.needs(tsGroupView)) {
            on = table
                    .join(tsGroupView)
                    .on(cwmsTsIdFields.getTsCode().eq(tsGroupView.TS_CODE));
            fromTable = on;
        }

        if (params.needs(AV_LOC_GRP_ASSGN.AV_LOC_GRP_ASSGN)) {
            if (on == null) {
                on = table
                        .leftJoin(AV_LOC_GRP_ASSGN.AV_LOC_GRP_ASSGN)
                        .on(cwmsTsIdFields.getLocationCode()
                                .eq(AV_LOC_GRP_ASSGN.AV_LOC_GRP_ASSGN.LOCATION_CODE));
            } else {
                on = on
                        .leftJoin(AV_LOC_GRP_ASSGN.AV_LOC_GRP_ASSGN)
                        .on(cwmsTsIdFields.getLocationCode()
                                .eq(AV_LOC_GRP_ASSGN.AV_LOC_GRP_ASSGN.LOCATION_CODE));
            }
            fromTable = on;
        }

        if (params.needs(AV_LOC.AV_LOC)) {
            if (on == null) {
                on = table
                        .leftJoin(AV_LOC.AV_LOC)
                        .on(AV_LOC.AV_LOC.LOCATION_CODE
                                .eq(cwmsTsIdFields.getLocationCode()
                                        .coerce(AV_LOC.AV_LOC.LOCATION_CODE)));
            } else {
                on = on
                        .leftJoin(AV_LOC.AV_LOC)
                        .on(AV_LOC.AV_LOC.LOCATION_CODE
                                .eq(cwmsTsIdFields.getLocationCode()
                                        .coerce(AV_LOC.AV_LOC.LOCATION_CODE)));
            }
            selectFields.add(AV_LOC.AV_LOC.BOUNDING_OFFICE_ID);
            fromTable = on;
        }

        if (params.isExcludeEmpty()) {
            if (on == null) {
                on = table
                        .leftJoin(AV_TS_EXTENTS_UTC)
                        .on(cwmsTsIdFields.getTsCode()
                                .eq(AV_TS_EXTENTS_UTC.TS_CODE
                                        .coerce(cwmsTsIdFields.getTsCode())));
            } else {
                on = on
                        .leftJoin(AV_TS_EXTENTS_UTC)
                        .on(cwmsTsIdFields.getTsCode()
                                .eq(AV_TS_EXTENTS_UTC.TS_CODE
                                        .coerce(cwmsTsIdFields.getTsCode())));
            }
            fromTable = on;
        }

        TableLike<?> innerSelect = selectDistinct(selectFields)
                                     .from(fromTable)
                                     .where(whereConditions).and(DSL.and(pagingConditions))
                                     .orderBy(cwmsTsIdFields.getDbOfficeId(),
                                             cwmsTsIdFields.getCwmsTsId())
                                     .asTable("limiterInner");
        if (forCount) {
            return name("limiter").as(
                    select(asterisk())
                    .from(innerSelect)
                    .orderBy(innerSelect.field(cwmsTsIdFields.getDbOfficeId()),
                            innerSelect.field(cwmsTsIdFields.getCwmsTsId()))
                    );
        } else {
            return name("limiter").as(
                    select(asterisk())
                    .from(innerSelect)
                    .where(field("rownum").lessOrEqual(pageSize))
                    .orderBy(innerSelect.field(cwmsTsIdFields.getDbOfficeId()),
                            innerSelect.field(cwmsTsIdFields.getCwmsTsId()))
                    );
        }
    }

    private Collection<? extends Condition> buildLocConditions(CatalogRequestParameters params) {
        List<Condition> retval = new ArrayList<>();

        if (params.needs(AV_LOC.AV_LOC)) {
            retval.add(caseInsensitiveLikeRegexNullTrue(AV_LOC.AV_LOC.BOUNDING_OFFICE_ID,
                    params.getBoundingOfficeLike()));
            retval.add(caseInsensitiveLikeRegexNullTrue(AV_LOC.AV_LOC.LOCATION_KIND_ID, params.getLocationKind()));
            retval.add(caseInsensitiveLikeRegexNullTrue(AV_LOC.AV_LOC.LOCATION_TYPE, params.getLocationType()));
            // we could add conditions based on lat/lon here too
            // or any bool fields.
        }

        return retval;
    }

    private Collection<? extends Condition> buildExtentsConditions(CatalogRequestParameters params) {
        List<Condition> retval = new ArrayList<>();

        if (params.isExcludeEmpty()) {
            retval.add(DSL.or(
                AV_TS_EXTENTS_UTC.VERSION_TIME.isNotNull(),
                AV_TS_EXTENTS_UTC.EARLIEST_TIME.isNotNull(),
                AV_TS_EXTENTS_UTC.LATEST_TIME.isNotNull(),
                AV_TS_EXTENTS_UTC.LAST_UPDATE.isNotNull())
            );
        }
        return retval;
    }

    private Collection<? extends Condition> buildLocGrpAssgnConditions(CatalogRequestParameters params) {
        List<Condition> retval = new ArrayList<>();

        if (params.needs(AV_LOC_GRP_ASSGN.AV_LOC_GRP_ASSGN)) {
            retval.add(caseInsensitiveLikeRegexNullTrue(locGroupField,
                    params.getLocGroupLike()));
            retval.add(caseInsensitiveLikeRegexNullTrue(locCategoryField,
                    params.getLocCatLike()));
        }
        return retval;
    }

    private Collection<? extends Condition> buildTsGrpAssgnConditions(CatalogRequestParameters params) {
        List<Condition> retval = new ArrayList<>();

        if (params.needs(AV_TS_GRP_ASSGN.AV_TS_GRP_ASSGN)) {
            retval.add(caseInsensitiveLikeRegexNullTrue(tsGroupField,
                    params.getTsGroupLike()));
            retval.add(caseInsensitiveLikeRegexNullTrue(tsCategoryField,
                    params.getTsCatLike()));
        }
        return retval;
    }

    private Collection<? extends Condition> buildCwmsTsIdConditions(CatalogRequestParameters params) {
        List<Condition> retval = new ArrayList<>();
        FieldMapping cwmsTsIdFields = params.includeAliases() ? AV_CWMS_TS_ID2_FIELD_MAP : AV_CWMS_TS_ID_FIELD_MAP;
        if (params.getOffice() != null) {
            retval.add(cwmsTsIdFields.getDbOfficeId().eq(params.getOffice().toUpperCase()));
        }

        retval.add(
            caseInsensitiveLikeRegexNullTrue(
                cwmsTsIdFields.getCwmsTsId(),
                params.getIdLike()));

        return retval;
    }

    // Finds the single most recent TsvDqu within the time window.
    public TsvDqu findMostRecent(String officeId, String tsId, String unit,
                                 Timestamp twoWeeksFromNow, Timestamp twoWeeksAgo) {
        TsvDqu retval = null;

        AV_TSV_DQU view = AV_TSV_DQU.AV_TSV_DQU;

        Condition nestedCondition = view.ALIASED_ITEM.isNull()
                .and(view.VALUE.isNotNull())
                .and(view.CWMS_TS_ID.eq(tsId))
                .and(view.OFFICE_ID.eq(officeId.toUpperCase()));

        if (twoWeeksFromNow != null) {
            nestedCondition = nestedCondition.and(view.DATE_TIME.lt(twoWeeksFromNow));
        }

        // Is this really optional?
        if (twoWeeksAgo != null) {
            nestedCondition = nestedCondition.and(view.DATE_TIME.gt(twoWeeksAgo));
        }


        SelectHavingStep<Record1<Timestamp>> maxSelect =
                dsl.select(max(view.DATE_TIME).as(MAX_DATE_TIME))
                        .from(view)
                        .where(nestedCondition)
                        .groupBy(view.TS_CODE);

        Record dquRecord = dsl.select(asterisk())
                .from(view)
                .where(view.DATE_TIME.in(maxSelect))
                .and(view.CWMS_TS_ID.eq(tsId))
                .and(view.OFFICE_ID.eq(officeId.toUpperCase()))
                .and(view.UNIT_ID.eq(unit))
                .and(view.VALUE.isNotNull())
                .and(view.ALIASED_ITEM.isNull())
                .fetchOne();

        if (dquRecord != null) {
            retval = dquRecord.map(jrecord -> new TsvDqu.Builder()
                    .withOfficeId(jrecord.getValue(view.OFFICE_ID.getName(), String.class))
                    .withCwmsTsId(jrecord.getValue(view.CWMS_TS_ID.getName(), String.class))
                    .withUnitId(jrecord.getValue(view.UNIT_ID.getName(), String.class))
                    .withDateTime(jrecord.getValue(view.DATE_TIME.getName(), Timestamp.class))
                    .withVersionDate(jrecord.getValue(view.VERSION_DATE.getName(), Timestamp.class))
                    .withDataEntryDate(jrecord.getValue(view.DATA_ENTRY_DATE.getName(), Timestamp.class))
                    .withValue(jrecord.getValue(view.VALUE.getName(), Double.class))
                    .withQualityCode(jrecord.getValue(view.QUALITY_CODE.getName(), Long.class))
                    .withStartDate(jrecord.getValue(view.START_DATE.getName(), Timestamp.class))
                    .withEndDate(jrecord.getValue(view.END_DATE.getName(), Timestamp.class))
                    .build());
        }

        return retval;
    }


    // This is similar to the code used for sparklines...
    // Finds all the Tsv data points in the time range for all the specified tsIds.
    public List<Tsv> findInDateRange(Collection<String> tsIds, Date startDate, Date endDate) {
        List<Tsv> retval = Collections.emptyList();

        if (tsIds != null && !tsIds.isEmpty()) {

            Timestamp start = new Timestamp(startDate.getTime());
            Timestamp end = new Timestamp(endDate.getTime());

            AV_TSV tsvView = AV_TSV.AV_TSV;
            usace.cwms.db.jooq.codegen.tables.AV_CWMS_TS_ID2 tsView = AV_CWMS_TS_ID2;
            retval = dsl.select(tsvView.asterisk(), tsView.CWMS_TS_ID)
                    .from(tsvView.join(tsView).on(tsvView.TS_CODE.eq(tsView.TS_CODE.cast(Long.class))))
                    .where(
                            tsView.CWMS_TS_ID.in(tsIds)
                                    .and(tsvView.DATE_TIME.ge(start))
                                    .and(tsvView.DATE_TIME.lt(end))
                                    .and(tsvView.START_DATE.le(end))
                                    .and(tsvView.END_DATE.gt(start)))
                    .orderBy(tsvView.DATE_TIME).fetch(
                            jrecord -> buildTsvFromViewRow(jrecord.into(tsvView)));
        }
        return retval;
    }

    @NotNull
    private Tsv buildTsvFromViewRow(usace.cwms.db.jooq.codegen.tables.records.AV_TSV into) {
        TsvId id = new TsvId(into.getTS_CODE(), into.getDATE_TIME(), into.getVERSION_DATE(),
                into.getDATA_ENTRY_DATE());

        return new Tsv(id, into.getVALUE(), into.getQUALITY_CODE(), into.getSTART_DATE(),
                into.getEND_DATE());
    }


    @Override
    public List<RecentValue> findMostRecentsInRange(String office, List<String> tsIds, Timestamp pastdate,
                                                    Timestamp futuredate, UnitSystem unitSystem) {
        List<RecentValue> retval = Collections.emptyList();

        if (tsIds != null && !tsIds.isEmpty()) {

            // build whereCondition depending on office
            Condition whereCondition = AV_CWMS_TS_ID2.CWMS_TS_ID.in(tsIds);
            if (office != null) {
                whereCondition = whereCondition.and(AV_CWMS_TS_ID2.DB_OFFICE_ID.eq(office.toUpperCase()));
            }

            // create baseIds alias
            CommonTableExpression<?> baseIds = name("base_ids").as(
                    selectDistinct(AV_CWMS_TS_ID2.TS_CODE, AV_CWMS_TS_ID2.CWMS_TS_ID, AV_CWMS_TS_ID2.UNIT_ID)
                            .from(AV_CWMS_TS_ID2)
                            .where(whereCondition));

            // convert timestamp to date
            java.sql.Date startDate = new java.sql.Date(pastdate.getTime());
            java.sql.Date endDate = new java.sql.Date(futuredate.getTime());

            // extract year
            LocalDate localEndDate = endDate.toLocalDate();
            LocalDate localStartDate = startDate.toLocalDate();
            int year1 = localStartDate.getYear();
            int year2 = localEndDate.getYear();


            // helper subquery for SELECT ts_code FROM base_ids
            Select<Record1<BigDecimal>> tsCodeSubquery = select(baseIds.field(AV_CWMS_TS_ID2.TS_CODE))
                    .from(baseIds);

            // references to appropriate year tables, current year and past year
            Table<?> AT_TSV_PREV_YEAR_TABLE = table(name(CWMS_20, "AT_TSV_" + year1));
            Table<?> AT_TSV_CURR_YEAR_TABLE = table(name(CWMS_20, "AT_TSV_" + year2));

            Select<Record> prevYearSelect = select(asterisk())
                    .from(AT_TSV_PREV_YEAR_TABLE)
                    .where(field(name(AT_TSV_PREV_YEAR_TABLE.getName(), DATE_TIME), java.sql.Date.class).between(startDate, endDate))
                    .and(field(name(AT_TSV_PREV_YEAR_TABLE.getName(), TS_CODE), BigDecimal.class).in(tsCodeSubquery));

            Select<Record> currYearSelect = select(asterisk())
                    .from(AT_TSV_CURR_YEAR_TABLE)
                    .where(field(name(AT_TSV_CURR_YEAR_TABLE.getName(), DATE_TIME), java.sql.Date.class).between(startDate, endDate))
                    .and(field(name(AT_TSV_CURR_YEAR_TABLE.getName(), TS_CODE), BigDecimal.class).in(tsCodeSubquery));

            // union tables if start and end date are not in the same year
            Select<Record> combinedSelect = (year1 == year2)
                    ? prevYearSelect
                    : prevYearSelect.unionAll(currYearSelect);

            CommonTableExpression<?> tsvLimited = name("tsv_limited").as(combinedSelect);

            // Create table and field references for AT_TS_EXTENTS
            Table<?> AT_TS_EXTENTS_TABLE = table(name(CWMS_20, AT_TS_EXTENTS));
            Field<BigDecimal> AT_TS_EXTENTS_TS_CODE = field(name(CWMS_20, AT_TS_EXTENTS, TS_CODE), BigDecimal.class);
            Field<java.sql.Date> AT_TS_EXTENTS_VERSION_TIME = field(name(CWMS_20, AT_TS_EXTENTS, "VERSION_TIME"), java.sql.Date.class);
            Field<Timestamp> AT_TS_EXTENTS_EARLIEST_ENTRY_TIME = field(name(CWMS_20, AT_TS_EXTENTS, "EARLIEST_ENTRY_TIME"), Timestamp.class);
            Field<Timestamp> AT_TS_EXTENTS_LATEST_ENTRY_TIME = field(name(CWMS_20, AT_TS_EXTENTS, "LATEST_ENTRY_TIME"), Timestamp.class);

            // Extract repeated TsCode and DateTime
            Field<BigDecimal> tsvLimitedTsCode = field(name(tsvLimited.getName(), TS_CODE), BigDecimal.class);
            Field<java.sql.Date> tsvLimitedDateTime = field(name(tsvLimited.getName(), DATE_TIME), java.sql.Date.class);

            // create max_values alias
            CommonTableExpression<?> maxValues = name("max_values").as(
                    select(
                            field(name(baseIds.getName(), CWMS_TS_ID), String.class),
                            field(name(baseIds.getName(), UNIT_ID), String.class),
                            tsvLimitedTsCode,
                            tsvLimitedDateTime,
                            field(name(tsvLimited.getName(), VALUE), BigDecimal.class),
                            field(name(tsvLimited.getName(), VERSION_DATE), java.sql.Date.class),
                            field(name(tsvLimited.getName(), DATA_ENTRY_DATE), java.sql.Date.class),
                            field(name(tsvLimited.getName(), QUALITY_CODE), Integer.class),
                            AT_TS_EXTENTS_EARLIEST_ENTRY_TIME.as(START_DATE),
                            AT_TS_EXTENTS_LATEST_ENTRY_TIME.as(END_DATE),
                            max(tsvLimitedDateTime)
                                    .over(partitionBy(tsvLimitedTsCode))
                                    .as(MAX_DATE_TIME)
                    )
                            .from(tsvLimited)
                            .join(baseIds).on(field(name(baseIds.getName(), TS_CODE), BigDecimal.class).equal(tsvLimitedTsCode))
                            .join(AT_TS_EXTENTS_TABLE).on(
                                    AT_TS_EXTENTS_TS_CODE.equal(tsvLimitedTsCode)
                                            .and(AT_TS_EXTENTS_VERSION_TIME
                                                    .equal(field(name(tsvLimited.getName(), VERSION_DATE), java.sql.Date.class)))
                            )
            );

            // Set default units and convert
            Field<String> getDefaultUnits = CWMS_UTIL_PACKAGE.call_GET_DEFAULT_UNITS(
                            CWMS_TS_PACKAGE.call_GET_BASE_PARAMETER_ID(field(name(maxValues.getName(), TS_CODE), BigDecimal.class)),
                            DSL.val(unitSystem, String.class));

            Field<Double> convertUnits = CWMS_UTIL_PACKAGE.call_CONVERT_UNITS(
                    field(name(maxValues.getName(), VALUE), Double.class),
                    field(name(maxValues.getName(), UNIT_ID), String.class),
                    getDefaultUnits
            );

            // Final query
            SelectConditionStep<Record10<String, java.sql.Date, java.sql.Date, java.sql.Date, Integer, java.sql.Date, java.sql.Date, String, java.sql.Date, Double>> query = dsl.with(baseIds)
                    .with(tsvLimited)
                    .with(maxValues)
                    .select(
                            field(name(maxValues.getName(), CWMS_TS_ID), String.class),
                            field(name(maxValues.getName(), DATE_TIME), java.sql.Date.class),
                            field(name(maxValues.getName(), VERSION_DATE), java.sql.Date.class),
                            field(name(maxValues.getName(), DATA_ENTRY_DATE), java.sql.Date.class),
                            field(name(maxValues.getName(), QUALITY_CODE), Integer.class),
                            field(name(maxValues.getName(), START_DATE), java.sql.Date.class),
                            field(name(maxValues.getName(), END_DATE), java.sql.Date.class),
                            getDefaultUnits.as(DEFAULT_UNITS),
                            field(name(maxValues.getName(), DATE_TIME), java.sql.Date.class).as(MAX_DATE_TIME),
                            convertUnits.as(VALUE_AT_MAX_DATE)
                    )
                    .from(maxValues)
                    .where(field(name(maxValues.getName(), DATE_TIME), java.sql.Date.class)
                            .eq(field(name(maxValues.getName(), MAX_DATE_TIME), java.sql.Date.class)));

            logger.atFine().log("%s", lazy(() -> query.getSQL(ParamType.INLINED)));

            // fetch and build records
            retval = query.fetch(r -> {
                TsvDqu tsv = new TsvDqu.Builder()
                        .withOfficeId(office)
                        .withCwmsTsId(r.getValue(CWMS_TS_ID, String.class))
                        .withUnitId(r.getValue(DEFAULT_UNITS, String.class))
                        .withDateTime(r.getValue(DATE_TIME, java.sql.Date.class))
                        .withVersionDate(r.getValue(VERSION_DATE, java.sql.Date.class))
                        .withDataEntryDate(r.getValue(DATA_ENTRY_DATE, java.sql.Date.class))
                        .withValue(r.getValue(VALUE_AT_MAX_DATE, Double.class))
                        .withQualityCode(r.getValue(QUALITY_CODE, Long.class))
                        .withStartDate(r.getValue(START_DATE, java.sql.Date.class))
                        .withEndDate(r.getValue(END_DATE, java.sql.Date.class))
                        .build();
                return new RecentValue(r.getValue(CWMS_TS_ID, String.class), tsv);
            });
        }

        return retval;
    }

    @Override
    public List<RecentValue> findRecentsInRange(String office, String categoryId, String groupId,
                                                @NotNull Timestamp pastLimit, @NotNull Timestamp futureLimit,
                                                 @NotNull UnitSystem unitSystem) {

        List<RecentValue> retval;

        // Create whereCondition for filtering by category, group, and office
        Condition whereCondition = DSL.noCondition();
        if (categoryId != null) {
            whereCondition = whereCondition.and(AV_TS_GRP_ASSGN.AV_TS_GRP_ASSGN.CATEGORY_ID.eq(categoryId));
        }
        if (groupId != null) {
            whereCondition = whereCondition.and(AV_TS_GRP_ASSGN.AV_TS_GRP_ASSGN.GROUP_ID.eq(groupId));
        }
        if (office != null) {
            whereCondition = whereCondition.and(AV_TS_GRP_ASSGN.AV_TS_GRP_ASSGN.DB_OFFICE_ID.eq(office.toUpperCase()));
        }

        CommonTableExpression<?> baseIds = name("base_ids").as(
                selectDistinct(AV_TS_GRP_ASSGN.AV_TS_GRP_ASSGN.TS_CODE, AV_TS_GRP_ASSGN.AV_TS_GRP_ASSGN.TS_ID)
                        .from(AV_TS_GRP_ASSGN.AV_TS_GRP_ASSGN)
                        .where(whereCondition));

        // convert timestamp to date
        java.sql.Date startDate = new java.sql.Date(pastLimit.getTime());
        java.sql.Date endDate = new java.sql.Date(futureLimit.getTime());

        // extract year
        LocalDate localEndDate = endDate.toLocalDate();
        LocalDate localStartDate = startDate.toLocalDate();
        int year1 = localStartDate.getYear();
        int year2 = localEndDate.getYear();


        // helper subquery for SELECT ts_code FROM base_ids
        Select<Record1<BigDecimal>> tsCodeSubquery = select(baseIds.field(AV_CWMS_TS_ID2.TS_CODE))
                .from(baseIds);

        // references to appropriate year tables, current year and past year
        Table<?> AT_TSV_PREV_YEAR_TABLE = table(name(CWMS_20, "AT_TSV_" + year1));
        Table<?> AT_TSV_CURR_YEAR_TABLE = table(name(CWMS_20, "AT_TSV_" + year2));

        Select<Record> prevYearSelect = select(asterisk())
                .from(AT_TSV_PREV_YEAR_TABLE)
                .where(field(name(AT_TSV_PREV_YEAR_TABLE.getName(), DATE_TIME), java.sql.Date.class).between(startDate, endDate))
                .and(field(name(AT_TSV_PREV_YEAR_TABLE.getName(), TS_CODE), BigDecimal.class).in(tsCodeSubquery));

        Select<Record> currYearSelect = select(asterisk())
                .from(AT_TSV_CURR_YEAR_TABLE)
                .where(field(name(AT_TSV_CURR_YEAR_TABLE.getName(), DATE_TIME), java.sql.Date.class).between(startDate, endDate))
                .and(field(name(AT_TSV_CURR_YEAR_TABLE.getName(), TS_CODE), BigDecimal.class).in(tsCodeSubquery));

        // union tables if start and end date are not in the same year
        Select<Record> combinedSelect = (year1 == year2)
                ? prevYearSelect
                : prevYearSelect.unionAll(currYearSelect);

        CommonTableExpression<?> tsvLimited = name("tsv_limited").as(combinedSelect);

        // Create table and field references for AT_TS_EXTENTS
        Table<?> AT_TS_EXTENTS_TABLE = table(name(CWMS_20, AT_TS_EXTENTS));
        Field<BigDecimal> AT_TS_EXTENTS_TS_CODE = field(name(CWMS_20, AT_TS_EXTENTS, TS_CODE), BigDecimal.class);
        Field<java.sql.Date> AT_TS_EXTENTS_VERSION_TIME = field(name(CWMS_20, AT_TS_EXTENTS, "VERSION_TIME"), java.sql.Date.class);
        Field<Timestamp> AT_TS_EXTENTS_EARLIEST_ENTRY_TIME = field(name(CWMS_20, AT_TS_EXTENTS, "EARLIEST_ENTRY_TIME"), Timestamp.class);
        Field<Timestamp> AT_TS_EXTENTS_LATEST_ENTRY_TIME = field(name(CWMS_20, AT_TS_EXTENTS, "LATEST_ENTRY_TIME"), Timestamp.class);

        // Extract repeated TsCode and DateTime
        Field<BigDecimal> tsvLimitedTsCode = field(name(tsvLimited.getName(), TS_CODE), BigDecimal.class);
        Field<java.sql.Date> tsvLimitedDateTime = field(name(tsvLimited.getName(), DATE_TIME), java.sql.Date.class);

        // Create max_values alias
        CommonTableExpression<?> maxValues = name("max_values").as(
                select(
                        field(name(baseIds.getName(), TS_ID), String.class),
                        AV_CWMS_TS_ID2.UNIT_ID,
                        tsvLimitedTsCode,
                        tsvLimitedDateTime,
                        field(name(tsvLimited.getName(), VALUE), BigDecimal.class),
                        field(name(tsvLimited.getName(), VERSION_DATE), java.sql.Date.class),
                        field(name(tsvLimited.getName(), DATA_ENTRY_DATE), java.sql.Date.class),
                        field(name(tsvLimited.getName(), QUALITY_CODE), Integer.class),
                        AT_TS_EXTENTS_EARLIEST_ENTRY_TIME.as(START_DATE),
                        AT_TS_EXTENTS_LATEST_ENTRY_TIME.as(END_DATE),
                        max(tsvLimitedDateTime)
                                .over(partitionBy(tsvLimitedTsCode))
                                .as(MAX_DATE_TIME)
                )
                        .from(tsvLimited)
                        .join(baseIds).on(field(name(baseIds.getName(), TS_CODE), BigDecimal.class).equal(tsvLimitedTsCode))
                        .join(AV_CWMS_TS_ID2).on(field(name(baseIds.getName(), TS_CODE), BigDecimal.class).equal(AV_CWMS_TS_ID2.TS_CODE))
                        .join(AT_TS_EXTENTS_TABLE).on(
                                AT_TS_EXTENTS_TS_CODE.equal(tsvLimitedTsCode)
                                        .and(AT_TS_EXTENTS_VERSION_TIME
                                                .equal(field(name(tsvLimited.getName(), VERSION_DATE), java.sql.Date.class)))
                        )
        );

        // Set default units and convert
        Field<String> getDefaultUnits = CWMS_UTIL_PACKAGE.call_GET_DEFAULT_UNITS(
                CWMS_TS_PACKAGE.call_GET_BASE_PARAMETER_ID(field(name(maxValues.getName(), TS_CODE), BigDecimal.class)),
                DSL.val(unitSystem, String.class));

        Field<Double> convertUnits = CWMS_UTIL_PACKAGE.call_CONVERT_UNITS(
                field(name(maxValues.getName(), VALUE), Double.class),
                field(name(maxValues.getName(), UNIT_ID), String.class),
                getDefaultUnits
        );

        // Final query
        SelectConditionStep<Record10<String, java.sql.Date, java.sql.Date, java.sql.Date, Integer, java.sql.Date, java.sql.Date, String, java.sql.Date, Double>> query = dsl.with(baseIds)
                .with(tsvLimited)
                .with(maxValues)
                .selectDistinct(
                        field(name(maxValues.getName(), TS_ID), String.class),
                        field(name(maxValues.getName(), DATE_TIME), java.sql.Date.class),
                        field(name(maxValues.getName(), VERSION_DATE), java.sql.Date.class),
                        field(name(maxValues.getName(), DATA_ENTRY_DATE), java.sql.Date.class),
                        field(name(maxValues.getName(), QUALITY_CODE), Integer.class),
                        field(name(maxValues.getName(), START_DATE), java.sql.Date.class),
                        field(name(maxValues.getName(), END_DATE), java.sql.Date.class),
                        getDefaultUnits.as(DEFAULT_UNITS),
                        field(name(maxValues.getName(), DATE_TIME), java.sql.Date.class).as(MAX_DATE_TIME),
                        convertUnits.as(VALUE_AT_MAX_DATE)
                )
                .from(maxValues)
                .where(field(name(maxValues.getName(), DATE_TIME), java.sql.Date.class)
                        .eq(field(name(maxValues.getName(), MAX_DATE_TIME), java.sql.Date.class)));

        logger.atFine().log("%s", lazy(() -> query.getSQL(ParamType.INLINED)));

        // fetch and build records
        retval = query.fetch(r -> {
            TsvDqu tsv = new TsvDqu.Builder()
                    .withOfficeId(office)
                    .withCwmsTsId(r.getValue(TS_ID, String.class))
                    .withUnitId(r.getValue(DEFAULT_UNITS, String.class))
                    .withDateTime(r.getValue(DATE_TIME, java.sql.Date.class))
                    .withVersionDate(r.getValue(VERSION_DATE, java.sql.Date.class))
                    .withDataEntryDate(r.getValue(DATA_ENTRY_DATE, java.sql.Date.class))
                    .withValue(r.getValue(VALUE_AT_MAX_DATE, Double.class))
                    .withQualityCode(r.getValue(QUALITY_CODE, Long.class))
                    .withStartDate(r.getValue(START_DATE, java.sql.Date.class))
                    .withEndDate(r.getValue(END_DATE, java.sql.Date.class))
                    .build();
            return new RecentValue(r.getValue(TS_ID, String.class), tsv);
        });

        return retval;
    }


    @Override
    public void create(TimeSeries input) {
        create(input, false, StoreRule.REPLACE_ALL, TimeSeriesDaoImpl.OVERRIDE_PROTECTION, null);
    }

    /**
     * Create and save, or update existing Timeseries.
     * Required attributes of {@link TimeSeries Timeseries} are
     *
     * <ul>
     *  <li>{@link TimeSeries#getName()}  Timeseries Id</li>
     *  <li>{@link TimeSeries#getOfficeId()}  Office ID</li>
     *  <li>{@link TimeSeries#getUnits()}  Units</li>
     *  <li>{@link TimeSeries#getValues()}  values</li>
     * </ul>
     *
     * Other parameters may be passed in, but will either be ignored or used to validate existing
     * database entries.
     *
     * @param input Actual timeseries data
     * @param createAsLrts Is this an irregular but well defined interval time series (e.g.
     *                     daily data in a local time zone.)
     *
     * @param storeRule How to update the database if data exists. {@see cwms.cda.data.dao.StoreRule for more detail}
     * @param overrideProtection honor override protection
     * @param vd The VerticalDatum in which specified elevations are interpreted.
     *
     */
    @SuppressWarnings("unused")
    public void create(TimeSeries input,
                       boolean createAsLrts, StoreRule storeRule, boolean overrideProtection, VerticalDatum vd) {

        Timestamp versionDate;
        if (input.getVersionDate() != null) {
            versionDate = Timestamp.from(input.getVersionDate().toInstant());
        } else {
            versionDate = null;
        }

        connection(dsl, connection -> {
            DSLContext dslContext = getDslContext(connection, input.getOfficeId());

            withDefaultDatum(vd, dslContext, (conn) -> {
                // the code does not need to be created before hand.
                // do not add a call to create_ts_code

                if (!input.getValues().isEmpty()) {
                    store(dslContext, input.getOfficeId(), input.getName(), input.getUnits(),
                            versionDate, input.getValues(), createAsLrts, storeRule,
                            overrideProtection);
                }
            });
        });
    }

    @Override
    public void store(TimeSeries timeSeries, Timestamp versionDate) {
        store(timeSeries, false, StoreRule.REPLACE_ALL, TimeSeriesDaoImpl.OVERRIDE_PROTECTION, null);
    }

    public void store(TimeSeries input, boolean createAsLrts, StoreRule replaceAll, boolean overrideProtection, VerticalDatum vd) {
        Timestamp versionDate;
        if (input.getVersionDate() != null) {
            versionDate = Timestamp.from(input.getVersionDate().toInstant());
        } else {
            versionDate = null;
        }

        connection(dsl, connection -> storeWithDefaultDatum(input, createAsLrts, replaceAll, overrideProtection, vd, connection, versionDate));
    }

    private void storeWithDefaultDatum(TimeSeries input, boolean createAsLrts, StoreRule replaceAll, boolean overrideProtection,
                                       VerticalDatum vd, Connection connection, Timestamp versionDate) throws Throwable {
        DSLContext dslContext = getDslContext(connection, input.getOfficeId());
        withDefaultDatum(vd, dslContext, (conn) -> store(dslContext, input.getOfficeId(), input.getName(), input.getUnits(),
                versionDate, input.getValues(), createAsLrts, replaceAll, overrideProtection));
    }

    private void store(DSLContext dslContext, String officeId, String tsId, String units,
                       Timestamp versionDate, List<TimeSeries.Record> values, boolean createAsLrts,
                       StoreRule storeRule, boolean overrideProtection) {

        final ZTSV_ARRAY tsvArray = new ZTSV_ARRAY();

        if (values != null && !values.isEmpty()) {
            for (TimeSeries.Record value : values) {
                Double dataValue = value.getValue();
                if (dataValue != null && dataValue == -Float.MAX_VALUE) {
                    dataValue = null;
                }
                tsvArray.add(new ZTSV_TYPE(value.getDateTime(), dataValue, BigDecimal.valueOf(value.getQualityCode())));
            }
        }


        if (versionDate != null) {
            try {
                CWMS_TS_PACKAGE.call_SET_TSID_VERSIONED(dslContext.configuration(),
                        tsId, "T", officeId);
            } catch (DataAccessException e) {
                if (e.getCause() instanceof SQLException) {
                    SQLException cause = (SQLException)e.getCause();

                    if (cause.getErrorCode() != TS_ID_MISSING_CODE) {
                        throw e;
                    }
                    // Ignore tsId not found exceptions. tsDao.store() will create tsId if it is not found
                    logger.atFiner().withCause(e).log("TS ID: %s not found at office: %s", tsId, officeId);
                } else {
                    throw e;
                }
            }
        }
        CWMS_TS_PACKAGE.call_ZSTORE_TS(dslContext.configuration(),
                                      tsId,
                                      units,
                                      tsvArray,
                                      storeRule.getRule(),
                                      formatBool(overrideProtection),
                                      versionDate,
                                      officeId,
                                      formatBool(createAsLrts));
    }

    protected BigDecimal retrieveTsCode(String tsId) {

        return dsl.select(AV_CWMS_TS_ID2.TS_CODE)
                .from(AV_CWMS_TS_ID2)
                .where(AV_CWMS_TS_ID2.CWMS_TS_ID.eq(tsId))
                .fetchOptional(AV_CWMS_TS_ID2.TS_CODE).orElse(null);
    }

    public boolean timeseriesExists(String tsId) {
        return retrieveTsCode(tsId) != null;
    }


    public void delete(String officeId, String tsId, TimeSeriesDeleteOptions options) {
        connection(dsl, connection -> {
            Timestamp startTime = options.getStartTime() == null ? null : Timestamp.from(options.getStartTime().toInstant());
            Timestamp endTime = options.getEndTime() == null ? null : Timestamp.from(options.getEndTime().toInstant());
            Timestamp versionDate = options.getVersionDate() == null ? null : Timestamp.from(options.getVersionDate().toInstant());
            DATE_TABLE_TYPE pDateTimes = null;
            BigInteger pTsItemMask = null;
            if (options.getTsItemMask() != null) {
                pTsItemMask = BigInteger.valueOf(options.getTsItemMask());
            }
            CWMS_TS_PACKAGE.call_DELETE_TS__3(getDslContext(connection, officeId).configuration(), tsId, options.getOverrideProtection(),
                startTime, endTime,
                formatBool(options.isStartTimeInclusive()), formatBool(options.isEndTimeInclusive()),
                versionDate, "UTC", pDateTimes, formatBool(options.getMaxVersion()),
                pTsItemMask, officeId);
        });
    }



    public enum OverrideProtection {
        /**
         * If set to True, all specified values are quietly deleted.
         */
        True,
        /**
         * If set to False, only non-protected values are quietly deleted.
         */
        False,
        /**
         * If set to E, all specified values are deleted only if
         * all values are non-protected values. If protected values are present,
         * then no values are deleted and the following error is raised:
         * cwms_err.raise('ERROR', 'One or more values are protected').
         */
        E;

        @Override
        public String toString() {
            return name().substring(0, 1);
        }
    }

    public static class DeleteOptions implements TimeSeriesDeleteOptions {
        private final Date startTime;
        private final Date endTime;
        private final boolean startTimeInclusive;
        private final boolean endTimeInclusive;
        private final Date versionDate;
        private final Boolean maxVersion;
        private final Integer tsItemMask;
        private final String overrideProtection;

        public DeleteOptions(Builder builder) {
            this.startTime = builder.startTime;
            this.endTime = builder.endTime;
            this.startTimeInclusive = builder.startTimeInclusive;
            this.endTimeInclusive = builder.endTimeInclusive;
            this.versionDate = builder.versionDate;
            this.maxVersion = builder.maxVersion;
            this.tsItemMask = builder.tsItemMask;
            this.overrideProtection = builder.overrideProtection;
        }

        @Override
        public Date getStartTime() {
            return startTime;
        }

        @Override
        public Date getEndTime() {
            return endTime;
        }

        @Override
        public boolean isStartTimeInclusive() {
            return startTimeInclusive;
        }

        @Override
        public boolean isEndTimeInclusive() {
            return endTimeInclusive;
        }

        @Override
        public Date getVersionDate() {
            return versionDate;
        }

        @Override
        public Boolean getMaxVersion() {
            return maxVersion;
        }

        @Override
        public Integer getTsItemMask() {
            return tsItemMask;
        }

        @Override
        public String getOverrideProtection() {
            return overrideProtection;
        }

        public static class Builder {
            private Date startTime;
            private Date endTime;
            private boolean startTimeInclusive = true;
            private boolean endTimeInclusive = true;
            private Date versionDate;
            private Boolean maxVersion = null;
            private Integer tsItemMask = -1;
            private String overrideProtection;

            public Builder withStartTime(Date startTime) {
                this.startTime = startTime;
                return this;
            }

            public Builder withEndTime(Date endTime) {
                this.endTime = endTime;
                return this;
            }

            public Builder withStartTimeInclusive(boolean startTimeInclusive) {
                this.startTimeInclusive = startTimeInclusive;
                return this;
            }

            public Builder withEndTimeInclusive(boolean endTimeInclusive) {
                this.endTimeInclusive = endTimeInclusive;
                return this;
            }

            public Builder withVersionDate(Date versionDate) {
                this.versionDate = versionDate;
                return this;
            }


            public Builder withMaxVersion(Boolean maxVersion) {
                this.maxVersion = maxVersion;
                return this;
            }

            public Builder withTsItemMask(Integer tsItemMask) {
                this.tsItemMask = tsItemMask;
                return this;
            }

            public Builder withOverrideProtection(String overrideProtection) {
                this.overrideProtection = overrideProtection;
                return this;
            }


            public DeleteOptions build() {
                return new DeleteOptions(this);
            }
        }
    }

    private static final class DirectReadMetadata {
        private final long tsCode;
        private final String tsId;
        private final String officeId;
        private final String units;
        private final String nativeUnits;
        private final long intervalMinutes;
        private final long intervalUtcOffset;
        private final String timeZoneId;
        private final String versionFlag;

        private DirectReadMetadata(long tsCode, String tsId, String officeId, String units, String nativeUnits,
                                   long intervalMinutes, long intervalUtcOffset,
                                   String timeZoneId, String versionFlag) {
            this.tsCode = tsCode;
            this.tsId = tsId;
            this.officeId = officeId;
            this.units = units;
            this.nativeUnits = nativeUnits;
            this.intervalMinutes = intervalMinutes;
            this.intervalUtcOffset = intervalUtcOffset;
            this.timeZoneId = timeZoneId;
            this.versionFlag = versionFlag;
        }
    }

    private interface FieldMapping {
        Field<BigDecimal> getTsCode();
        Field<BigDecimal> getLocationCode();
        Field<String> getDbOfficeId();
        Field<String> getCwmsTsId();
        Field<String> getUnitId();
        Field<String> getIntervalId();
        Field<BigDecimal> getIntervalUtcOffset();
        Field<String> getTimeZoneId();
        Field<String> getVerionFlag();
        boolean includesAliases();
    }

    private static class CwmsTsIdFieldMapping implements FieldMapping {
        @Override
        public Field<BigDecimal> getTsCode() {
            return AV_CWMS_TS_ID.AV_CWMS_TS_ID.TS_CODE;
        }

        @Override
        public Field<BigDecimal> getLocationCode() {
            return AV_CWMS_TS_ID.AV_CWMS_TS_ID.LOCATION_CODE;
        }

        @Override
        public Field<String> getDbOfficeId() {
            return AV_CWMS_TS_ID.AV_CWMS_TS_ID.DB_OFFICE_ID;
        }

        @Override
        public Field<String> getCwmsTsId() {
            return AV_CWMS_TS_ID.AV_CWMS_TS_ID.CWMS_TS_ID;
        }

        @Override
        public Field<String> getUnitId() {
            return AV_CWMS_TS_ID.AV_CWMS_TS_ID.UNIT_ID;
        }

        @Override
        public Field<String> getIntervalId() {
            return AV_CWMS_TS_ID.AV_CWMS_TS_ID.INTERVAL_ID;
        }

        @Override
        public Field<BigDecimal> getIntervalUtcOffset() {
            return AV_CWMS_TS_ID.AV_CWMS_TS_ID.INTERVAL_UTC_OFFSET;
        }

        @Override
        public Field<String> getTimeZoneId() {
            return AV_CWMS_TS_ID.AV_CWMS_TS_ID.TIME_ZONE_ID;
        }

        @Override
        public boolean includesAliases() {
            return false;
        }

        @Override
        public Field<String> getVerionFlag() {
            return AV_CWMS_TS_ID.AV_CWMS_TS_ID.VERSION_FLAG;
        }
    }

    private static class CwmsTsId2FieldMapping implements FieldMapping {
        @Override
        public Field<BigDecimal> getTsCode() {
            return AV_CWMS_TS_ID2.TS_CODE;
        }

        @Override
        public Field<BigDecimal> getLocationCode() {
            return AV_CWMS_TS_ID2.LOCATION_CODE;
        }

        @Override
        public Field<String> getDbOfficeId() {
            return AV_CWMS_TS_ID2.DB_OFFICE_ID;
        }

        @Override
        public Field<String> getCwmsTsId() {
            return AV_CWMS_TS_ID2.CWMS_TS_ID;
        }

        @Override
        public Field<String> getUnitId() {
            return AV_CWMS_TS_ID2.UNIT_ID;
        }

        @Override
        public Field<String> getIntervalId() {
            return AV_CWMS_TS_ID2.INTERVAL_ID;
        }

        @Override
        public Field<BigDecimal> getIntervalUtcOffset() {
            return AV_CWMS_TS_ID2.INTERVAL_UTC_OFFSET;
        }

        @Override
        public Field<String> getTimeZoneId() {
            return AV_CWMS_TS_ID2.TIME_ZONE_ID;
        }

        @Override
        public boolean includesAliases() {
            return true;
        }

        @Override
        public Field<String> getVerionFlag() {
            return AV_CWMS_TS_ID2.VERSION_FLAG;
        }
    }


}
