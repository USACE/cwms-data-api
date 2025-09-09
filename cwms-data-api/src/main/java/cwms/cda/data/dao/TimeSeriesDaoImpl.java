package cwms.cda.data.dao;


import com.codahale.metrics.Timer;
import cwms.cda.api.Controllers;
import cwms.cda.data.dao.rsql.FieldResolver;
import cwms.cda.data.dao.rsql.MapFieldResolver;
import cwms.cda.data.dao.rsql.RSQLConditionBuilder;
import cwms.cda.data.dto.filteredtimeseries.FilteredTimeSeries;
import cwms.cda.data.dto.catalog.TimeSeriesAlias;
import cwms.cda.helpers.DateUtils;
import java.util.HashSet;
import java.util.Set;
import static org.jooq.impl.DSL.asterisk;
import static org.jooq.impl.DSL.countDistinct;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.max;
import static org.jooq.impl.DSL.name;
import static org.jooq.impl.DSL.noCondition;
import static org.jooq.impl.DSL.partitionBy;
import static org.jooq.impl.DSL.select;
import static org.jooq.impl.DSL.selectDistinct;

import org.jooq.Configuration;
import usace.cwms.db.jooq.codegen.tables.AV_CWMS_TS_ID;
import static org.jooq.impl.DSL.table;
import static usace.cwms.db.jooq.codegen.tables.AV_CWMS_TS_ID2.AV_CWMS_TS_ID2;
import static usace.cwms.db.jooq.codegen.tables.AV_TS_EXTENTS_UTC.AV_TS_EXTENTS_UTC;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheStats;
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
import cwms.cda.formatters.FormattingException;
import cwms.cda.formatters.xml.XMLv1;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jooq.CommonTableExpression;
import org.jooq.Condition;
import org.jooq.Cursor;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Record1;
import org.jooq.Record3;
import org.jooq.Record4;
import org.jooq.Record7;
import org.jooq.Record10;
import org.jooq.Result;
import org.jooq.SQL;
import org.jooq.SelectConditionStep;
import org.jooq.SelectHavingStep;
import org.jooq.SelectJoinStep;
import org.jooq.SelectSeekStep2;
import org.jooq.Table;
import org.jooq.TableField;
import org.jooq.TableLike;
import org.jooq.TableOnConditionStep;
import org.jooq.Select;
import org.jooq.conf.ParamType;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import usace.cwms.db.dao.ifc.ts.CwmsDbTs;
import usace.cwms.db.dao.util.services.CwmsDbServiceLookup;
import usace.cwms.db.jooq.codegen.packages.CWMS_LOC_PACKAGE;
import usace.cwms.db.jooq.codegen.packages.CWMS_TS_PACKAGE;
import usace.cwms.db.jooq.codegen.packages.CWMS_UTIL_PACKAGE;
import usace.cwms.db.jooq.codegen.tables.AV_LOC;
import usace.cwms.db.jooq.codegen.tables.AV_LOC_GRP_ASSGN;
import usace.cwms.db.jooq.codegen.tables.AV_TSV;
import usace.cwms.db.jooq.codegen.tables.AV_TSV_DQU;
import usace.cwms.db.jooq.codegen.tables.AV_TS_GRP_ASSGN;
import usace.cwms.db.jooq.codegen.udt.records.ZTSV_ARRAY;
import usace.cwms.db.jooq.codegen.udt.records.ZTSV_TYPE;

public class TimeSeriesDaoImpl extends JooqDao<TimeSeries> implements TimeSeriesDao {
    private static final Logger logger = Logger.getLogger(TimeSeriesDaoImpl.class.getName());

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

    public static final boolean OVERRIDE_PROTECTION = true;
    public static final int TS_ID_MISSING_CODE = 20001;
    public static final String MAX_DATE_TIME = "max_date_time";
    public static final String DEFAULT_UNITS = "def_units";
    public static final String PROP_BASE = "cwms.cda.data.dao.ts";

    public static final String VERSIONED_NAME = "isVersioned";

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

    private final MetricRegistry metrics;

    private Timer.Context markAndTime(String subject) {
        return Controllers.markAndTime(metrics, getClass().getName(), subject);
    }



    public TimeSeriesDaoImpl(DSLContext dsl, @NotNull MetricRegistry metrics) {
        super(dsl);

        this.metrics = metrics;

        CacheStats stats = isVersionedCache.stats();
        String hrName = MetricRegistry.name(this.getClass().getName(), VERSIONED_NAME, "hit-rate");
        if (metrics.getGauges().get(hrName) == null) {
            MetricRegistry.MetricSupplier<? extends Gauge> hr = () -> (Gauge<Double>) stats::hitRate;
            metrics.gauge(hrName, hr);
        }
        String mrName = MetricRegistry.name(this.getClass().getName(),VERSIONED_NAME, "miss-rate");
        if (metrics.getGauges().get(mrName) == null) {
            MetricRegistry.MetricSupplier<? extends Gauge> mr = () -> (Gauge<Double>) stats::missRate;
            metrics.gauge(mrName, mr);
        }
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
    public FilteredTimeSeries getTimeseries(String page, int pageSize, TimeSeriesRequestParameters requestParameters, FilteredTimeSeriesParameters filterParams){
        TimeSeries ts =  getRequestedTimeSeries(page, pageSize, requestParameters, filterParams);
        FilteredTimeSeries fts = new FilteredTimeSeries(ts, filterParams);
        fts.clearTimeSeriesPagination();  // we are wrapping the ts, it doesn't need to serialize its own page, nextPage etc.
        return fts;
    }

    protected TimeSeries getRequestedTimeSeries(String page, int pageSize, @NotNull TimeSeriesRequestParameters requestParameters,
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

            logger.fine("Decoded cursor");
            logger.finest(() -> {
                StringBuilder sb = new StringBuilder();
                for (String p : parts) {
                    sb.append(p).append("\n");
                }
                return sb.toString();
            });

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
        if(fp != null) {
            Map<String, Field<?>> nameToField = new LinkedHashMap<>();
            nameToField.put("value", valueCol);
            nameToField.put("date_time", dateTimeCol);
            nameToField.put("quality", qualityCol);
            nameToField.put("data_entry_date", dataEntryDate);
            FieldResolver resolver = new MapFieldResolver(nameToField);
            filterConditions = getFilterCondition(fp, resolver);
        }

        Field<Integer> totalField;
        if (total != null) {
            totalField = DSL.val(total).as("TOTAL");
        } else {
            // If we don't know the total, fetch it from the database (only for first fetch).
            // Total is only an estimate, as it can change if fetching current data,
            // or the timeseries otherwise changes between queries.

            SelectConditionStep<Record3<Timestamp, Double, Integer>> retrieveSelectCount = select(
                    dateTimeCol, valueCol, qualityCol
            ).from(DSL.sql(
                    "table(cwms_20.cwms_ts.retrieve_ts_out_tab(?,?,"
                            + "cwms_20.cwms_util.to_timestamp(?),cwms_20.cwms_util.to_timestamp(?),"
                            + "'UTC',?,?,?,?,?," + getVersionPart(versionDate) + ",?,?) ) retrieveTsTotal",
                    valid.field("tsid", String.class),
                    valid.field("units", String.class),
                    beginTimeMilli,
                    endTimeMilli,
                    trim, startInclusive, endInclusive, previous, next, versionDateMilli, maxVersion,
                    valid.field("office_id", String.class)
            ))
                    .where(filterConditions)
                    ;

            totalField = DSL.selectCount().from(table(retrieveSelectCount)).asField("TOTAL");
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
                                DSL.choose(valid.field("parm_part", String.class))
                                        .when(
                                                "ELEV",
                                                CWMS_LOC_PACKAGE.call_GET_VERTICAL_DATUM_INFO_F__2(
                                                        valid.field("loc_part", String.class),
                                                        valid.field("units", String.class),
                                                        valid.field("office_id", String.class)))
                                        .otherwise("")
                                        .as("VERTICAL_DATUM"),
                                totalField,
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

        logger.fine(() -> metadataQuery.getSQL(ParamType.INLINED));


        TimeSeries timeseries = metadataQuery.fetchOne(tsMetadata -> {
            String vert = (String) tsMetadata.getValue("VERTICAL_DATUM");
            VerticalDatumInfo verticalDatumInfo = parseVerticalDatumInfo(vert);
            VersionType finalDateVersionType = getVersionType(dsl, names, office, versionDate != null);
                return new TimeSeries(recordCursor, recordPageSize, tsMetadata.getValue("TOTAL",
                        Integer.class), tsMetadata.getValue("NAME", String.class),
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
        );

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

            if (requestParameters.isIncludeEntryDate()) {
                logger.fine(() -> query2.getSQL(ParamType.INLINED));
                try (Cursor<Record4<Timestamp, Double, BigDecimal, Timestamp>> recCursor = query2.fetchLazy()) {
                    for (Record tsRecord: recCursor) {
                        timeseries.addValue(
                                tsRecord.getValue(dateTimeCol),
                                tsRecord.getValue(valueCol),
                                tsRecord.getValue(qualityNormCol).intValue(),
                                tsRecord.getValue(dataEntryDate));
                    }
                }
            } else {
                logger.fine(() -> query.getSQL(ParamType.INLINED));
                try (Cursor<Record3<Timestamp, Double, BigDecimal>> recCursor = query.fetchLazy()) {
                    for (Record tsRecord: recCursor) {
                        timeseries.addValue(
                                tsRecord.getValue(dateTimeCol),
                                tsRecord.getValue(valueCol),
                                tsRecord.getValue(qualityNormCol).intValue());
                    }
                }
            }
            retVal = timeseries;
        }

        return retVal;
    }

    private void validateEntryDateSupport(boolean includeEntryDate) {
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

    public static String parseLocFromTimeSeriesId(String tsId) {
        String[] parts = tsId.split("\\.");
        return parts[0];
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
            try {
                retVal = new XMLv1().parseContent(body, VerticalDatumInfo.class);
            } catch (FormattingException e) {
                logger.log(Level.WARNING, e, () -> "Failed to parse:" + body);
            }
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
            logger.fine(() -> totalQuery.getSQL(ParamType.INLINED));
            total = totalQuery.fetchOne(0, int.class);
        } else {
            logger.fine("getting non-default page");
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
        SelectJoinStep<?> tmpQuery = dsl.with(limiter)
                                        .select(pageEntryFields)
                                        .from(limiter)
                                        .join(table).on(limiterCode.eq(cwmsTsIdFields.getTsCode()));

        if (params.isIncludeExtents()) {

            tmpQuery = tmpQuery.leftOuterJoin(AV_TS_EXTENTS_UTC)
                                       .on(limiterCode
                                         .eq(AV_TS_EXTENTS_UTC.TS_CODE.coerce(limiterCode)));
        }
        final SelectSeekStep2<?, String, String> overallQuery = tmpQuery
                .orderBy(cwmsTsIdFields.getDbOfficeId(),
                        cwmsTsIdFields.getCwmsTsId());
        logger.fine(() -> overallQuery.getSQL(ParamType.INLINED));
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
                        .intervalOffset(row.get(cwmsTsIdFields.getIntervalUtcOffset()));

                builder.timeZone(row.get("TIME_ZONE_ID", String.class));

                if (params.isIncludeExtents()) {
                    builder.withExtents(new ArrayList<>());
                }
                if(includeAliases) {
                    if(row.get(AV_CWMS_TS_ID2.ALIASED_ITEM) == null) {
                        tsIdExtentMap.put(officeTsId, builder); //only add non-aliases... aliases get added as a node to each entry later
                    }
                } else {
                    tsIdExtentMap.put(officeTsId, builder);
                }

            }
            if(includeAliases) {
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
                if(entryBuilder != null) {
                    entryBuilder.withExtent(extents);
                }
            }
        });

        if(includeAliases) {
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
        if(ip != null) {
            String query = ip.getQuery();
            if(query != null) {
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
        if(isAlias) {
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
        if(cwmsTsIdFields.includesAliases()) {
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
                .and(view.OFFICE_ID.eq(officeId));

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
                .and(view.OFFICE_ID.eq(officeId))
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
                whereCondition = whereCondition.and(AV_CWMS_TS_ID2.DB_OFFICE_ID.eq(office));
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

            logger.fine(() -> query.getSQL(ParamType.INLINED));

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
        Condition whereCondition = DSL.trueCondition();
        if (categoryId != null) {
            whereCondition = whereCondition.and(AV_TS_GRP_ASSGN.AV_TS_GRP_ASSGN.CATEGORY_ID.eq(categoryId));
        }
        if (groupId != null) {
            whereCondition = whereCondition.and(AV_TS_GRP_ASSGN.AV_TS_GRP_ASSGN.GROUP_ID.eq(groupId));
        }
        if (office != null) {
            whereCondition = whereCondition.and(AV_TS_GRP_ASSGN.AV_TS_GRP_ASSGN.DB_OFFICE_ID.eq(office));
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

        logger.fine(() -> query.getSQL(ParamType.INLINED));

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
        create(input, false, StoreRule.REPLACE_ALL, TimeSeriesDaoImpl.OVERRIDE_PROTECTION);
    }

    /**
     * Create and save, or update existing Timeseries.
     * Required attributes of {@link TimeSeries Timeseries} are
     *
     * <ul>
     *  <li>{@link TimeSeries#getName()} ()}  Timeseries Id}</li>
     *  <li>{@link TimeSeries#getOfficeId()}  Office ID}</li>
     *  <li>{@link TimeSeries#getUnits()}  Units}</li>
     *  <li>{@link TimeSeries#getValues()}  values}
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
     *
     */
    @SuppressWarnings("unused")
    public void create(TimeSeries input,
                       boolean createAsLrts, StoreRule storeRule, boolean overrideProtection) {

        final ZTSV_ARRAY tsvArray = buildZtsv(input.getValues()); // Do this before we get the connection

        int intervalForward = 0;
        int intervalBackward = 0;
        boolean activeFlag = true;
        // the code does not need to be created before hand.
        // do not add a call to create_ts_code
        if (!input.getValues().isEmpty()) {
            final Timestamp versionDate;
            if (input.getVersionDate() != null) {
                versionDate = Timestamp.from(input.getVersionDate().toInstant());
            } else {
                versionDate = null;
            }
            connection(dsl, connection -> store(connection, input.getOfficeId(), input.getName(), input.getUnits(),
                    versionDate, tsvArray, createAsLrts, storeRule, overrideProtection));
        }
    }

    @Override
    public void store(TimeSeries timeSeries, Timestamp versionDate) {
        store(timeSeries, false, StoreRule.REPLACE_ALL, TimeSeriesDaoImpl.OVERRIDE_PROTECTION);
    }

    public void store(TimeSeries input, boolean createAsLrts, StoreRule replaceAll, boolean overrideProtection) {
        final ZTSV_ARRAY tsvArray = buildZtsv(input.getValues());
        try(Timer.Context ignored = markAndTime("store")) {
            connection(dsl, connection -> {
                Timestamp versionDate = null;
                if (input.getVersionDate() != null) {
                    versionDate = Timestamp.from(input.getVersionDate().toInstant());
                }

                store(connection, input.getOfficeId(), input.getName(), input.getUnits(),
                        versionDate, tsvArray, createAsLrts, replaceAll, overrideProtection);
            });
        }
    }

    private void store(Connection connection, String officeId, String tsId, String units,
                       Timestamp versionDate, ZTSV_ARRAY tsvArray, boolean createAsLrts,
                       StoreRule storeRule, boolean overrideProtection) {


        Configuration jooqConfig = getDslContext(connection, officeId).configuration();  // this does set_session_office_id
        if (versionDate != null) {
            try {
                CWMS_TS_PACKAGE.call_SET_TSID_VERSIONED(jooqConfig,  tsId, "T", officeId);
            } catch (DataAccessException e) {
                if (e.getCause() instanceof SQLException) {
                    SQLException cause = (SQLException)e.getCause();

                    if (cause.getErrorCode() != TS_ID_MISSING_CODE) {
                        throw e;
                    }
                    // Ignore tsId not found exceptions. tsDao.store() will create tsId if it is not found
                    logger.log(Level.FINER, e, () -> "TS ID: " + tsId + " not found at office: " + officeId);
                } else {
                    throw e;
                }
            }
        }
        CWMS_TS_PACKAGE.call_ZSTORE_TS(jooqConfig,
                                      tsId,
                                      units,
                                      tsvArray,
                                      storeRule.getRule(),
                                      formatBool(overrideProtection),
                                      versionDate,
                                      officeId,
                                      formatBool(createAsLrts));

    }

    @NotNull
    private ZTSV_ARRAY buildZtsv(List<TimeSeries.Record> values) {
        try (Timer.Context ignored = markAndTime("buildZtsv")) {
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

            return tsvArray;
        }
    }

    public void update(TimeSeries input, boolean createAsLrts, StoreRule storeRule,
                       Timestamp versionDate, boolean overrideProtection) throws SQLException {
        String name = input.getName();
        if (!timeseriesExists(name)) {
            throw new SQLException("Cannot update a non-existant Timeseries. Create " + name + " "
                    + "first.");
        }

        final ZTSV_ARRAY tsvArray = buildZtsv(input.getValues());

        connection(dsl, connection -> {
            setOffice(connection,input.getOfficeId());
            store(connection, input.getOfficeId(), name, input.getUnits(), versionDate,
                    tsvArray, createAsLrts, storeRule, overrideProtection);
        });
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
            setOffice(connection,officeId);
            CwmsDbTs tsDao = CwmsDbServiceLookup.buildCwmsDb(CwmsDbTs.class, connection);
            tsDao.deleteTs(connection, officeId, tsId, options.getStartTime(), options.getEndTime(),
                    options.isStartTimeInclusive(), options.isEndTimeInclusive(),
                    options.getVersionDate(), null, options.getMaxVersion(),
                    options.getTsItemMask(), options.getOverrideProtection());
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

    private interface FieldMapping {
        Field<BigDecimal> getTsCode();
        Field<BigDecimal> getLocationCode();
        Field<String> getDbOfficeId();
        Field<String> getCwmsTsId();
        Field<String> getUnitId();
        Field<String> getIntervalId();
        Field<BigDecimal> getIntervalUtcOffset();
        Field<String> getTimeZoneId();
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
    }


}
