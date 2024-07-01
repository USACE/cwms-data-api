package cwms.cda.data.dao;

import static org.jooq.impl.DSL.asterisk;
import static org.jooq.impl.DSL.countDistinct;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.max;
import static org.jooq.impl.DSL.name;
import static org.jooq.impl.DSL.partitionBy;
import static org.jooq.impl.DSL.select;
import static org.jooq.impl.DSL.selectDistinct;
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
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import cwms.cda.formatters.FormattingException;
import cwms.cda.formatters.xml.XMLv1;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jooq.CommonTableExpression;
import org.jooq.Condition;
import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Record1;
import org.jooq.Record3;
import org.jooq.Record7;
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
import org.jooq.WindowOrderByStep;
import org.jooq.WindowSpecification;
import org.jooq.WindowSpecificationRowsStep;
import org.jooq.conf.ParamType;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import usace.cwms.db.dao.ifc.ts.CwmsDbTs;
import usace.cwms.db.dao.util.OracleTypeMap;
import usace.cwms.db.dao.util.services.CwmsDbServiceLookup;
import usace.cwms.db.jooq.codegen.packages.CWMS_LOC_PACKAGE;
import usace.cwms.db.jooq.codegen.packages.CWMS_TS_PACKAGE;
import usace.cwms.db.jooq.codegen.packages.CWMS_UTIL_PACKAGE;
import usace.cwms.db.jooq.codegen.tables.AV_CWMS_TS_ID;
import usace.cwms.db.jooq.codegen.tables.AV_LOC;
import usace.cwms.db.jooq.codegen.tables.AV_LOC_GRP_ASSGN;
import usace.cwms.db.jooq.codegen.tables.AV_TSV;
import usace.cwms.db.jooq.codegen.tables.AV_TSV_DQU;
import usace.cwms.db.jooq.codegen.tables.AV_TS_GRP_ASSGN;

public class TimeSeriesDaoImpl extends JooqDao<TimeSeries> implements TimeSeriesDao {
    private static final Logger logger = Logger.getLogger(TimeSeriesDaoImpl.class.getName());

    public static final boolean OVERRIDE_PROTECTION = true;
    public static final int TS_ID_MISSING_CODE = 20001;
    public static final String MAX_DATE_TIME = "max_date_time";
    public static final String DEFAULT_UNITS = "def_units";
    public static final String PROP_BASE = "cwms.cda.data.dao.ts";

    public static final String VERSIONED_NAME = "isVersioned";

    /** To be able to use a named inner table (otherwise JOOQ creates a random alias which messes
     * with the planner) we need to use fixed names to be able to reference the required columns.
    ) */
    private static final AV_CWMS_TS_ID cwmsTsIdView = AV_CWMS_TS_ID.AV_CWMS_TS_ID;
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


    public TimeSeriesDaoImpl(DSLContext dsl) {
        this(dsl, null);
    }

    public TimeSeriesDaoImpl(DSLContext dsl, @Nullable MetricRegistry metrics) {
        super(dsl);

        if (metrics != null) {
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


    @Override
    public TimeSeries getTimeseries(String page, int pageSize, String names, String office,
                                       String units,
                                       ZonedDateTime beginTime, ZonedDateTime endTime,
                                    ZonedDateTime versionDate, boolean shouldTrim) {
        TimeSeries retVal = null;
        String cursor = null;
        Timestamp tsCursor = null;
        Integer total = null;

        if (page != null && !page.isEmpty()) {
            final String[] parts = CwmsDTOPaginated.decodeCursor(page);

            logger.fine("Decoded cursor");
            logger.finest(() -> {
                StringBuilder sb = new StringBuilder();
                for (String p: parts) {
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
        Field<Timestamp> dateTimeCol = field("DATE_TIME", Timestamp.class).as("DATE_TIME");
        Field<Double> valueCol = field("VALUE", Double.class).as("VALUE");
        Field<Integer> qualityCol = field("QUALITY_CODE", Integer.class).as("QUALITY_CODE");

        Field<BigDecimal> qualityNormCol = CWMS_TS_PACKAGE.call_NORMALIZE_QUALITY(
                DSL.nvl(qualityCol, DSL.inline(5))).as("QUALITY_NORM");

        Long beginTimeMilli = beginTime.toInstant().toEpochMilli();
        Long endTimeMilli = endTime.toInstant().toEpochMilli();
        String trim = OracleTypeMap.formatBool(shouldTrim);
        String startInclusive = "T";
        String endInclusive = "T";
        String previous = "F";
        String next = "F";
        Long versionDateMilli = null;
        String maxVersion = "F";

        if (versionDate != null) {
            versionDateMilli = versionDate.toInstant().toEpochMilli();
        } else {
            maxVersion = "T";
        }

        // Now we're going to call the retrieve_ts_out_tab function to get the data and build an
        // internal table from it so we can manipulate it further
        // This code assumes the database timezone is in UTC (per Oracle recommendation)
        SQL retrieveSelectData = DSL.sql(
                "table(cwms_20.cwms_ts.retrieve_ts_out_tab(?,?,"
                        + "cwms_20.cwms_util.to_timestamp(?), cwms_20.cwms_util.to_timestamp(?), 'UTC',"
                        + "?,?,?,?,?,"
                        + getVersionPart(versionDate) + ",?,?) ) retrieveTs",
                tsId, unit,
                beginTimeMilli, endTimeMilli,  //tz hardcoded
                trim, startInclusive, endInclusive, previous, next,
                versionDateMilli, maxVersion, officeId);

        Field<String> tzName;
        if (this.getDbVersion() >= Dao.CWMS_21_1_1) {
            tzName = AV_CWMS_TS_ID2.TIME_ZONE_ID;
        } else {
            tzName = DSL.inline(null, SQLDataType.VARCHAR);
        }


        Field<Integer> totalField;
        if (total != null) {
            totalField = DSL.val(total).as("TOTAL");
        } else {
            // If we don't know the total, fetch it from the database (only for first fetch).
            // Total is only an estimate, as it can change if fetching current data,
            // or the timeseries otherwise changes between queries.

            SelectJoinStep<Record3<Timestamp, Double, Integer>> retrieveSelectCount = select(
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
            ));

            totalField = DSL.selectCount().from(DSL.table(retrieveSelectCount)).asField("TOTAL");
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

        VersionType finalDateVersionType = getVersionType(dsl, names, office, versionDate != null);
        TimeSeries timeseries = metadataQuery.fetchOne(tsMetadata -> {
            String vert = (String) tsMetadata.getValue("VERTICAL_DATUM");
            VerticalDatumInfo verticalDatumInfo = parseVerticalDatumInfo(vert);

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
        });

        if (pageSize != 0) {
            SelectConditionStep<Record3<Timestamp, Double, BigDecimal>> query =
                    dsl.select(
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
                            );

            if (pageSize > 0) {
                query.limit(DSL.val(pageSize + 1));
            }

            logger.fine(() -> query.getSQL(ParamType.INLINED));

            query.forEach(tsRecord -> timeseries.addValue(
                            tsRecord.getValue(dateTimeCol),
                            tsRecord.getValue(valueCol),
                            tsRecord.getValue(qualityNormCol).intValue()
                    )
            );

            retVal = timeseries;
        }

        return retVal;
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
        return dsl.connectionResult(c -> {
            Configuration config = getDslContext(c, officeId).configuration();
            String locationId = TimeSeriesDaoImpl.parseLocFromTimeSeriesId(tsId);
            return CWMS_LOC_PACKAGE.call_GET_LOCAL_TIMEZONE__2(config, locationId, officeId);
        });
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
            cachedValue = connectionResult(dsl, connection -> {
                Configuration configuration = getDslContext(connection, office).configuration();
                boolean isVersioned =
                        OracleTypeMap.parseBool(CWMS_TS_PACKAGE.call_IS_TSID_VERSIONED(configuration,
                                tsId, office));
                isVersionedCache.put(cacheKey, isVersioned);
                return isVersioned;
            });
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
        if (page == null || page.isEmpty()) {
            CommonTableExpression<?> limiter = buildWithClause(inputParams, buildWhereConditions(inputParams), new ArrayList<Condition>(), pageSize, true);
            SelectJoinStep<Record1<Integer>> totalQuery = dsl.with(limiter)
                    .select(countDistinct(limiter.field(AV_CWMS_TS_ID.AV_CWMS_TS_ID.TS_CODE)))
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

        List<TableField<?,?>> pageEntryFields = new ArrayList<>(getCwmsTsIdFields());
        if (params.isIncludeExtents()) {
            pageEntryFields.addAll(getExtentsFields());
        }

        List<Condition> whereConditions = buildWhereConditions(params);
        List<Condition> pagingConditions = buildPagingConditions(cursorOffice, cursorTsId);
        CommonTableExpression<?> limiter = buildWithClause(params, whereConditions, pagingConditions, pageSize, false);
        Field<BigDecimal> limiterCode = limiter.field(AV_CWMS_TS_ID.AV_CWMS_TS_ID.TS_CODE);
        SelectJoinStep<?> tmpQuery = dsl.with(limiter)
                                        .select(pageEntryFields)
                                        .from(limiter)
                                        .join(AV_CWMS_TS_ID.AV_CWMS_TS_ID).on(limiterCode.eq(AV_CWMS_TS_ID.AV_CWMS_TS_ID.TS_CODE));

        if (params.isIncludeExtents()) {

            tmpQuery = tmpQuery.leftOuterJoin(AV_TS_EXTENTS_UTC)
                                       .on(limiterCode
                                         .eq(AV_TS_EXTENTS_UTC.TS_CODE.coerce(limiterCode)));
        }
        final SelectSeekStep2<?, String, String> overallQuery = tmpQuery.orderBy(AV_CWMS_TS_ID.AV_CWMS_TS_ID.DB_OFFICE_ID, AV_CWMS_TS_ID.AV_CWMS_TS_ID.CWMS_TS_ID);
        logger.info(() -> overallQuery.getSQL(ParamType.INLINED));
        Result<?> result = overallQuery.fetch();

        Map<String, TimeseriesCatalogEntry.Builder> tsIdExtentMap = new LinkedHashMap<>();
        result.forEach(row -> {
            String officeTsId = row.get(AV_CWMS_TS_ID.AV_CWMS_TS_ID.DB_OFFICE_ID)
                    + "/"
                    + row.get(AV_CWMS_TS_ID.AV_CWMS_TS_ID.CWMS_TS_ID);
            if (!tsIdExtentMap.containsKey(officeTsId)) {
                TimeseriesCatalogEntry.Builder builder = new TimeseriesCatalogEntry.Builder()
                        .officeId(row.get(AV_CWMS_TS_ID.AV_CWMS_TS_ID.DB_OFFICE_ID))
                        .cwmsTsId(row.get(AV_CWMS_TS_ID.AV_CWMS_TS_ID.CWMS_TS_ID))
                        .units(row.get(AV_CWMS_TS_ID.AV_CWMS_TS_ID.UNIT_ID))
                        .interval(row.get(AV_CWMS_TS_ID.AV_CWMS_TS_ID.INTERVAL_ID))
                        .intervalOffset(row.get(AV_CWMS_TS_ID.AV_CWMS_TS_ID.INTERVAL_UTC_OFFSET));
                if (this.getDbVersion() > Dao.CWMS_21_1_1) {
                    builder.timeZone(row.get("TIME_ZONE_ID", String.class));
                }
                if (params.isIncludeExtents()) {
                    builder.withExtents(new ArrayList<>());
                }
                tsIdExtentMap.put(officeTsId, builder);
            }

            if (params.isIncludeExtents()) {
                TimeSeriesExtents extents =
                        new TimeSeriesExtents(row.get(AV_TS_EXTENTS_UTC.VERSION_TIME),
                                row.get(AV_TS_EXTENTS_UTC.EARLIEST_TIME),
                                row.get(AV_TS_EXTENTS_UTC.LATEST_TIME),
                                row.get(AV_TS_EXTENTS_UTC.LAST_UPDATE)
                        );
                tsIdExtentMap.get(officeTsId).withExtent(extents);
            }
        });

        List<? extends CatalogEntry> entries = tsIdExtentMap.entrySet().stream()
                .map(e -> e.getValue().build())
                .collect(Collectors.toList());

        return new Catalog(catPage != null ? catPage.toString() : null,
                total, pageSize, entries, params);
    }

    private static @NotNull List<Condition> buildPagingConditions(String cursorOffice, String cursorTsId) {
        List<Condition> pagingConditions = new ArrayList<>();

        // Can't do the rownum thing here b/c we want global ordering, not ordering within the page.
        //pagingConditions.add(DSL.noCondition());

        if (cursorOffice != null) {
            Condition moreInSameOffice = AV_CWMS_TS_ID.AV_CWMS_TS_ID.DB_OFFICE_ID
                    .eq(cursorOffice.toUpperCase())
                    .and(DSL.upper(AV_CWMS_TS_ID.AV_CWMS_TS_ID.CWMS_TS_ID)
                            .greaterThan(cursorTsId));
            Condition nextOffice = AV_CWMS_TS_ID.AV_CWMS_TS_ID.DB_OFFICE_ID
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

    private @NotNull List<TableField<?,?>> getCwmsTsIdFields() {
        List<TableField<?,?>> cwmsTsIdFields = new ArrayList<>();
        cwmsTsIdFields.add(AV_CWMS_TS_ID.AV_CWMS_TS_ID.DB_OFFICE_ID);
        cwmsTsIdFields.add(AV_CWMS_TS_ID.AV_CWMS_TS_ID.CWMS_TS_ID);
        cwmsTsIdFields.add(AV_CWMS_TS_ID.AV_CWMS_TS_ID.UNIT_ID);
        cwmsTsIdFields.add(AV_CWMS_TS_ID.AV_CWMS_TS_ID.INTERVAL_ID);
        cwmsTsIdFields.add(AV_CWMS_TS_ID.AV_CWMS_TS_ID.INTERVAL_UTC_OFFSET);
        if (this.getDbVersion() >= Dao.CWMS_21_1_1) {
            cwmsTsIdFields.add(AV_CWMS_TS_ID.AV_CWMS_TS_ID.TIME_ZONE_ID);
        }
        return cwmsTsIdFields;
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

    private static @NotNull CommonTableExpression<?> buildWithClause(CatalogRequestParameters params, List<Condition> whereConditions, List<Condition> pagingConditions, int pageSize, boolean forCount) {
        TableLike<?> fromTable = AV_CWMS_TS_ID.AV_CWMS_TS_ID;

        TableOnConditionStep<Record> on = null;
        List<Field<?>> selectFields = new ArrayList<>();
        selectFields.add(fromTable.field(cwmsTsIdView.TS_CODE));
        selectFields.add(fromTable.field(cwmsTsIdView.DB_OFFICE_ID));

        selectFields.add(fromTable.field(cwmsTsIdView.CWMS_TS_ID));

        if (params.needs(tsGroupView)) {
            on = AV_CWMS_TS_ID.AV_CWMS_TS_ID
                    .join(tsGroupView)
                    .on(cwmsTsIdView.TS_CODE.eq(tsGroupView.TS_CODE));
            fromTable = on;
        }

        if (params.needs(AV_LOC_GRP_ASSGN.AV_LOC_GRP_ASSGN)) {
            if (on == null) {
                on = AV_CWMS_TS_ID.AV_CWMS_TS_ID
                        .leftJoin(AV_LOC_GRP_ASSGN.AV_LOC_GRP_ASSGN)
                        .on(AV_CWMS_TS_ID.AV_CWMS_TS_ID.LOCATION_CODE
                                .eq(AV_LOC_GRP_ASSGN.AV_LOC_GRP_ASSGN.LOCATION_CODE));
            } else {
                on = on
                        .leftJoin(AV_LOC_GRP_ASSGN.AV_LOC_GRP_ASSGN)
                        .on(AV_CWMS_TS_ID.AV_CWMS_TS_ID.LOCATION_CODE
                                .eq(AV_LOC_GRP_ASSGN.AV_LOC_GRP_ASSGN.LOCATION_CODE));
            }
            fromTable = on;
        }

        if (params.needs(AV_LOC.AV_LOC)) {
            if (on == null) {
                on = AV_CWMS_TS_ID.AV_CWMS_TS_ID
                        .leftJoin(AV_LOC.AV_LOC)
                        .on(AV_LOC.AV_LOC.LOCATION_CODE
                                .eq(AV_CWMS_TS_ID.AV_CWMS_TS_ID.LOCATION_CODE
                                        .coerce(AV_LOC.AV_LOC.LOCATION_CODE)));
            } else {
                on = on
                        .leftJoin(AV_LOC.AV_LOC)
                        .on(AV_LOC.AV_LOC.LOCATION_CODE
                                .eq(AV_CWMS_TS_ID.AV_CWMS_TS_ID.LOCATION_CODE
                                        .coerce(AV_LOC.AV_LOC.LOCATION_CODE)));
            }
            selectFields.add(AV_LOC.AV_LOC.BOUNDING_OFFICE_ID);
            fromTable = on;
        }

        if (params.isExcludeEmpty()) {
            if (on == null) {
                on = AV_CWMS_TS_ID.AV_CWMS_TS_ID
                        .leftJoin(AV_TS_EXTENTS_UTC)
                        .on(AV_CWMS_TS_ID.AV_CWMS_TS_ID.TS_CODE
                                .eq(AV_TS_EXTENTS_UTC.TS_CODE
                                        .coerce(AV_CWMS_TS_ID.AV_CWMS_TS_ID.TS_CODE)));
            } else {
                on = on
                        .leftJoin(AV_TS_EXTENTS_UTC)
                        .on(AV_CWMS_TS_ID.AV_CWMS_TS_ID.TS_CODE
                                .eq(AV_TS_EXTENTS_UTC.TS_CODE
                                        .coerce(AV_CWMS_TS_ID.AV_CWMS_TS_ID.TS_CODE)));
            }
            fromTable = on;
        }

        TableLike<?> innerSelect = selectDistinct(selectFields)
                                     .from(fromTable)
                                     .where(whereConditions).and(DSL.and(pagingConditions))
                                     .orderBy(AV_CWMS_TS_ID.AV_CWMS_TS_ID.DB_OFFICE_ID, AV_CWMS_TS_ID.AV_CWMS_TS_ID.CWMS_TS_ID)
                                     .asTable("limiterInner");
        if (forCount) {
            return name("limiter").as(
                    select(asterisk())
                    .from(innerSelect)
                    .orderBy(innerSelect.field(AV_CWMS_TS_ID.AV_CWMS_TS_ID.DB_OFFICE_ID), innerSelect.field(AV_CWMS_TS_ID.AV_CWMS_TS_ID.CWMS_TS_ID))
                    );
        } else {
            return name("limiter").as(
                    select(asterisk())
                    .from(innerSelect)
                    .where(field("rownum").lessOrEqual(pageSize))
                    .orderBy(innerSelect.field(AV_CWMS_TS_ID.AV_CWMS_TS_ID.DB_OFFICE_ID), innerSelect.field(AV_CWMS_TS_ID.AV_CWMS_TS_ID.CWMS_TS_ID))
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

        if (params.getOffice() != null) {
            retval.add(AV_CWMS_TS_ID.AV_CWMS_TS_ID.DB_OFFICE_ID.eq(params.getOffice().toUpperCase()));
        }

        retval.add(
            caseInsensitiveLikeRegexNullTrue(
                AV_CWMS_TS_ID.AV_CWMS_TS_ID.CWMS_TS_ID,
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
    public List<RecentValue> findMostRecentsInRange(List<String> tsIds, Timestamp pastdate,
                                                    Timestamp futuredate, UnitSystem unitSystem) {
        List<RecentValue> retval = Collections.emptyList();

        if (tsIds != null && !tsIds.isEmpty()) {
            String tsFieldName = "TSVIEW_CWMS_TS_ID";
            Field<String> tsField = AV_CWMS_TS_ID2.CWMS_TS_ID.as(tsFieldName);

            Field<Timestamp> maxDateField = max(AV_TSV_DQU.AV_TSV_DQU.DATE_TIME)
                    .over(partitionBy(AV_TSV_DQU.AV_TSV_DQU.TS_CODE))
                    .as(MAX_DATE_TIME);

            Field<String> defUnitsField = CWMS_UTIL_PACKAGE.call_GET_DEFAULT_UNITS(
                    CWMS_TS_PACKAGE.call_GET_BASE_PARAMETER_ID(AV_TSV_DQU.AV_TSV_DQU.TS_CODE),
                    DSL.val(unitSystem, String.class))
                    .as(DEFAULT_UNITS);

            SelectConditionStep<? extends Record> innerSelect = dsl.select(
                            AV_TSV_DQU.AV_TSV_DQU.OFFICE_ID,
                            AV_TSV_DQU.AV_TSV_DQU.CWMS_TS_ID,
                            AV_TSV_DQU.AV_TSV_DQU.TS_CODE,
                            AV_TSV_DQU.AV_TSV_DQU.UNIT_ID,
                            AV_TSV_DQU.AV_TSV_DQU.DATE_TIME,
                            AV_TSV_DQU.AV_TSV_DQU.VERSION_DATE,
                            AV_TSV_DQU.AV_TSV_DQU.DATA_ENTRY_DATE,
                            AV_TSV_DQU.AV_TSV_DQU.VALUE,
                            AV_TSV_DQU.AV_TSV_DQU.QUALITY_CODE,
                            AV_TSV_DQU.AV_TSV_DQU.START_DATE,
                            AV_TSV_DQU.AV_TSV_DQU.END_DATE,
                            defUnitsField,
                            maxDateField,
                            tsField
                    )
                    .from(AV_TSV_DQU.AV_TSV_DQU.join(AV_CWMS_TS_ID2)
                            .on(AV_TSV_DQU.AV_TSV_DQU.TS_CODE.eq(
                                    AV_CWMS_TS_ID2.TS_CODE.cast(Long.class))))
                    .where(
                            AV_CWMS_TS_ID2.CWMS_TS_ID.in(tsIds)
                                    .and(AV_TSV_DQU.AV_TSV_DQU.VALUE.isNotNull())
                                    .and(AV_TSV_DQU.AV_TSV_DQU.DATE_TIME.lt(futuredate))
                                    .and(AV_TSV_DQU.AV_TSV_DQU.DATE_TIME.gt(pastdate))
                                    .and(AV_TSV_DQU.AV_TSV_DQU.START_DATE.le(futuredate))
                                    .and(AV_TSV_DQU.AV_TSV_DQU.END_DATE.gt(pastdate)));

            // We want to use some of the fields from the innerSelect statement in our WHERE clause
            // Its cleaner if we call them out individually.
            Field<Timestamp> dateTimeField = innerSelect.field(AV_TSV_DQU.AV_TSV_DQU.DATE_TIME);
            Field<String> unitField = innerSelect.field(AV_TSV_DQU.AV_TSV_DQU.UNIT_ID);

            // We want to return fields from the innerSelect.
            // Note: Although they are both fields, jOOQ treats
            //      innerSelect.field(AV_TSV_DQU.AV_TSV_DQU.DATA_ENTRY_DATE)
            //      differently than
            //      AV_TSV_DQU.AV_TSV_DQU.DATA_ENTRY_DATE
            // Using the innerSelect field makes DATA_ENTRY_DATE correctly map to Timestamp
            // and the generated sql refers to columns from the alias_??? table.
            Field[] queryFields = new Field[]{
                    innerSelect.field(AV_TSV_DQU.AV_TSV_DQU.CWMS_TS_ID),
                    innerSelect.field(AV_TSV_DQU.AV_TSV_DQU.OFFICE_ID),
                    innerSelect.field(AV_TSV_DQU.AV_TSV_DQU.TS_CODE),
                    innerSelect.field(AV_TSV_DQU.AV_TSV_DQU.VERSION_DATE),
                    innerSelect.field(AV_TSV_DQU.AV_TSV_DQU.DATA_ENTRY_DATE),
                    innerSelect.field(AV_TSV_DQU.AV_TSV_DQU.VALUE),
                    innerSelect.field(AV_TSV_DQU.AV_TSV_DQU.QUALITY_CODE),
                    innerSelect.field(AV_TSV_DQU.AV_TSV_DQU.START_DATE),
                    innerSelect.field(AV_TSV_DQU.AV_TSV_DQU.END_DATE),
                    unitField,
                    dateTimeField,
                    innerSelect.field(tsField)
            };

            SelectConditionStep<? extends Record> query = dsl.select(queryFields)
                    .from(innerSelect)
                    .where(dateTimeField.eq(maxDateField).and(unitField.eq(defUnitsField)));

            logger.fine(() -> query.getSQL(ParamType.INLINED));
            retval = query.fetch(r -> buildRecentValue(AV_TSV_DQU.AV_TSV_DQU, r, tsFieldName));
        }
        return retval;
    }


    @NotNull
    private RecentValue buildRecentValue(AV_TSV_DQU tsvView, Record jrecord, String tsColumnName) {

        TsvDqu tsv = buildTsvDqu(tsvView, jrecord);
        String tsId = jrecord.getValue(tsColumnName, String.class);
        return new RecentValue(tsId, tsv);
    }

    @NotNull
    private TsvDqu buildTsvDqu(AV_TSV_DQU tsvView, Record jrecord) {
        return new TsvDqu.Builder()
                .withOfficeId(jrecord.getValue(tsvView.OFFICE_ID))
                .withCwmsTsId(jrecord.getValue(tsvView.CWMS_TS_ID))
                .withUnitId(jrecord.getValue(tsvView.UNIT_ID))
                .withDateTime(jrecord.getValue(tsvView.DATE_TIME))
                .withVersionDate(jrecord.getValue(tsvView.VERSION_DATE))
                .withDataEntryDate(jrecord.getValue(tsvView.DATA_ENTRY_DATE))
                .withValue(jrecord.getValue(tsvView.VALUE))
                .withQualityCode(jrecord.getValue(tsvView.QUALITY_CODE))
                .withStartDate(jrecord.getValue(tsvView.START_DATE))
                .withEndDate(jrecord.getValue(tsvView.END_DATE))
                .build();
    }

    @Override
    public List<RecentValue> findRecentsInRange(String office, String categoryId, String groupId,
                                                @NotNull Timestamp pastLimit, @NotNull Timestamp futureLimit,
                                                 @NotNull UnitSystem unitSystem) {
        AV_TSV_DQU tsvView = AV_TSV_DQU.AV_TSV_DQU;  // should we look at the daterange and
        // possible use 30D view?

        Condition whereCondition =
                tsvView.VALUE.isNotNull()
                        .and(tsvView.DATE_TIME.lt(futureLimit))
                        .and(tsvView.DATE_TIME.gt(pastLimit))
                        .and(tsvView.START_DATE.le(futureLimit))
                        .and(tsvView.END_DATE.gt(pastLimit));

        if (office != null) {
            whereCondition = whereCondition.and(AV_TS_GRP_ASSGN.AV_TS_GRP_ASSGN.DB_OFFICE_ID.eq(office));
        }

        if (categoryId != null) {
            whereCondition = whereCondition.and(AV_TS_GRP_ASSGN.AV_TS_GRP_ASSGN.CATEGORY_ID.eq(categoryId));
        }

        if (groupId != null) {
            whereCondition = whereCondition.and(AV_TS_GRP_ASSGN.AV_TS_GRP_ASSGN.GROUP_ID.eq(groupId));
        }

        Field<String> defUnitsField = CWMS_UTIL_PACKAGE.call_GET_DEFAULT_UNITS(
                        CWMS_TS_PACKAGE.call_GET_BASE_PARAMETER_ID(AV_TSV_DQU.AV_TSV_DQU.TS_CODE),
                        DSL.val(unitSystem, String.class))
                .as(DEFAULT_UNITS);
        Field<Timestamp> maxDateTimeField = max(tsvView.DATE_TIME).over(partitionBy(tsvView.TS_CODE))
                .as(MAX_DATE_TIME);

        SelectConditionStep<? extends Record> innerSelect
                = dsl.select(tsvView.OFFICE_ID, tsvView.TS_CODE, tsvView.DATE_TIME,
                        tsvView.VERSION_DATE, tsvView.DATA_ENTRY_DATE, tsvView.VALUE,
                        tsvView.QUALITY_CODE, tsvView.START_DATE, tsvView.END_DATE, tsvView.UNIT_ID,
                        defUnitsField,
                        maxDateTimeField,
                        AV_TS_GRP_ASSGN.AV_TS_GRP_ASSGN.ATTRIBUTE,
                        AV_TS_GRP_ASSGN.AV_TS_GRP_ASSGN.TS_ID)
                .from(tsvView.join(AV_TS_GRP_ASSGN.AV_TS_GRP_ASSGN)
                        .on(tsvView.TS_CODE.eq(AV_TS_GRP_ASSGN.AV_TS_GRP_ASSGN.TS_CODE.cast(Long.class))))
                .where(whereCondition);

        Field<Timestamp> dateTime = innerSelect.field(tsvView.DATE_TIME);
        Field<String> unit = innerSelect.field(tsvView.UNIT_ID);

        Field[] queryFields = new Field[]{
                innerSelect.field(tsvView.OFFICE_ID),
                innerSelect.field(tsvView.TS_CODE),
                innerSelect.field(tsvView.VERSION_DATE),
                innerSelect.field(tsvView.DATA_ENTRY_DATE),
                innerSelect.field(tsvView.VALUE),
                innerSelect.field(tsvView.QUALITY_CODE),
                innerSelect.field(tsvView.START_DATE),
                innerSelect.field(tsvView.END_DATE),
                dateTime,
                unit,
                innerSelect.field(AV_TS_GRP_ASSGN.AV_TS_GRP_ASSGN.TS_ID),
                innerSelect.field(AV_TS_GRP_ASSGN.AV_TS_GRP_ASSGN.ATTRIBUTE)};

        return dsl.select(queryFields)
                .from(innerSelect)
                .where(dateTime.eq(maxDateTimeField).and(defUnitsField.eq(unit)))
                .orderBy(field(AV_TS_GRP_ASSGN.AV_TS_GRP_ASSGN.ATTRIBUTE.getName()))
                .fetch(r -> buildRecentValue(tsvView, r, AV_TS_GRP_ASSGN.AV_TS_GRP_ASSGN.TS_ID.getName()))
                ;
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
        connection(dsl, connection -> {
            int intervalForward = 0;
            int intervalBackward = 0;
            boolean activeFlag = true;
            // the code does not need to be created before hand.
            // do not add a call to create_ts_code
            if (!input.getValues().isEmpty()) {
                Timestamp versionDate = null;
                if (input.getVersionDate() != null) {
                    versionDate = Timestamp.from(input.getVersionDate().toInstant());
                }

                store(connection, input.getOfficeId(), input.getName(), input.getUnits(),
                        versionDate, input.getValues(), createAsLrts, storeRule,
                        overrideProtection);
            }
        });
    }

    @Override
    public void store(TimeSeries timeSeries, Timestamp versionDate) {
        store(timeSeries, false, StoreRule.REPLACE_ALL, TimeSeriesDaoImpl.OVERRIDE_PROTECTION);
    }

    public void store(TimeSeries input, boolean createAsLrts, StoreRule replaceAll, boolean overrideProtection) {
        connection(dsl, connection -> {
            Timestamp versionDate = null;
            if (input.getVersionDate() != null) {
                versionDate = Timestamp.from(input.getVersionDate().toInstant());
            }

            store(connection, input.getOfficeId(), input.getName(), input.getUnits(),
                    versionDate, input.getValues(), createAsLrts, replaceAll, overrideProtection);
        });
    }

    private void store(Connection connection, String officeId, String tsId, String units,
                       Timestamp versionDate, List<TimeSeries.Record> values, boolean createAsLrts,
                       StoreRule storeRule, boolean overrideProtection) throws SQLException {
        setOffice(connection,officeId);
        CwmsDbTs tsDao = CwmsDbServiceLookup.buildCwmsDb(CwmsDbTs.class, connection);

        final int count = values == null ? 0 : values.size();

        final long[] timeArray = new long[count];
        final double[] valueArray = new double[count];
        final int[] qualityArray = new int[count];

        if (values != null && !values.isEmpty()) {
            Iterator<TimeSeries.Record> iter = values.iterator();
            for (int i = 0; iter.hasNext(); i++) {
                TimeSeries.Record value = iter.next();
                timeArray[i] = value.getDateTime().getTime();
                valueArray[i] = value.getValue();
                qualityArray[i] = value.getQualityCode();
            }
        }

        if (versionDate != null) {
            try {
                CWMS_TS_PACKAGE.call_SET_TSID_VERSIONED(getDslContext(connection, officeId).configuration(),
                        tsId, "T", officeId);
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

        tsDao.store(connection, officeId, tsId, units, timeArray, valueArray, qualityArray, count,
                storeRule.getRule(), overrideProtection, versionDate, createAsLrts);

    }

    public void update(TimeSeries input, boolean createAsLrts, StoreRule storeRule,
                       Timestamp versionDate, boolean overrideProtection) throws SQLException {
        String name = input.getName();
        if (!timeseriesExists(name)) {
            throw new SQLException("Cannot update a non-existant Timeseries. Create " + name + " "
                    + "first.");
        }
        connection(dsl, connection -> {
            setOffice(connection,input.getOfficeId());
            store(connection, input.getOfficeId(), name, input.getUnits(), versionDate,
                    input.getValues(), createAsLrts, storeRule, overrideProtection);
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

}
