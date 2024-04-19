package cwms.cda.data.dao;

import static org.jooq.impl.DSL.asterisk;
import static org.jooq.impl.DSL.condition;
import static org.jooq.impl.DSL.count;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.max;
import static org.jooq.impl.DSL.name;
import static org.jooq.impl.DSL.partitionBy;
import static org.jooq.impl.DSL.select;
import static usace.cwms.db.jooq.codegen.tables.AV_CWMS_TS_ID2.AV_CWMS_TS_ID2;
import static usace.cwms.db.jooq.codegen.tables.AV_TS_EXTENTS_UTC.AV_TS_EXTENTS_UTC;

import cwms.cda.api.enums.VersionType;
import cwms.cda.data.dto.Catalog;
import cwms.cda.data.dto.CwmsDTOPaginated;
import cwms.cda.data.dto.RecentValue;
import cwms.cda.data.dto.TimeSeries;
import cwms.cda.data.dto.TimeSeriesExtents;
import cwms.cda.data.dto.Tsv;
import cwms.cda.data.dto.TsvDqu;
import cwms.cda.data.dto.TsvDquId;
import cwms.cda.data.dto.TsvId;
import cwms.cda.data.dto.VerticalDatumInfo;
import cwms.cda.data.dto.catalog.CatalogEntry;
import cwms.cda.data.dto.catalog.TimeseriesCatalogEntry;
import java.io.StringReader;
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
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import org.jetbrains.annotations.NotNull;
import org.jooq.CommonTableExpression;
import org.jooq.Condition;
import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.JoinType;
import org.jooq.Operator;
import org.jooq.Record;
import org.jooq.Record1;
import org.jooq.Record3;
import org.jooq.Record7;
import org.jooq.Result;
import org.jooq.SQL;
import org.jooq.SelectConditionStep;
import org.jooq.SelectHavingStep;
import org.jooq.SelectJoinStep;
import org.jooq.SelectQuery;
import org.jooq.Table;
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
import usace.cwms.db.jooq.codegen.tables.AV_CWMS_TS_ID2;
import usace.cwms.db.jooq.codegen.tables.AV_LOC2;
import usace.cwms.db.jooq.codegen.tables.AV_TSV;
import usace.cwms.db.jooq.codegen.tables.AV_TSV_DQU;
import usace.cwms.db.jooq.codegen.tables.AV_TS_GRP_ASSGN;

public class TimeSeriesDaoImpl extends JooqDao<TimeSeries> implements TimeSeriesDao {
    private static final Logger logger = Logger.getLogger(TimeSeriesDaoImpl.class.getName());

    public static final boolean OVERRIDE_PROTECTION = true;
    public static final int TS_ID_MISSING_CODE = 20001;


    public TimeSeriesDaoImpl(DSLContext dsl) {
        super(dsl);

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


        // Now we're going to call the retrieve_ts_out_tab function to get the data and build an
        // internal table from it so we can manipulate it further
        // This code assumes the database timezone is in UTC (per Oracle recommendation)
        SQL retrieveSelectData = null;

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

        // Query based on versionDate or query max aggregate
        // to_timestamp will allow null in the next schema release
        if (versionDate != null) {
            retrieveSelectData = DSL.sql(
                    "table(cwms_20.cwms_ts.retrieve_ts_out_tab(?,?,cwms_20.cwms_util.to_timestamp(?),cwms_20.cwms_util.to_timestamp(?),"
                            + "'UTC',?,?,?,?,?,cwms_20.cwms_util.to_timestamp(?),?,?) ) retrieveTs",
                    tsId, unit, beginTimeMilli, endTimeMilli,
                    trim, startInclusive, endInclusive, previous, next, versionDateMilli, maxVersion, officeId);
        } else {
            retrieveSelectData = DSL.sql(
                    "table(cwms_20.cwms_ts.retrieve_ts_out_tab(?,?,cwms_20.cwms_util.to_timestamp(?),cwms_20.cwms_util.to_timestamp(?),"
                            + "'UTC',?,?,?,?,?,?,?,?) ) retrieveTs",
                    tsId, unit, beginTimeMilli, endTimeMilli,
                    trim, startInclusive, endInclusive, previous, next, versionDateMilli, maxVersion,officeId);
}

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

            SelectJoinStep<Record3<Timestamp, Double, Integer>> retrieveSelectCount = null;

            // Query based on versionDate or query max aggregate
            // to_timestamp will allow null in the next schema release
            if(versionDate != null) {
                retrieveSelectCount = select(
                        dateTimeCol, valueCol, qualityCol
                ).from(DSL.sql(
                        "table(cwms_20.cwms_ts.retrieve_ts_out_tab(?,?,cwms_20.cwms_util.to_timestamp(?),cwms_20.cwms_util.to_timestamp(?),"
                                + "'UTC',?,?,?,?,?,cwms_20.cwms_util.to_timestamp(?),?,?) ) retrieveTsTotal",
                        valid.field("tsid", String.class),
                        valid.field("units", String.class),
                        beginTimeMilli,
                        endTimeMilli,
                        trim, startInclusive, endInclusive, previous, next, versionDateMilli, maxVersion,
                        valid.field("office_id", String.class)
                ));
            } else {
                retrieveSelectCount = select(
                        dateTimeCol, valueCol, qualityCol
                ).from(DSL.sql(
                        "table(cwms_20.cwms_ts.retrieve_ts_out_tab(?,?,cwms_20.cwms_util.to_timestamp(?),cwms_20.cwms_util.to_timestamp(?),"
                                + "'UTC',?,?,?,?,?,?,?,?) ) retrieveTsTotal",
                        valid.field("tsid", String.class),
                        valid.field("units", String.class),
                        beginTimeMilli,
                        endTimeMilli,
                        trim, startInclusive, endInclusive, previous, next, versionDateMilli, maxVersion,
                        valid.field("office_id", String.class)
                ));
            }

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
                                    .lessOrEqual(CWMS_UTIL_PACKAGE.call_TO_TIMESTAMP__2(DSL.val(endTime.toInstant().toEpochMilli())))
                            );

            if (pageSize > 0) {
                query.limit(DSL.val(pageSize + 1));
            }

            logger.fine(() -> query.getSQL(ParamType.INLINED));

            query.fetchInto(tsRecord -> timeseries.addValue(
                            tsRecord.getValue(dateTimeCol),
                            tsRecord.getValue(valueCol),
                            tsRecord.getValue(qualityNormCol).intValue()
                    )
            );

            retVal = timeseries;
        }

        return retVal;
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

    private static boolean isVersioned(DSLContext dsl, String names, String office) {
        return connectionResult(dsl, connection -> {
            Configuration configuration = getDslContext(connection, office).configuration();
            return OracleTypeMap.parseBool(CWMS_TS_PACKAGE.call_IS_TSID_VERSIONED(configuration,
                    names, office));
        });
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
                JAXBContext jaxbContext = JAXBContext.newInstance(VerticalDatumInfo.class);
                Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
                retVal = (VerticalDatumInfo) unmarshaller.unmarshal(new StringReader(body));
            } catch (JAXBException e) {
                logger.log(Level.WARNING, "Failed to parse:" + body, e);
            }
        }
        return retVal;
    }

    @Override
    public Catalog getTimeSeriesCatalog(String page, int pageSize, String office) {
        return getTimeSeriesCatalog(page, pageSize, office, ".*", null, null, null, null, null);
    }

    @Override
    public Catalog getTimeSeriesCatalog(String page, int pageSize, String office,
                                        String idLike, String locCategoryLike, String locGroupLike,
                                        String tsCategoryLike, String tsGroupLike, String boundingOfficeLike) {
        int total;
        String tsCursor = "*";
        String searchOffice = office;
        String curOffice = null;
        Catalog.CatalogPage catPage = null;

        Condition locJoinCondition = AV_LOC2.AV_LOC2.DB_OFFICE_ID.eq(AV_CWMS_TS_ID2.DB_OFFICE_ID)
                .and(AV_LOC2.AV_LOC2.LOCATION_CODE.eq(AV_CWMS_TS_ID2.LOCATION_CODE.coerce(AV_LOC2.AV_LOC2.LOCATION_CODE)))
                .and(AV_LOC2.AV_LOC2.ALIASED_ITEM.isNull());


        if (page == null || page.isEmpty()) {

            Condition condition = caseInsensitiveLikeRegex(AV_CWMS_TS_ID2.CWMS_TS_ID, idLike)
                    .and(AV_CWMS_TS_ID2.ALIASED_ITEM.isNull());
            if (searchOffice != null) {
                condition = condition.and(DSL.upper(AV_CWMS_TS_ID2.DB_OFFICE_ID).eq(searchOffice.toUpperCase()));
            }

            if (locCategoryLike != null) {
                condition = condition.and(caseInsensitiveLikeRegex(AV_CWMS_TS_ID2.LOC_ALIAS_CATEGORY, locCategoryLike));
            }

            if (locGroupLike != null) {
                condition = condition.and(caseInsensitiveLikeRegex(AV_CWMS_TS_ID2.LOC_ALIAS_GROUP, locGroupLike));
            }

            if (tsCategoryLike != null) {
                condition = condition.and(caseInsensitiveLikeRegex(AV_CWMS_TS_ID2.TS_ALIAS_CATEGORY, tsCategoryLike));
            }

            if (tsGroupLike != null) {
                condition = condition.and(caseInsensitiveLikeRegex(AV_CWMS_TS_ID2.TS_ALIAS_GROUP, tsGroupLike));
            }

            SelectJoinStep<Record1<Integer>> selectCountFrom;
            if (boundingOfficeLike == null) {
                selectCountFrom = dsl.select(count(asterisk())).from(AV_CWMS_TS_ID2);
            } else {
                condition = condition.and(caseInsensitiveLikeRegex(AV_LOC2.AV_LOC2.BOUNDING_OFFICE_ID, boundingOfficeLike));
                condition = condition.and(AV_LOC2.AV_LOC2.UNIT_SYSTEM.eq("EN"));

                selectCountFrom = dsl.select(count(asterisk()))
                        .from(AV_CWMS_TS_ID2)
                        .innerJoin(AV_LOC2.AV_LOC2)
                        .on(locJoinCondition);
            }
            total = selectCountFrom.where(condition).fetchOne().value1();
        } else {
            logger.fine("getting non-default page");
            // Information provided by the page value overrides anything provided
            catPage = new Catalog.CatalogPage(page);
            tsCursor = catPage.getCursorId();
            total = catPage.getTotal();
            pageSize = catPage.getPageSize();
            searchOffice = catPage.getSearchOffice();
            curOffice = catPage.getCurOffice();
            idLike = catPage.getIdLike();
            locCategoryLike = catPage.getLocCategoryLike();
            locGroupLike = catPage.getLocGroupLike();
            tsCategoryLike = catPage.getTsCategoryLike();
            tsGroupLike = catPage.getTsGroupLike();
            boundingOfficeLike = catPage.getBoundingOfficeLike();
        }
        SelectQuery<?> primaryDataQuery = dsl.selectQuery();
        primaryDataQuery.addSelect(AV_CWMS_TS_ID2.DB_OFFICE_ID);
        primaryDataQuery.addSelect(AV_CWMS_TS_ID2.CWMS_TS_ID);
        primaryDataQuery.addSelect(AV_CWMS_TS_ID2.TS_CODE);
        primaryDataQuery.addSelect(AV_CWMS_TS_ID2.UNIT_ID);
        primaryDataQuery.addSelect(AV_CWMS_TS_ID2.INTERVAL_ID);
        primaryDataQuery.addSelect(AV_CWMS_TS_ID2.INTERVAL_UTC_OFFSET);
        if (this.getDbVersion() >= Dao.CWMS_21_1_1) {
            primaryDataQuery.addSelect(AV_CWMS_TS_ID2.TIME_ZONE_ID);
        }

        if (boundingOfficeLike != null) {
            primaryDataQuery.addFrom(AV_CWMS_TS_ID2
                    .innerJoin(AV_LOC2.AV_LOC2)
                    .on(locJoinCondition));
        } else {
            primaryDataQuery.addFrom(AV_CWMS_TS_ID2);
        }

        primaryDataQuery.addConditions(AV_CWMS_TS_ID2.ALIASED_ITEM.isNull());

        // add the regexp_like clause. reg fd
        primaryDataQuery.addConditions(caseInsensitiveLikeRegex(AV_CWMS_TS_ID2.CWMS_TS_ID, idLike));

        if (searchOffice != null) {
            primaryDataQuery.addConditions(DSL.upper(AV_CWMS_TS_ID2.DB_OFFICE_ID).eq(office.toUpperCase()));
        }

        if (locCategoryLike != null) {
            primaryDataQuery.addConditions(caseInsensitiveLikeRegex(AV_CWMS_TS_ID2.LOC_ALIAS_CATEGORY, locCategoryLike));
        }

        if (locGroupLike != null) {
            primaryDataQuery.addConditions(caseInsensitiveLikeRegex(AV_CWMS_TS_ID2.LOC_ALIAS_GROUP, locGroupLike));
        }

        if (tsCategoryLike != null) {
            primaryDataQuery.addConditions(caseInsensitiveLikeRegex(AV_CWMS_TS_ID2.TS_ALIAS_CATEGORY, tsCategoryLike));
        }

        if (tsGroupLike != null) {
            primaryDataQuery.addConditions(caseInsensitiveLikeRegex(AV_CWMS_TS_ID2.TS_ALIAS_GROUP, tsGroupLike));
        }

        if (boundingOfficeLike != null) {
            primaryDataQuery.addConditions(caseInsensitiveLikeRegex(AV_LOC2.AV_LOC2.BOUNDING_OFFICE_ID, boundingOfficeLike));
        }

        if (curOffice != null) {
            Condition officeEqualCur = DSL.upper(AV_CWMS_TS_ID2.DB_OFFICE_ID).eq(curOffice.toUpperCase());
            Condition curOfficeTsIdGreater = DSL.upper(AV_CWMS_TS_ID2.CWMS_TS_ID).gt(tsCursor);
            Condition officeGreaterThanCur = DSL.upper(AV_CWMS_TS_ID2.DB_OFFICE_ID).gt(curOffice.toUpperCase());
            primaryDataQuery.addConditions(Operator.AND,
                    officeEqualCur.and(curOfficeTsIdGreater).or(officeGreaterThanCur));
        } else {
            primaryDataQuery.addConditions(DSL.upper(AV_CWMS_TS_ID2.CWMS_TS_ID).gt(tsCursor));
        }

        primaryDataQuery.addOrderBy(DSL.upper(AV_CWMS_TS_ID2.DB_OFFICE_ID), DSL.upper(AV_CWMS_TS_ID2.CWMS_TS_ID));
        Table<?> dataTable = primaryDataQuery.asTable("data");
        SelectQuery<?> limitQuery = dsl.selectQuery();
        limitQuery.addSelect(dataTable.fields());
        limitQuery.addFrom(dataTable);
        limitQuery.addConditions(field("rownum").lessOrEqual(pageSize));

        Table<?> limitTable = limitQuery.asTable("limiter");

        SelectQuery<?> overallQuery = dsl.selectQuery();
        overallQuery.addSelect(limitTable.fields());
        overallQuery.addSelect(AV_TS_EXTENTS_UTC.VERSION_TIME);
        overallQuery.addSelect(AV_TS_EXTENTS_UTC.EARLIEST_TIME);
        overallQuery.addSelect(AV_TS_EXTENTS_UTC.LATEST_TIME);
        overallQuery.addSelect(AV_TS_EXTENTS_UTC.LAST_UPDATE);
        overallQuery.addFrom(limitTable);
        overallQuery.addJoin(AV_TS_EXTENTS_UTC, JoinType.LEFT_OUTER_JOIN,
                condition("\"CWMS_20\".\"AV_TS_EXTENTS_UTC\".\"TS_CODE\" = " + field("\"limiter\""
                        + ".\"TS_CODE\"")));

        overallQuery.addOrderBy(limitTable.field("DB_OFFICE_ID").upper(), limitTable.field("CWMS_TS_ID").upper());

        logger.fine(() -> overallQuery.getSQL(ParamType.INLINED));
        Result<?> result = overallQuery.fetch();

        // NOTE: leave as separate, eventually this will include aliases which
        // will at extra rows per TS
        LinkedHashMap<String, TimeseriesCatalogEntry.Builder> tsIdExtentMap = new LinkedHashMap<>();
        result.forEach(row -> {
            String officeTsId = row.get(AV_CWMS_TS_ID2.DB_OFFICE_ID)
                    + "/"
                    + row.get(AV_CWMS_TS_ID2.CWMS_TS_ID);
            if (!tsIdExtentMap.containsKey(officeTsId)) {
                TimeseriesCatalogEntry.Builder builder = new TimeseriesCatalogEntry.Builder()
                        .officeId(row.get(AV_CWMS_TS_ID2.DB_OFFICE_ID))
                        .cwmsTsId(row.get(AV_CWMS_TS_ID2.CWMS_TS_ID))
                        .units(row.get(AV_CWMS_TS_ID2.UNIT_ID))
                        .interval(row.get(AV_CWMS_TS_ID2.INTERVAL_ID))
                        .intervalOffset(row.get(AV_CWMS_TS_ID2.INTERVAL_UTC_OFFSET));
                if (this.getDbVersion() > Dao.CWMS_21_1_1) {
                    builder.timeZone(row.get("TIME_ZONE_ID", String.class));
                }
                tsIdExtentMap.put(officeTsId, builder);
            }

            if (row.get(AV_TS_EXTENTS_UTC.EARLIEST_TIME) != null) {
                //tsIdExtentMap.get(tsId)
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
                total, pageSize, entries, office,
                idLike, locCategoryLike, locGroupLike,
                tsCategoryLike, tsGroupLike);
    }


    // Finds the single most recent TsvDqu within the time window.
    public TsvDqu findMostRecent(String tOfficeId, String tsId, String unit,
                                 Timestamp twoWeeksFromNow, Timestamp twoWeeksAgo) {
        TsvDqu retval = null;

        AV_TSV_DQU view = AV_TSV_DQU.AV_TSV_DQU;

        Condition nestedCondition = view.ALIASED_ITEM.isNull()
                .and(view.VALUE.isNotNull())
                .and(view.CWMS_TS_ID.eq(tsId))
                .and(view.OFFICE_ID.eq(tOfficeId));

        if (twoWeeksFromNow != null) {
            nestedCondition = nestedCondition.and(view.DATE_TIME.lt(twoWeeksFromNow));
        }

        // Is this really optional?
        if (twoWeeksAgo != null) {
            nestedCondition = nestedCondition.and(view.DATE_TIME.gt(twoWeeksAgo));
        }

        String maxFieldName = "MAX_DATE_TIME";
        SelectHavingStep<Record1<Timestamp>> select =
                dsl.select(max(view.DATE_TIME).as(maxFieldName)).from(
                        view).where(nestedCondition).groupBy(view.TS_CODE);

        Record dquRecord = dsl.select(asterisk()).from(view).where(view.DATE_TIME.in(select)).and(
                view.CWMS_TS_ID.eq(tsId)).and(view.OFFICE_ID.eq(tOfficeId)).and(view.UNIT_ID.eq(unit)).and(
                view.VALUE.isNotNull()).and(view.ALIASED_ITEM.isNull()).fetchOne();

        if (dquRecord != null) {
            retval = dquRecord.map(r -> {
                usace.cwms.db.jooq.codegen.tables.records.AV_TSV_DQU dqu = r.into(view);
                return new TsvDqu(
                        new TsvDquId(dqu.getOFFICE_ID(), dqu.getTS_CODE(),
                        dqu.getUNIT_ID(), dqu.getDATE_TIME()),
                        dqu.getCWMS_TS_ID(), dqu.getVERSION_DATE(),
                        dqu.getDATA_ENTRY_DATE(), dqu.getVALUE(), dqu.getQUALITY_CODE(),
                        dqu.getSTART_DATE(), dqu.getEND_DATE());
            });
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

    // Finds single most recent value within the window for each of the tsCodes
    public List<RecentValue> findMostRecentsInRange(List<String> tsIds, Timestamp pastdate,
                                                    Timestamp futuredate) {
        final List<RecentValue> retval = new ArrayList<>();

        if (tsIds != null && !tsIds.isEmpty()) {
            AV_TSV_DQU tsvView = AV_TSV_DQU.AV_TSV_DQU;
            AV_CWMS_TS_ID2 tsView = AV_CWMS_TS_ID2;
            SelectConditionStep<Record> innerSelect
                    = dsl.select(
                            tsvView.asterisk(),
                            max(tsvView.DATE_TIME).over(partitionBy(tsvView.TS_CODE)).as(
                                    "max_date_time"),
                            tsView.CWMS_TS_ID)
                    .from(tsvView.join(tsView).on(tsvView.TS_CODE.eq(tsView.TS_CODE.cast(Long.class))))
                    .where(
                            tsView.CWMS_TS_ID.in(tsIds)
                                    .and(tsvView.VALUE.isNotNull())
                                    .and(tsvView.DATE_TIME.lt(futuredate))
                                    .and(tsvView.DATE_TIME.gt(pastdate))
                                    .and(tsvView.START_DATE.le(futuredate))
                                    .and(tsvView.END_DATE.gt(pastdate)));


            Field[] queryFields = new Field[]{tsView.CWMS_TS_ID, tsvView.OFFICE_ID,
                    tsvView.TS_CODE, tsvView.UNIT_ID, tsvView.DATE_TIME, tsvView.VERSION_DATE,
                    tsvView.DATA_ENTRY_DATE, tsvView.VALUE, tsvView.QUALITY_CODE,
                    tsvView.START_DATE, tsvView.END_DATE,};

            // look them back up by name b/c we are using them on results of innerselect.
            List<Field<Object>> fields = Arrays.stream(queryFields)
                    .map(Field::getName)
                    .map(DSL::field).collect(
                            Collectors.toList());

            // I want to select tsvView.asterisk but we are selecting from an inner select and
            // even though the inner select selects tsvView.asterisk it isn't the same.
            // So we will just select the fields we want.  Unfortunately that means our results
            // won't map into AV_TSV.AV_TSV
            dsl.select(fields)
                    .from(innerSelect)
                    .where(field("DATE_TIME").eq(innerSelect.field("max_date_time")))
                    .forEach(jrecord -> {
                        RecentValue recentValue = buildRecentValue(tsvView, tsView, jrecord);
                        retval.add(recentValue);
                    });
        }
        return retval;
    }

    @NotNull
    private RecentValue buildRecentValue(AV_TSV_DQU tsvView,
                                         usace.cwms.db.jooq.codegen.tables.AV_CWMS_TS_ID2 tsView,
                                         Record jrecord) {
        return buildRecentValue(tsvView, jrecord, tsView.CWMS_TS_ID.getName());
    }

    @NotNull
    private RecentValue buildRecentValue(AV_TSV_DQU tsvView, AV_TS_GRP_ASSGN tsView,
                                         Record jrecord) {
        return buildRecentValue(tsvView, jrecord, tsView.TS_ID.getName());
    }

    @NotNull
    private RecentValue buildRecentValue(AV_TSV_DQU tsvView, Record jrecord, String tsColumnName) {
        Timestamp dataEntryDate;
        // TODO:
        // !!! skipping DATA_ENTRY_DATE for now.  Need to figure out how to fix mapping in jooq.
        // !! dataEntryDate= jrecord.getValue("data_entry_date", Timestamp.class); // maps to
        // oracle.sql.TIMESTAMP
        // !!!
        dataEntryDate = null;
        // !!!

        TsvDqu tsv = buildTsvDqu(tsvView, jrecord, dataEntryDate);
        String tsId = jrecord.getValue(tsColumnName, String.class);
        return new RecentValue(tsId, tsv);
    }

    @NotNull
    private TsvDqu buildTsvDqu(AV_TSV_DQU tsvView, Record jrecord, Timestamp dataEntryDate) {
        TsvDquId id = buildDquId(tsvView, jrecord);

        return new TsvDqu(id, jrecord.getValue(tsvView.CWMS_TS_ID.getName(), String.class),
                jrecord.getValue(tsvView.VERSION_DATE.getName(), Timestamp.class), dataEntryDate,
                jrecord.getValue(tsvView.VALUE.getName(), Double.class),
                jrecord.getValue(tsvView.QUALITY_CODE.getName(), Long.class),
                jrecord.getValue(tsvView.START_DATE.getName(), Timestamp.class),
                jrecord.getValue(tsvView.END_DATE.getName(), Timestamp.class));
    }


    public List<RecentValue> findRecentsInRange(String office, String categoryId, String groupId,
                                                Timestamp pastLimit, Timestamp futureLimit) {
        List<RecentValue> retVal = new ArrayList<>();

        if (categoryId != null && groupId != null) {
            AV_TSV_DQU tsvView = AV_TSV_DQU.AV_TSV_DQU;  // should we look at the daterange and
            // possible use 30D view?

            AV_TS_GRP_ASSGN tsView = AV_TS_GRP_ASSGN.AV_TS_GRP_ASSGN;

            SelectConditionStep<Record> innerSelect
                    = dsl.select(tsvView.asterisk(), tsView.TS_ID, tsView.ATTRIBUTE,
                            max(tsvView.DATE_TIME).over(partitionBy(tsvView.TS_CODE)).as(
                                    "max_date_time"), tsView.TS_ID)
                    .from(tsvView.join(tsView).on(tsvView.TS_CODE.eq(tsView.TS_CODE.cast(Long.class))))
                    .where(
                            tsView.DB_OFFICE_ID.eq(office)
                                    .and(tsView.CATEGORY_ID.eq(categoryId))
                                    .and(tsView.GROUP_ID.eq(groupId))
                                    .and(tsvView.VALUE.isNotNull())
                                    .and(tsvView.DATE_TIME.lt(futureLimit))
                                    .and(tsvView.DATE_TIME.gt(pastLimit))
                                    .and(tsvView.START_DATE.le(futureLimit))
                                    .and(tsvView.END_DATE.gt(pastLimit)));

            Field[] queryFields = new Field[]{tsvView.OFFICE_ID, tsvView.TS_CODE,
                    tsvView.DATE_TIME, tsvView.VERSION_DATE, tsvView.DATA_ENTRY_DATE,
                    tsvView.VALUE, tsvView.QUALITY_CODE, tsvView.START_DATE, tsvView.END_DATE,
                    tsvView.UNIT_ID, tsView.TS_ID, tsView.ATTRIBUTE};

            List<Field<Object>> fields = Arrays.stream(queryFields)
                    .map(Field::getName)
                    .map(DSL::field).collect(
                            Collectors.toList());


            // I want to select tsvView.asterisk but we are selecting from an inner select and
            // even though the inner select selects tsvView.asterisk it isn't the same.
            // So we will just select the fields we want.
            // Unfortunately that means our results won't map into AV_TSV.AV_TSV
            dsl.select(fields)
                    .from(innerSelect)
                    .where(field(tsvView.DATE_TIME.getName()).eq(innerSelect.field("max_date_time"
                    )))
                    .orderBy(field(tsView.ATTRIBUTE.getName()))
                    .forEach(jrecord -> {
                        RecentValue recentValue = buildRecentValue(tsvView, tsView, jrecord);
                        retVal.add(recentValue);
                    });
        }
        return retVal;
    }

    @NotNull
    private TsvDquId buildDquId(AV_TSV_DQU tsvView, Record jrecord) {
        return new TsvDquId(jrecord.getValue(tsvView.OFFICE_ID.getName(), String.class),
                jrecord.getValue(tsvView.TS_CODE.getName(), Long.class),
                jrecord.getValue(tsvView.UNIT_ID.getName(), String.class),
                jrecord.getValue(tsvView.DATE_TIME.getName(), Timestamp.class));
    }

    @Override
    public void create(TimeSeries input) {
        create(input, false, StoreRule.REPLACE_ALL, TimeSeriesDaoImpl.OVERRIDE_PROTECTION);
    }

    /**
     * Create and save, or update existing Timeseries.
     *
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
