package cwms.radar.data;

import static org.jooq.impl.DSL.asterisk;
import static org.jooq.impl.DSL.count;
import static usace.cwms.db.jooq.codegen.tables.AV_CWMS_TS_ID2.AV_CWMS_TS_ID2;
import static usace.cwms.db.jooq.codegen.tables.AV_LOC.AV_LOC;
import static usace.cwms.db.jooq.codegen.tables.AV_LOC_ALIAS.AV_LOC_ALIAS;
import static usace.cwms.db.jooq.codegen.tables.AV_LOC_GRP_ASSGN.AV_LOC_GRP_ASSGN;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Record1;
import org.jooq.Record3;
import org.jooq.Record5;
import org.jooq.RecordMapper;
import org.jooq.Result;
import org.jooq.SQL;
import org.jooq.SQLDialect;
import org.jooq.SelectConditionStep;
import org.jooq.SelectJoinStep;
import org.jooq.SelectSelectStep;
import org.jooq.Table;
import org.jooq.conf.ParamType;
import org.jooq.impl.DSL;

import cwms.radar.data.dto.AssignedLocation;
import cwms.radar.data.dto.Catalog;
import cwms.radar.data.dto.LocationCategory;
import cwms.radar.data.dto.LocationGroup;
import cwms.radar.data.dto.Office;
import cwms.radar.data.dto.TimeSeries;
import cwms.radar.data.dto.TimeSeriesCategory;
import cwms.radar.data.dto.TimeSeriesGroup;
import cwms.radar.data.dto.catalog.CatalogEntry;
import cwms.radar.data.dto.catalog.LocationAlias;
import cwms.radar.data.dto.catalog.LocationCatalogEntry;
import cwms.radar.data.dto.catalog.TimeseriesCatalogEntry;
import io.javalin.http.Context;
import kotlin.Pair;
import usace.cwms.db.jooq.codegen.packages.CWMS_CAT_PACKAGE;
import usace.cwms.db.jooq.codegen.packages.CWMS_ENV_PACKAGE;
import usace.cwms.db.jooq.codegen.packages.CWMS_LEVEL_PACKAGE;
import usace.cwms.db.jooq.codegen.packages.CWMS_LOC_PACKAGE;
import usace.cwms.db.jooq.codegen.packages.CWMS_RATING_PACKAGE;
import usace.cwms.db.jooq.codegen.packages.CWMS_ROUNDING_PACKAGE;
import usace.cwms.db.jooq.codegen.packages.CWMS_TS_PACKAGE;
import usace.cwms.db.jooq.codegen.packages.CWMS_UTIL_PACKAGE;
import usace.cwms.db.jooq.codegen.tables.AV_LOC_CAT_GRP;
import usace.cwms.db.jooq.codegen.tables.AV_LOC_GRP_ASSGN;
import usace.cwms.db.jooq.codegen.tables.AV_OFFICE;
import usace.cwms.db.jooq.codegen.tables.AV_TS_CAT_GRP;


public class CwmsDataManager implements AutoCloseable {
    private static final Logger logger = Logger.getLogger("CwmsDataManager");
    public static final String FAILED = "Failed to process database request";

    private Connection conn;
    private DSLContext dsl;

    public CwmsDataManager(Context ctx) throws SQLException{
        this(ctx.attribute("database"), ctx.attribute("office_id"));
    }

    public CwmsDataManager(Connection conn, String officeId) throws SQLException{
        this.conn = conn;
        dsl = DSL.using(conn, SQLDialect.ORACLE11G);
        setOfficeId(officeId);
    }

    private void setOfficeId(String officeId)
    {
        CWMS_ENV_PACKAGE.call_SET_SESSION_OFFICE_ID(dsl.configuration(), officeId);
    }

    @Override
    public void close() throws SQLException {
        conn.close();
    }

    public String getLocations(String names,String format, String units, String datum, String officeId) {
        return CWMS_LOC_PACKAGE.call_RETRIEVE_LOCATIONS_F(dsl.configuration(),
                names, format, units, datum, officeId);
    }

    public List<Office> getOffices() {
        List<Office> retval = null;
        AV_OFFICE view = AV_OFFICE.AV_OFFICE;
        // The .as snippets lets it map directly into the Office ctor fields.
        retval = dsl.select(view.OFFICE_ID.as("name"), view.LONG_NAME, view.OFFICE_TYPE.as("type"),
                view.REPORT_TO_OFFICE_ID.as("reportsTo")).from(view).fetch().into(
                Office.class);

        return retval;
    }

	public Office getOfficeById(String officeId) {
        AV_OFFICE view = AV_OFFICE.AV_OFFICE;
        // The .as snippets lets it map directly into the Office ctor fields.
        return dsl.select(view.OFFICE_ID.as("name"), view.LONG_NAME, view.OFFICE_TYPE.as("type"),
                view.REPORT_TO_OFFICE_ID.as("reportsTo")).from(view).where(view.OFFICE_ID.eq(officeId)).fetchOne().into(
                Office.class);
	}

	public String getRatings(String names, String format, String unit, String datum, String office, String start,
			String end, String timezone, String size) {
        return CWMS_RATING_PACKAGE.call_RETRIEVE_RATINGS_F(dsl.configuration(),
                names, format, unit, datum, start, end, timezone, office);
	}

	public String getUnits(String format) {
        return CWMS_CAT_PACKAGE.call_RETRIEVE_UNITS_F(dsl.configuration(), format);
	}

	public String getParameters(String format){
        return CWMS_CAT_PACKAGE.call_RETRIEVE_PARAMETERS_F(dsl.configuration(), format);
    }

	public String getTimeZones(String format) {
            return CWMS_CAT_PACKAGE.call_RETRIEVE_TIME_ZONES_F(dsl.configuration(), format);
    }

	public String getLocationLevels(String format, String names, String office, String unit, String datum, String begin,
			String end, String timezone) {
        return CWMS_LEVEL_PACKAGE.call_RETRIEVE_LOCATION_LEVELS_F(dsl.configuration(),
                names, format, office,unit,datum, begin, end, timezone);
    }

	public String getTimeseries(String format, String names, String office, String units, String datum, String begin,
			String end, String timezone) {
        return CWMS_TS_PACKAGE.call_RETRIEVE_TIME_SERIES_F(dsl.configuration(),
                names, format, units,datum, begin, end, timezone, office);
	}

    public TimeSeries getTimeseries(String page, int pageSize, String names, String office, String units, String datum, String begin, String end, String timezone) {
        String cursor = null;
        Timestamp tsCursor = null;
        Integer total = null;

        if(begin == null)
            begin = ZonedDateTime.now().minusDays(1).toLocalDateTime().toString();
        if(end == null)
            end = ZonedDateTime.now().toLocalDateTime().toString();

        if(page != null && !page.isEmpty())
        {
            String[] parts = TimeSeries.decodeCursor(page);

            logger.info("Decoded cursor");
            for( String p: parts){
                logger.info(p);
            }

            if(parts.length > 1)
            {
                cursor = parts[0];
                tsCursor = Timestamp.from(Instant.ofEpochMilli(Long.parseLong(parts[0])));

                if(parts.length > 2)
                    total = Integer.parseInt(parts[1]);

                // Use the pageSize from the original cursor, for consistent paging
                pageSize = Integer.parseInt(parts[parts.length - 1]);   // Last item is pageSize
            }
        }

        ZoneId zone = timezone == null ? ZoneOffset.UTC.normalized() : ZoneId.of(timezone);

        // Parse the date time in the best format it can find. Timezone is optional, but use it if it's found.
        TemporalAccessor begin_parsed = DateTimeFormatter.ISO_DATE_TIME.parseBest(begin, ZonedDateTime::from, LocalDateTime::from);
        TemporalAccessor end_parsed = DateTimeFormatter.ISO_DATE_TIME.parseBest(end, ZonedDateTime::from, LocalDateTime::from);

        ZonedDateTime begin_time = begin_parsed instanceof ZonedDateTime ? ZonedDateTime.from(begin_parsed) : LocalDateTime.from(begin_parsed).atZone(zone);
        // If the end time doesn't have a timezone, but begin did, use begin's timezone as end's.
        ZonedDateTime end_time = end_parsed instanceof ZonedDateTime ? ZonedDateTime.from(end_parsed) : LocalDateTime.from(end_parsed).atZone(begin_time.getZone());

        if(timezone == null) {
            if(begin_time.getZone().equals(begin_time.getOffset()))
                throw new IllegalArgumentException("Time cannot contain only an offset without the timezone.");
            // If no timezone was found, get it from begin_time
            zone = begin_time.getZone();
        }

        final String recordCursor = cursor;
        final int recordPageSize = pageSize;

        Field<String> office_id = CWMS_UTIL_PACKAGE.call_GET_DB_OFFICE_ID(office != null ? DSL.val(office) : CWMS_UTIL_PACKAGE.call_USER_OFFICE_ID());
        Field<String> ts_id = CWMS_TS_PACKAGE.call_GET_TS_ID__2(DSL.val(names), office_id);
        Field<BigDecimal> ts_code = CWMS_TS_PACKAGE.call_GET_TS_CODE__2(ts_id, office_id);
        Field<String> unit = units.compareToIgnoreCase("SI") == 0 || units.compareToIgnoreCase("EN") == 0 ?
            CWMS_UTIL_PACKAGE.call_GET_DEFAULT_UNITS(CWMS_TS_PACKAGE.call_GET_BASE_PARAMETER_ID(ts_code), DSL.val(units, String.class)) :
            DSL.val(units, String.class);

        // This code assumes the database timezone is in UTC (per Oracle recommendation)
        // Wrap in table() so JOOQ can parse the result
        @SuppressWarnings("deprecated")
        SQL retrieveTable = DSL.sql("table(" + CWMS_TS_PACKAGE.call_RETRIEVE_TS_OUT_TAB(
            ts_id,
            unit,
            CWMS_UTIL_PACKAGE.call_TO_TIMESTAMP__2(DSL.val(begin_time.toInstant().toEpochMilli())),
            CWMS_UTIL_PACKAGE.call_TO_TIMESTAMP__2(DSL.val(end_time.toInstant().toEpochMilli())),
            DSL.inline("UTC", String.class),    // All times are sent as UTC to the database, regardless of requested timezone.
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            office_id) + ")"
        );

        SelectSelectStep<Record5<String,String,String,BigDecimal,Integer>> metadataQuery = dsl.select(
            ts_id.as("NAME"),
            office_id.as("OFFICE_ID"),
            unit.as("UNITS"),
            CWMS_TS_PACKAGE.call_GET_INTERVAL(ts_id).as("INTERVAL"),
            // If we don't know the total, fetch it from the database (only for first fetch).
            // Total is only an estimate, as it can change if fetching current data, or the timeseries otherwise changes between queries.
            total != null ? DSL.val(total).as("TOTAL") : DSL.selectCount().from(retrieveTable).asField("TOTAL")
        );

        logger.info( metadataQuery.getSQL(ParamType.INLINED));

        TimeSeries timeseries = metadataQuery.fetchOne(tsMetadata ->
                new TimeSeries(recordCursor,
                    recordPageSize,
                    tsMetadata.getValue("TOTAL", Integer.class),
                    tsMetadata.getValue("NAME", String.class),
                    tsMetadata.getValue("OFFICE_ID", String.class),
                    begin_time,
                    end_time,
                    tsMetadata.getValue("UNITS", String.class),
                    Duration.ofMinutes(tsMetadata.get("INTERVAL") == null ? 0 : tsMetadata.getValue("INTERVAL", Long.class)))
        );

        if(pageSize != 0) {
            SelectConditionStep<Record3<Timestamp, Double, BigDecimal>> query = dsl.select(
                DSL.field("DATE_TIME", Timestamp.class).as("DATE_TIME"),
                CWMS_ROUNDING_PACKAGE.call_ROUND_DD_F(DSL.field("VALUE", Double.class), DSL.inline("5567899996"), DSL.inline('T')).as("VALUE"),
                CWMS_TS_PACKAGE.call_NORMALIZE_QUALITY(DSL.nvl(DSL.field("QUALITY_CODE", Integer.class), DSL.inline(5))).as("QUALITY_CODE")
            )
            .from(retrieveTable)
            .where(DSL.field("DATE_TIME", Timestamp.class)
                .greaterOrEqual(CWMS_UTIL_PACKAGE.call_TO_TIMESTAMP__2(
                                    DSL.nvl(DSL.val(tsCursor == null ? null : tsCursor.toInstant().toEpochMilli()),
                                            DSL.val(begin_time.toInstant().toEpochMilli())))))
            .and(DSL.field("DATE_TIME", Timestamp.class)
                .lessOrEqual(CWMS_UTIL_PACKAGE.call_TO_TIMESTAMP__2(DSL.val(end_time.toInstant().toEpochMilli())))
            );

            if(pageSize > 0)
                query.limit(DSL.val(pageSize + 1));

            logger.info( query.getSQL(ParamType.INLINED));

            query.fetchInto(record -> {
                    timeseries.addValue(
                        record.getValue("DATE_TIME", Timestamp.class),
                        record.getValue("VALUE", Double.class),
                        record.getValue("QUALITY_CODE", Integer.class)
                    );
                }
            );
        }
        return timeseries;
    }

    public Catalog getTimeSeriesCatalog(String page, int pageSize, Optional<String> office){
        int total = 0;
        String tsCursor = "*";
        if( page == null || page.isEmpty() ){
            SelectJoinStep<Record1<Integer>> count = dsl.select(count(asterisk())).from(AV_CWMS_TS_ID2);
            if( office.isPresent() ){
                count.where(AV_CWMS_TS_ID2.DB_OFFICE_ID.eq(office.get()));
            }
            total = count.fetchOne().value1().intValue();
        } else {
            logger.info("getting non-default page");
            // get totally from page
            String[] parts = Catalog.decodeCursor(page, "|||");

            logger.info("decoded cursor: " + String.join("|||", parts));
            for( String p: parts){
                logger.info(p);
            }

            if(parts.length > 1) {
                tsCursor = parts[0].split("\\/")[1];
                total = Integer.parseInt(parts[1]);
            }
        }

        SelectJoinStep<Record3<String, String, String>> query = dsl.select(
                                        AV_CWMS_TS_ID2.DB_OFFICE_ID,
                                        AV_CWMS_TS_ID2.CWMS_TS_ID,
                                        AV_CWMS_TS_ID2.UNIT_ID
                                    )
                                .from(AV_CWMS_TS_ID2);

        if( office.isPresent() ){
            query.where(AV_CWMS_TS_ID2.DB_OFFICE_ID.upper().eq(office.get().toUpperCase()))
                 .and(AV_CWMS_TS_ID2.CWMS_TS_ID.upper().greaterThan(tsCursor));
        } else {
            query.where(AV_CWMS_TS_ID2.CWMS_TS_ID.upper().gt(tsCursor));
        }
        query.orderBy(AV_CWMS_TS_ID2.CWMS_TS_ID).limit(pageSize);
        logger.info( query.getSQL(ParamType.INLINED));
        Result<Record3<String, String, String>> result = query.fetch();
        List<? extends CatalogEntry> entries = result.stream()
                //.map( e -> e.into(usace.cwms.db.jooq.codegen.tables.records.AV_CWMS_TIMESERIES_ID2) )
                .map( e -> new TimeseriesCatalogEntry(e.get(AV_CWMS_TS_ID2.DB_OFFICE_ID),
                                                      e.get(AV_CWMS_TS_ID2.CWMS_TS_ID),
                                                      e.get(AV_CWMS_TS_ID2.UNIT_ID) )
                )
                .collect(Collectors.toList());
        Catalog cat = new Catalog(tsCursor,total,pageSize,entries);
        return cat;
    }

    public Catalog getLocationCatalog(String cursor, int pageSize, String unitSystem, Optional<String> office) {
        int total = 0;
        String locCursor = "*";
        if( cursor == null || cursor.isEmpty() ){
            SelectJoinStep<Record1<Integer>> count = dsl.select(count(asterisk())).from(AV_LOC);
            if( office.isPresent() ){
                count.where(AV_LOC.DB_OFFICE_ID.upper().eq(office.get().toUpperCase()));
            }
            total = count.fetchOne().value1().intValue();
        } else {
            logger.info("getting non-default page");
            // get totally from page
            String[] parts = Catalog.decodeCursor(cursor, "|||");

            logger.info("decoded cursor: " + String.join("|||", parts));
            for( String p: parts){
                logger.info(p);
            }

            if(parts.length > 1) {
                locCursor = parts[0].split("\\/")[1];
                total = Integer.parseInt(parts[1]);
            }
        }

        SelectConditionStep<Record1<String>> tmp = dsl.select(AV_LOC.LOCATION_ID)
                               .from(AV_LOC)
                               .where(AV_LOC.LOCATION_ID.greaterThan(locCursor))
                               .and(AV_LOC.UNIT_SYSTEM.eq(unitSystem));
        if( office.isPresent()){
            tmp = tmp.and(AV_LOC.DB_OFFICE_ID.upper().eq(office.get().toUpperCase()));
        }
        Table<?> forLimit = tmp.orderBy(AV_LOC.BASE_LOCATION_ID).limit(pageSize).asTable();
        SelectConditionStep<Record> query = dsl.select(
                                    AV_LOC.asterisk(),
                                    AV_LOC_GRP_ASSGN.asterisk()
                                )
                                .from(AV_LOC)
                                .innerJoin(forLimit).on(forLimit.field(AV_LOC.LOCATION_ID).eq(AV_LOC.LOCATION_ID))
                                .leftJoin(AV_LOC_GRP_ASSGN).on(AV_LOC_GRP_ASSGN.LOCATION_ID.eq(AV_LOC.LOCATION_ID))
                                .where(AV_LOC.UNIT_SYSTEM.eq(unitSystem))
                                .and(AV_LOC.LOCATION_ID.upper().greaterThan(locCursor));

        if( office.isPresent() ){
            query.and(AV_LOC.DB_OFFICE_ID.upper().eq(office.get().toUpperCase()));
        }
        query.orderBy(AV_LOC.LOCATION_ID);
        logger.info( query.getSQL(ParamType.INLINED));
        HashMap<usace.cwms.db.jooq.codegen.tables.records.AV_LOC, ArrayList<usace.cwms.db.jooq.codegen.tables.records.AV_LOC_ALIAS>> theMap = new HashMap<>();
        //Result<?> result = query.fetch();
        query.fetch().forEach( row -> {
            usace.cwms.db.jooq.codegen.tables.records.AV_LOC loc = row.into(AV_LOC);
            if( !theMap.containsKey(loc)){
                theMap.put(loc, new ArrayList<>() );
            }
            usace.cwms.db.jooq.codegen.tables.records.AV_LOC_ALIAS alias = row.into(AV_LOC_ALIAS);
            usace.cwms.db.jooq.codegen.tables.records.AV_LOC_GRP_ASSGN group = row.into(AV_LOC_GRP_ASSGN);
            if( group.getALIAS_ID() != null ){
                theMap.get(loc).add(alias);
            }
        });

        List<? extends CatalogEntry> entries =
        theMap.entrySet().stream().map( e -> {
            logger.info(e.getKey().toString());
            LocationCatalogEntry ce = new LocationCatalogEntry(
                e.getKey().getDB_OFFICE_ID(),
                e.getKey().getLOCATION_ID(),
                e.getKey().getNEAREST_CITY(),
                e.getKey().getPUBLIC_NAME(),
                e.getKey().getLONG_NAME(),
                e.getKey().getDESCRIPTION(),
                e.getKey().getLOCATION_KIND_ID(),
                e.getKey().getLOCATION_TYPE(),
                e.getKey().getTIME_ZONE_NAME(),
                e.getKey().getLATITUDE() != null ? e.getKey().getLATITUDE().doubleValue() : null,
                e.getKey().getLONGITUDE() != null ? e.getKey().getLONGITUDE().doubleValue() : null,
                e.getKey().getPUBLISHED_LATITUDE() != null ? e.getKey().getPUBLISHED_LATITUDE().doubleValue() : null,
                e.getKey().getPUBLISHED_LONGITUDE() != null ? e.getKey().getPUBLISHED_LONGITUDE().doubleValue() : null,
                e.getKey().getHORIZONTAL_DATUM(),
                e.getKey().getELEVATION(),
                e.getKey().getUNIT_ID(),
                e.getKey().getVERTICAL_DATUM(),
                e.getKey().getNATION_ID(),
                e.getKey().getSTATE_INITIAL(),
                e.getKey().getCOUNTY_NAME(),
                e.getKey().getBOUNDING_OFFICE_ID(),
                e.getKey().getMAP_LABEL(),
                e.getKey().getACTIVE_FLAG().equalsIgnoreCase("T") ? true : false,
                e.getValue().stream().map( a -> {
                    return new LocationAlias(a.getCATEGORY_ID()+"-"+a.getGROUP_ID(),a.getALIAS_ID());
                }).collect(Collectors.toList())
            );

            return ce;
        }).collect(Collectors.toList());

        Catalog cat = new Catalog(cursor,total,pageSize,entries);
        return cat;
    }


    public List<LocationCategory> getLocationCategories()
    {
        AV_LOC_CAT_GRP table = AV_LOC_CAT_GRP.AV_LOC_CAT_GRP;

        return dsl.selectDistinct(
                table.CAT_DB_OFFICE_ID,
                table.LOC_CATEGORY_ID,
                table.LOC_CATEGORY_DESC)
        .from(table)
        .fetch().into(LocationCategory.class);
    }

    public List<LocationCategory> getLocationCategories(String officeId)
    {
        if(officeId == null || officeId.isEmpty()){
            return getLocationCategories();
        }
        AV_LOC_CAT_GRP table = AV_LOC_CAT_GRP.AV_LOC_CAT_GRP;

        return dsl.select(table.CAT_DB_OFFICE_ID,
                table.LOC_CATEGORY_ID, table.LOC_CATEGORY_DESC)
                .from(table)
                .where(table.CAT_DB_OFFICE_ID.eq(officeId))
                .fetch().into(LocationCategory.class);
    }

    public LocationCategory getLocationCategory(String officeId, String categoryId)
    {
        AV_LOC_CAT_GRP table = AV_LOC_CAT_GRP.AV_LOC_CAT_GRP;

        return dsl.select(table.CAT_DB_OFFICE_ID,
                table.LOC_CATEGORY_ID, table.LOC_CATEGORY_DESC)
                .from(table)
                .where(table.CAT_DB_OFFICE_ID.eq(officeId).and(table.LOC_CATEGORY_ID.eq(categoryId)))
                .fetchOne().into(LocationCategory.class);
    }


    public List<LocationGroup> getLocationGroups(){
        AV_LOC_CAT_GRP table = AV_LOC_CAT_GRP.AV_LOC_CAT_GRP;

        return dsl.selectDistinct(table.CAT_DB_OFFICE_ID,
                table.LOC_CATEGORY_ID, table.LOC_CATEGORY_DESC,
                table.GRP_DB_OFFICE_ID, table.LOC_GROUP_ID,
                table.LOC_GROUP_DESC, table.SHARED_LOC_ALIAS_ID,
                table.SHARED_REF_LOCATION_ID,
                table.LOC_GROUP_ATTRIBUTE).from(table)
                .orderBy(table.LOC_GROUP_ATTRIBUTE)
                .fetch().into(LocationGroup.class);
    }

    public List<LocationGroup> getLocationGroups(String officeId)
    {
        List<LocationGroup> retval;
        if(officeId == null || officeId.isEmpty()){
            retval = getLocationGroups();
        } else
        {
            AV_LOC_CAT_GRP table = AV_LOC_CAT_GRP.AV_LOC_CAT_GRP;
            retval = dsl.selectDistinct(table.CAT_DB_OFFICE_ID, table.LOC_CATEGORY_ID, table.LOC_CATEGORY_DESC,
                    table.GRP_DB_OFFICE_ID, table.LOC_GROUP_ID, table.LOC_GROUP_DESC, table.SHARED_LOC_ALIAS_ID, table.SHARED_REF_LOCATION_ID,
                    table.LOC_GROUP_ATTRIBUTE)
                    .from(table)
                    .where(table.GRP_DB_OFFICE_ID.eq(officeId))
                    .orderBy(table.LOC_GROUP_ATTRIBUTE).fetch().into(LocationGroup.class);
        }
        return retval;
    }

    public List<TimeSeriesCategory> getTimeSeriesCategories(String officeId)
    {
        AV_TS_CAT_GRP table = AV_TS_CAT_GRP.AV_TS_CAT_GRP;

        return dsl.select(table.CAT_DB_OFFICE_ID,
                table.TS_CATEGORY_ID, table.TS_CATEGORY_DESC)
                .from(table)
                .where(table.CAT_DB_OFFICE_ID.eq(officeId))
                .fetch().into(TimeSeriesCategory.class);
    }

    public List<TimeSeriesCategory> getTimeSeriesCategories()
    {
        AV_TS_CAT_GRP table = AV_TS_CAT_GRP.AV_TS_CAT_GRP;

        return dsl.select(table.CAT_DB_OFFICE_ID,
                table.TS_CATEGORY_ID, table.TS_CATEGORY_DESC)
                .from(table)
                .fetch().into(TimeSeriesCategory.class);
    }

    public List<TimeSeriesGroup> getTimeSeriesGroups()
    {
        AV_TS_CAT_GRP table = AV_TS_CAT_GRP.AV_TS_CAT_GRP;

        return dsl.selectDistinct(table.CAT_DB_OFFICE_ID,
                table.TS_CATEGORY_ID, table.TS_CATEGORY_DESC,
                table.GRP_DB_OFFICE_ID,
                table.TS_GROUP_ID,
                table.TS_GROUP_DESC, table.SHARED_TS_ALIAS_ID,
                table.SHARED_REF_TS_ID).from(table)
              //  .orderBy(AV_TS_CAT_GRP.AV_TS_CAT_GRP.TS_GROUP_ATTRIBUTE)
                .fetch().into(TimeSeriesGroup.class);
    }

    public LocationGroup getLocationGroup(String officeId, String categoryId, String groupId)
    {
        AV_LOC_GRP_ASSGN alga = AV_LOC_GRP_ASSGN.AV_LOC_GRP_ASSGN;
        AV_LOC_CAT_GRP alcg = AV_LOC_CAT_GRP.AV_LOC_CAT_GRP;

        final RecordMapper<Record,
                Pair<LocationGroup, AssignedLocation>> mapper = record17 -> {
            LocationGroup group = buildLocationGroup(record17);
            AssignedLocation loc = buildAssignedLocation(record17);

            return new Pair<>(group, loc);
        };

        List<Pair<LocationGroup, AssignedLocation>> assignments = dsl
                .select(alga.CATEGORY_ID, alga.GROUP_ID,
                alga.LOCATION_CODE, alga.DB_OFFICE_ID, alga.BASE_LOCATION_ID, alga.SUB_LOCATION_ID, alga.LOCATION_ID,
                alga.ALIAS_ID, alga.ATTRIBUTE, alga.REF_LOCATION_ID, alga.SHARED_ALIAS_ID, alga.SHARED_REF_LOCATION_ID,
                        alcg.CAT_DB_OFFICE_ID,
                alcg.LOC_CATEGORY_ID, alcg.LOC_CATEGORY_DESC, alcg.LOC_GROUP_DESC, alcg.LOC_GROUP_ATTRIBUTE)
                .from(alcg)
                .join(alga)
                .on(
                        alcg.LOC_CATEGORY_ID.eq(alga.CATEGORY_ID)
                                .and(
                                        alcg.LOC_GROUP_ID.eq(alga.GROUP_ID)))
                .where(alcg.LOC_CATEGORY_ID.eq(categoryId).and(alcg.LOC_GROUP_ID.eq(groupId)).and(alga.DB_OFFICE_ID.eq(officeId)))
                .orderBy(alga.ATTRIBUTE)
                .fetch(mapper);

        // Might want to verify that all the groups in the list are the same?
        LocationGroup locGroup = assignments.stream()
                .map(g -> g.component1())
                .findFirst().orElse(null);

        if(locGroup != null)
        {
            List<AssignedLocation> assignedLocations = assignments.stream()
                    .map(g -> g.component2())
                    .collect(Collectors.toList());
            locGroup = new LocationGroup(locGroup, assignedLocations);
        }
        return locGroup;
    }

    private AssignedLocation buildAssignedLocation(Record resultRecord)
    {
        AV_LOC_GRP_ASSGN alga = AV_LOC_GRP_ASSGN.AV_LOC_GRP_ASSGN;

        String locationId = resultRecord.get(alga.LOCATION_ID);
        String baseLocationId = resultRecord.get(alga.BASE_LOCATION_ID);
        String subLocationId = resultRecord.get(alga.SUB_LOCATION_ID);
        String aliasId = resultRecord.get(alga.ALIAS_ID);
        Number attribute = resultRecord.get(alga.ATTRIBUTE);
        Number locationCode = resultRecord.get(alga.LOCATION_CODE);
        String refLocationId = resultRecord.get(alga.REF_LOCATION_ID);

        return new AssignedLocation(locationId, baseLocationId, subLocationId, aliasId, attribute,
                locationCode, refLocationId);
    }

    private LocationGroup buildLocationGroup(Record resultRecord)
    {
        // This method needs the record to have fields
        // from both AV_LOC_GRP_ASSGN _and_ AV_LOC_CAT_GRP
        AV_LOC_GRP_ASSGN alga = AV_LOC_GRP_ASSGN.AV_LOC_GRP_ASSGN;
        AV_LOC_CAT_GRP alcg = AV_LOC_CAT_GRP.AV_LOC_CAT_GRP;

        String officeId = resultRecord.get(alga.DB_OFFICE_ID);
        String groupId = resultRecord.get(alga.GROUP_ID);
        String sharedAliasId = resultRecord.get(alga.SHARED_ALIAS_ID);
        String sharedRefLocationId = resultRecord.get(alga.SHARED_REF_LOCATION_ID);

        String grpDesc = resultRecord.get(alcg.LOC_GROUP_DESC);
        Number grpAttribute = resultRecord.get(alcg.LOC_GROUP_ATTRIBUTE);

        LocationCategory locationCategory = buildLocationCategory(resultRecord);

        return new LocationGroup(
                locationCategory,
                officeId, groupId, grpDesc,
                sharedAliasId, sharedRefLocationId, grpAttribute);
    }

    private LocationCategory buildLocationCategory(Record resultRecord)
    {
        AV_LOC_CAT_GRP alcg = AV_LOC_CAT_GRP.AV_LOC_CAT_GRP;

        String categoryId = resultRecord.get(alcg.LOC_CATEGORY_ID);
        String catDesc = resultRecord.get(alcg.LOC_CATEGORY_DESC);
        String catDbOfficeId = resultRecord.get(alcg.CAT_DB_OFFICE_ID);
        return new LocationCategory(catDbOfficeId, categoryId, catDesc);
    }


}
