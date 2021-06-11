package cwms.radar.data;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import cwms.radar.data.dto.AssignedLocation;
import cwms.radar.data.dto.Catalog;
import cwms.radar.data.dto.LocationCategory;
import cwms.radar.data.dto.LocationGroup;
import cwms.radar.data.dto.Office;
import cwms.radar.data.dto.TimeSeriesCategory;
import cwms.radar.data.dto.TimeSeriesGroup;
import cwms.radar.data.dto.catalog.CatalogEntry;
import cwms.radar.data.dto.catalog.TimeseriesCatalogEntry;
import io.javalin.http.Context;
import kotlin.Pair;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Record1;
import org.jooq.Record15;
import org.jooq.Record3;
import org.jooq.RecordMapper;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.jooq.SelectJoinStep;
import org.jooq.conf.ParamType;
import org.jooq.impl.DSL;

import usace.cwms.db.jooq.codegen.packages.CWMS_CAT_PACKAGE;
import usace.cwms.db.jooq.codegen.packages.CWMS_ENV_PACKAGE;
import usace.cwms.db.jooq.codegen.packages.CWMS_LEVEL_PACKAGE;
import usace.cwms.db.jooq.codegen.packages.CWMS_LOC_PACKAGE;
import usace.cwms.db.jooq.codegen.packages.CWMS_RATING_PACKAGE;
import usace.cwms.db.jooq.codegen.packages.CWMS_TS_PACKAGE;
import usace.cwms.db.jooq.codegen.tables.AV_LOC_CAT_GRP;
import usace.cwms.db.jooq.codegen.tables.AV_LOC_GRP_ASSGN;
import usace.cwms.db.jooq.codegen.tables.AV_OFFICE;
import usace.cwms.db.jooq.codegen.tables.AV_TS_CAT_GRP;

import static org.jooq.impl.DSL.asterisk;
import static org.jooq.impl.DSL.count;
import static usace.cwms.db.jooq.codegen.tables.AV_CWMS_TS_ID2.AV_CWMS_TS_ID2;


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
            String cursor = new String( Base64.getDecoder().decode(page) );
            logger.info("decoded cursor: " + cursor);
            String[] parts = cursor.split("\\|\\|\\|");
            for( String p: parts){
                logger.info(p);
            }
            tsCursor = parts[0].split("\\/")[1];
            total = Integer.parseInt(parts[1]);
        }
        
        SelectJoinStep<Record3<String, String, String>> query = dsl.select(
                                    AV_CWMS_TS_ID2.DB_OFFICE_ID,
                                    AV_CWMS_TS_ID2.CWMS_TS_ID,
                                    AV_CWMS_TS_ID2.UNIT_ID)
                                .from(AV_CWMS_TS_ID2);
                                
        if( office.isPresent() ){
            query.where(AV_CWMS_TS_ID2.DB_OFFICE_ID.upper().eq(office.get().toUpperCase()))
                 .and(AV_CWMS_TS_ID2.CWMS_TS_ID.upper().greaterThan(tsCursor));
        } else {
            query.where(AV_CWMS_TS_ID2.CWMS_TS_ID.upper().gt(tsCursor));
        }                    
        query.orderBy(AV_CWMS_TS_ID2.CWMS_TS_ID).limit(pageSize);
        logger.info( query.getSQL(ParamType.INLINED));
        Result<Record3<String,String,String>> result = query.fetch();
        List<? extends CatalogEntry> entries = result.stream()
                .map( e -> new TimeseriesCatalogEntry(e.value1(),e.value2(),e.value3()))
                .collect(Collectors.toList());
        Catalog cat = new Catalog(tsCursor,total,pageSize,entries);
        return cat;
    }

    public Catalog getLocationCatalog(String cursor, int pageSize, Optional<String> office) {
//        int total = 0;
//        String locCursor = "*";
//        if( cursor == null || cursor.isEmpty() ){
//            SelectJoinStep<Record1<Integer>> count = dsl.select(count(asterisk())).from(AV_LOC);
//            if( office.isPresent() ){
//                count.where(AV_LOC.DB_OFFICE_ID.eq(office.get()));
//            }
//            total = count.fetchOne().value1().intValue();
//        } else {
//            logger.info("getting non-default page");
//            // get totally from page
//            String _cursor = new String( Base64.getDecoder().decode(cursor) );
//            logger.info("decoded cursor: " + cursor);
//            String parts[] = _cursor.split("\\|\\|\\|");
//            for( String p: parts){
//                logger.info(p);
//            }
//            locCursor = parts[0].split("\\/")[1];
//            total = Integer.parseInt(parts[1]);
//        }
//        /*
//        Field<?> aliases = dsl.select(
//                                collect(
//                                    //AV_LOC_ALIAS.CATEGORY_ID.concat(",").concat(AV_LOC_ALIAS.ALIAS_ID), String.class
//                                    AV_LOC_ALIAS.ALIAS_ID.as("test"),null//, SQLDataType.VARCHAR
//                                )
//                            ).from(AV_LOC_ALIAS)
//                            .where(AV_LOC_ALIAS.LOCATION_ID.eq(AV_LOC2.LOCATION_ID))
//                            .asField("aliases");*/
//
//        /**
//         * INNER JOIN ( SELECT * FROM A WHERE A.FIELD1='X' ORDER BY A.FIELD2 LIMIT 10) X
//             ON (A.KEYFIELD=X.KEYFIELD)
//         */
//
//        Table<?> forLimit = dsl.select(AV_LOC.LOCATION_ID)
//                               .from(AV_LOC)
//                               .where(AV_LOC.LOCATION_ID.greaterThan(locCursor))
//                               .and(AV_LOC.UNIT_SYSTEM.eq("SI"))
//                               .orderBy(AV_LOC.BASE_LOCATION_ID).limit(pageSize).asTable();
//        SelectConditionStep<Record> query = dsl.select(
//                                    AV_LOC.asterisk(),
//                                    AV_LOC_GRP_ASSGN.asterisk()
//                                )
//                                .from(AV_LOC)
//                                .innerJoin(forLimit).on(forLimit.field(AV_LOC.LOCATION_ID).eq(AV_LOC.LOCATION_ID))
//                                .leftJoin(AV_LOC_GRP_ASSGN).on(AV_LOC_GRP_ASSGN.LOCATION_ID.eq(AV_LOC.LOCATION_ID))
//                                .where(AV_LOC.UNIT_SYSTEM.eq("SI"))
//                                .and(AV_LOC.LOCATION_ID.upper().greaterThan(locCursor));
//
//        if( office.isPresent() ){
//            query.and(AV_LOC.DB_OFFICE_ID.upper().eq(office.get().toUpperCase()));
//        }
//        query.orderBy(AV_LOC.LOCATION_ID);
//        logger.info( query.getSQL(ParamType.INLINED));
//        //Result<?> result = query.fetch();
//        List<? extends CatalogEntry> entries =
//        //Map<AV_LOC2, List<AV_LOC_ALIAS>> collect =
//        query.collect(
//            groupingBy(
//                r -> r.into(AV_LOC),
//                filtering(
//                    r -> r.get(AV_LOC_GRP_ASSGN.ALIAS_ID) != null,
//                    mapping(
//                        r -> r.into(AV_LOC_GRP_ASSGN),
//                        toList()
//                    )
//                )
//            )
//        ).entrySet().stream().map( e -> {
//            logger.info(e.getKey().toString());
//            LocationCatalogEntry ce = new LocationCatalogEntry(
//                e.getKey().getDB_OFFICE_ID(),
//                e.getKey().getLOCATION_ID(),
//                e.getKey().getNEAREST_CITY(),
//                e.getValue().stream().map( a -> {
//                    return new LocationAlias(a.getCATEGORY_ID()+"-"+a.getGROUP_ID(),a.getALIAS_ID());
//                }).collect(Collectors.toList())
//            );
//
//            return ce;
//        }).collect(Collectors.toList());
//
//        Catalog cat = new Catalog(cursor,total,pageSize,entries);
//        return cat;
        return  null;
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

        final RecordMapper<? super Record15<String, String, BigDecimal, String, String, String, String, String, BigDecimal, String, String, String, String, String, BigDecimal>,
                Pair<LocationGroup, AssignedLocation>> mapper = (RecordMapper<Record15<String, String, BigDecimal, String, String, String, String, String, BigDecimal, String, String, String, String, String, BigDecimal>, Pair<LocationGroup, AssignedLocation>>) record15 -> {
            LocationGroup group = buildLocationGroup(record15);
            AssignedLocation loc = buildAssignedLocation(record15);

            return new Pair<>(group, loc);
        };

        List<Pair<LocationGroup, AssignedLocation>> assignments = dsl
                .select(alga.CATEGORY_ID, alga.GROUP_ID,
                alga.LOCATION_CODE, alga.DB_OFFICE_ID, alga.BASE_LOCATION_ID, alga.SUB_LOCATION_ID, alga.LOCATION_ID,
                alga.ALIAS_ID, alga.ATTRIBUTE, alga.REF_LOCATION_ID, alga.SHARED_ALIAS_ID, alga.SHARED_REF_LOCATION_ID,
                alcg.LOC_CATEGORY_DESC, alcg.LOC_GROUP_DESC, alcg.LOC_GROUP_ATTRIBUTE)
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
