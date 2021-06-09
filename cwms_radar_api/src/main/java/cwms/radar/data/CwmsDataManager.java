package cwms.radar.data;

import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import cwms.radar.data.dto.Catalog;
import cwms.radar.data.dto.LocationCategory;
import cwms.radar.data.dto.LocationGroup;
import cwms.radar.data.dto.Office;
import cwms.radar.data.dto.TimeSeriesCategory;
import cwms.radar.data.dto.TimeSeriesGroup;
import cwms.radar.data.dto.catalog.CatalogEntry;
import cwms.radar.data.dto.catalog.TimeseriesCatalogEntry;
import io.javalin.http.Context;
import org.jooq.DSLContext;
import org.jooq.Record1;
import org.jooq.Record3;
import org.jooq.Result;
import org.jooq.SQLDialect;
import org.jooq.SelectJoinStep;
import org.jooq.conf.ParamType;
import org.jooq.impl.DSL;

import usace.cwms.db.jooq.codegen.packages.CWMS_ENV_PACKAGE;
import usace.cwms.db.jooq.codegen.tables.AV_LOC_CAT_GRP;
import usace.cwms.db.jooq.codegen.tables.AV_TS_CAT_GRP;

import static org.jooq.impl.DSL.asterisk;
import static org.jooq.impl.DSL.count;
import static usace.cwms.db.jooq.codegen.tables.AV_CWMS_TS_ID2.AV_CWMS_TS_ID2;


public class CwmsDataManager implements AutoCloseable {
    private static final Logger logger = Logger.getLogger("CwmsDataManager");
    public static final String ALL_OFFICES_QUERY = "select office_id,long_name,office_type,report_to_office_id from cwms_20.av_office";
    public static final String SINGLE_OFFICE = "select office_id,long_name,office_type,report_to_office_id from cwms_20.av_office where office_id=?";
    public static final String ALL_LOCATIONS_QUERY = "select cwms_loc.retrieve_locations_f(?,?,?,?,?) from dual";
    public static final String ALL_RATINGS_QUERY = "select cwms_rating.retrieve_ratings_f(?,?,?,?,?,?,?,?) from dual";                                                               
    public static final String ALL_UNITS_QUERY = "select cwms_cat.retrieve_units_f(?) from dual";
    private static final String ALL_PARAMETERS_QUERY = "select cwms_cat.retrieve_parameters_f(?) from dual";
    private static final String ALL_TIMEZONES_QUERY = "select cwms_cat.retrieve_time_zones_f(?) from dual";
    private static final String ALL_LOCATION_LEVELS_QUERY = "select cwms_level.retrieve_location_levels_f(?,?,?,?,?,?,?,?) from dual";
    private static final String ALL_TIMESERIES_QUERY = "select cwms_ts.retrieve_time_series_f(?,?,?,?,?,?,?,?) from dual";
    public static final String FAILED = "Failed to process database request";

    private Connection conn;
    private DSLContext dsl = null;

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

    private static String extractClobString(ResultSet rs) throws SQLException
    {
        return extractClobString(rs, 1);
    }

    private static String extractClobString(ResultSet rs, int columnIndex) throws SQLException
    {
        String retval = null;
        if(rs != null && rs.next()){
            retval = getAndFree(rs.getClob(columnIndex));
        }
        return retval;
    }

    private static String getAndFree(Clob clob) throws SQLException
    {
        String retval = null;
        if(clob != null)
        {
            try
            {
                retval = clob.getSubString(1L, (int) clob.length());
            }
            finally
            {
                clob.free();
            }
        }
        return retval;
    }

    public String getLocations(String names,String format, String units, String datum, String officeId) {
        try( PreparedStatement stmt = conn.prepareStatement(ALL_LOCATIONS_QUERY) ) {
            stmt.setString(1,names);
            stmt.setString(2,format);
            stmt.setString(3,units);
            stmt.setString(4,datum);
            stmt.setString(5,officeId);
            try(ResultSet rs = stmt.executeQuery()){
                return extractClobString(rs);
            }
        }catch (SQLException err) {
            logger.log(Level.WARNING, err.getLocalizedMessage(), err);            
        }
        return null;
    }

    public List<Office> getOffices() throws SQLException {
        try (PreparedStatement stmt = conn.prepareStatement(ALL_OFFICES_QUERY); ResultSet rs = stmt.executeQuery()) {
            List<Office> offices = new ArrayList<>();
            while (rs.next()) {
                String name = rs.getString("office_id");
                String longName = rs.getString("long_name");
                String type = rs.getString("office_type");
                String reportsTo = rs.getString("report_to_office_id");

                offices.add(new Office(name, longName, type, reportsTo));
            }
            return offices;
        } catch (SQLException err) {
            logger.log(Level.WARNING, err.getLocalizedMessage(), err);            
        }
        return null;
    }

	public Office getOfficeById(String officeId) {
        
        try(
            PreparedStatement stmt = conn.prepareStatement(SINGLE_OFFICE)
        ) {
            stmt.setString(1, officeId);
            try(
                ResultSet rs = stmt.executeQuery()
            ){ 
                if(rs.next()){
                    String name = rs.getString("office_id");
                    String longName = rs.getString("long_name");
                    String type = rs.getString("office_type");
                    String reportsTo = rs.getString("report_to_office_id");

                    return new Office(name, longName, type, reportsTo);
                }
            }
        } catch( SQLException err ){
            logger.log(Level.WARNING, FAILED,err);
        }
		return null;
	}

	public String getRatings(String names, String format, String unit, String datum, String office, String start,
			String end, String timezone, String size) {
                try(
                    PreparedStatement stmt = conn.prepareStatement(ALL_RATINGS_QUERY)
                ) {
                    stmt.setString(1, names);
                    stmt.setString(2, format);
                    stmt.setString(3, unit);
                    stmt.setString(4, datum);
                    stmt.setString(5, start);
                    stmt.setString(6, end);
                    stmt.setString(7, timezone);
                    stmt.setString(8, office);
                    try(
                        ResultSet rs = stmt.executeQuery()
                    ){
                        return extractClobString(rs);
                    }
                } catch( SQLException err ){
                    logger.warning(FAILED + err.getLocalizedMessage() );
                }
                return null;
	}
	public String getUnits(String format) {
		try (
            PreparedStatement stmt = conn.prepareStatement(ALL_UNITS_QUERY)
        ) {
            stmt.setString(1, format);
            try( ResultSet rs = stmt.executeQuery() ){
                return extractClobString(rs);
            }
        } catch (SQLException err ){
            logger.log(Level.WARNING,"Failed to process database request",err);
        }
        return null;
	}

	public String getParameters(String format) {
		try (
            PreparedStatement stmt = conn.prepareStatement(ALL_PARAMETERS_QUERY)
        ) {
            stmt.setString(1, format);
            try( ResultSet rs = stmt.executeQuery() ){
                return extractClobString(rs);
            }
        } catch (SQLException err ){
            logger.log(Level.WARNING,"Failed to process database request",err);
        }
        return null;
	}

	public String getTimeZones(String format) {
		try (
            PreparedStatement stmt = conn.prepareStatement(ALL_TIMEZONES_QUERY)
        ) {
            stmt.setString(1, format);
            try( ResultSet rs = stmt.executeQuery() ){
                return extractClobString(rs);
            }
        } catch (SQLException err ){
            logger.log(Level.WARNING,"Failed to process database request",err);
        }
        return null;
	}

	

	public String getLocationLevels(String format, String names, String office, String unit, String datum, String begin,
			String end, String timezone) {
        try (
            PreparedStatement stmt = conn.prepareStatement(ALL_LOCATION_LEVELS_QUERY)
        ) {
            stmt.setString(1, names);
            stmt.setString(2, format);
            stmt.setString(3, office);
            stmt.setString(4, unit);
            stmt.setString(5, datum);
            stmt.setString(6, begin);
            stmt.setString(7, end);
            stmt.setString(8,timezone);
            try( ResultSet rs = stmt.executeQuery() ){
                return extractClobString(rs);
            }
        } catch (SQLException err ){
            logger.log(Level.WARNING,"Failed to process database request",err);
        }
        return null;
    }
    
	public String getTimeseries(String format, String names, String office, String units, String datum, String begin,
			String end, String timezone) {                
                try( PreparedStatement stmt = conn.prepareStatement(ALL_TIMESERIES_QUERY) ) {
                    stmt.setString(1,names);
                    stmt.setString(2,format);
                    stmt.setString(3,units);
                    stmt.setString(4,datum);
                    stmt.setString(5,begin);
                    stmt.setString(6,end);
                    stmt.setString(7,timezone);
                    stmt.setString(8,office);            
                    try( ResultSet rs = stmt.executeQuery()){
                        return extractClobString(rs);
                    }
                }catch (SQLException err) {
                    logger.log(Level.WARNING, err.getLocalizedMessage(), err);            
                } 
                return null;
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
        List<LocationCategory> retval =
                dsl.selectDistinct(AV_LOC_CAT_GRP.AV_LOC_CAT_GRP.CAT_DB_OFFICE_ID,
                AV_LOC_CAT_GRP.AV_LOC_CAT_GRP.LOC_CATEGORY_ID,
                        AV_LOC_CAT_GRP.AV_LOC_CAT_GRP.LOC_CATEGORY_DESC)
                .from(AV_LOC_CAT_GRP.AV_LOC_CAT_GRP)
                .fetch().into(LocationCategory.class);

        return retval;
    }

    public List<LocationCategory> getLocationCategories(String officeId)
    {
        List<LocationCategory> retval =
                dsl.select(AV_LOC_CAT_GRP.AV_LOC_CAT_GRP.CAT_DB_OFFICE_ID,
                        AV_LOC_CAT_GRP.AV_LOC_CAT_GRP.LOC_CATEGORY_ID, AV_LOC_CAT_GRP.AV_LOC_CAT_GRP.LOC_CATEGORY_DESC)
                        .from(AV_LOC_CAT_GRP.AV_LOC_CAT_GRP)
                        .where(AV_LOC_CAT_GRP.AV_LOC_CAT_GRP.CAT_DB_OFFICE_ID.eq(officeId))
                        .fetch().into(LocationCategory.class);

        return retval;
    }


    public List<LocationGroup> getLocationGroups(){

        List<LocationGroup> retval = dsl.selectDistinct(AV_LOC_CAT_GRP.AV_LOC_CAT_GRP.CAT_DB_OFFICE_ID,
                AV_LOC_CAT_GRP.AV_LOC_CAT_GRP.LOC_CATEGORY_ID, AV_LOC_CAT_GRP.AV_LOC_CAT_GRP.LOC_CATEGORY_DESC,
                AV_LOC_CAT_GRP.AV_LOC_CAT_GRP.GRP_DB_OFFICE_ID, AV_LOC_CAT_GRP.AV_LOC_CAT_GRP.LOC_GROUP_ID,
                AV_LOC_CAT_GRP.AV_LOC_CAT_GRP.LOC_GROUP_DESC, AV_LOC_CAT_GRP.AV_LOC_CAT_GRP.SHARED_LOC_ALIAS_ID,
                AV_LOC_CAT_GRP.AV_LOC_CAT_GRP.SHARED_REF_LOCATION_ID,
                AV_LOC_CAT_GRP.AV_LOC_CAT_GRP.LOC_GROUP_ATTRIBUTE).from(AV_LOC_CAT_GRP.AV_LOC_CAT_GRP)
                .orderBy(AV_LOC_CAT_GRP.AV_LOC_CAT_GRP.LOC_GROUP_ATTRIBUTE)
                .fetch().into(LocationGroup.class);


        return retval;
    }

    public List<LocationGroup> getLocationGroups(String officeId)
    {
        List<LocationGroup> retval = dsl.selectDistinct(AV_LOC_CAT_GRP.AV_LOC_CAT_GRP.CAT_DB_OFFICE_ID,
                AV_LOC_CAT_GRP.AV_LOC_CAT_GRP.LOC_CATEGORY_ID, AV_LOC_CAT_GRP.AV_LOC_CAT_GRP.LOC_CATEGORY_DESC,
                AV_LOC_CAT_GRP.AV_LOC_CAT_GRP.GRP_DB_OFFICE_ID, AV_LOC_CAT_GRP.AV_LOC_CAT_GRP.LOC_GROUP_ID,
                AV_LOC_CAT_GRP.AV_LOC_CAT_GRP.LOC_GROUP_DESC, AV_LOC_CAT_GRP.AV_LOC_CAT_GRP.SHARED_LOC_ALIAS_ID,
                AV_LOC_CAT_GRP.AV_LOC_CAT_GRP.SHARED_REF_LOCATION_ID,
                AV_LOC_CAT_GRP.AV_LOC_CAT_GRP.LOC_GROUP_ATTRIBUTE).from(AV_LOC_CAT_GRP.AV_LOC_CAT_GRP)
                .where(AV_LOC_CAT_GRP.AV_LOC_CAT_GRP.GRP_DB_OFFICE_ID.eq(officeId))
                .orderBy(AV_LOC_CAT_GRP.AV_LOC_CAT_GRP.LOC_GROUP_ATTRIBUTE)
                .fetch().into(LocationGroup.class);

        return retval;
    }

    public List<TimeSeriesCategory> getTimeSeriesCategories(String officeId)
    {
        List<TimeSeriesCategory> retval =
                dsl.select(AV_TS_CAT_GRP.AV_TS_CAT_GRP.CAT_DB_OFFICE_ID,
                        AV_TS_CAT_GRP.AV_TS_CAT_GRP.TS_CATEGORY_ID, AV_TS_CAT_GRP.AV_TS_CAT_GRP.TS_CATEGORY_DESC)
                        .from(AV_TS_CAT_GRP.AV_TS_CAT_GRP)
                        .where(AV_TS_CAT_GRP.AV_TS_CAT_GRP.CAT_DB_OFFICE_ID.eq(officeId))
                        .fetch().into(TimeSeriesCategory.class);

        return retval;
    }

    public List<TimeSeriesCategory> getTimeSeriesCategories()
    {
        List<TimeSeriesCategory> retval =
                dsl.select(AV_TS_CAT_GRP.AV_TS_CAT_GRP.CAT_DB_OFFICE_ID,
                        AV_TS_CAT_GRP.AV_TS_CAT_GRP.TS_CATEGORY_ID, AV_TS_CAT_GRP.AV_TS_CAT_GRP.TS_CATEGORY_DESC)
                        .from(AV_TS_CAT_GRP.AV_TS_CAT_GRP)
                        .fetch().into(TimeSeriesCategory.class);

        return retval;
    }

    public List<TimeSeriesGroup> getTimeSeriesGroups()
    {
        List<TimeSeriesGroup> retval = dsl.selectDistinct(AV_TS_CAT_GRP.AV_TS_CAT_GRP.CAT_DB_OFFICE_ID,
                AV_TS_CAT_GRP.AV_TS_CAT_GRP.TS_CATEGORY_ID, AV_TS_CAT_GRP.AV_TS_CAT_GRP.TS_CATEGORY_DESC,
                AV_TS_CAT_GRP.AV_TS_CAT_GRP.GRP_DB_OFFICE_ID,
                AV_TS_CAT_GRP.AV_TS_CAT_GRP.TS_GROUP_ID,
                AV_TS_CAT_GRP.AV_TS_CAT_GRP.TS_GROUP_DESC, AV_TS_CAT_GRP.AV_TS_CAT_GRP.SHARED_TS_ALIAS_ID,
                AV_TS_CAT_GRP.AV_TS_CAT_GRP.SHARED_REF_TS_ID).from(AV_TS_CAT_GRP.AV_TS_CAT_GRP)
              //  .orderBy(AV_TS_CAT_GRP.AV_TS_CAT_GRP.TS_GROUP_ATTRIBUTE)
                .fetch().into(TimeSeriesGroup.class);

        return retval;
    }




}
