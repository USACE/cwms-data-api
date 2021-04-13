package cwms.radar.data;

import java.util.logging.Level;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import cwms.radar.data.dao.Office;
import io.javalin.http.Context;

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
    private static final String CALL_CWMS_ENV_SET_SESSION_OFFICE_ID = "CALL cwms_env.set_session_office_id(?)";

    private Connection conn;

    public CwmsDataManager(Context ctx) throws SQLException{
        conn = ctx.attribute("database");
        try(
            CallableStatement setOffice = conn.prepareCall(CALL_CWMS_ENV_SET_SESSION_OFFICE_ID);
        ){ 
            setOffice.setString(1,ctx.attribute("office_id"));
            setOffice.execute();
        }
    }

    @Override
    public void close() throws SQLException {
        conn.close();
    }

    public String getLocations(String names,String format, String units, String datum, String OfficeId) {        
        try( PreparedStatement stmt = conn.prepareStatement(ALL_LOCATIONS_QUERY); ) {
            stmt.setString(1,names);
            stmt.setString(2,format);
            stmt.setString(3,units);
            stmt.setString(4,datum);
            stmt.setString(5,OfficeId);
            try( ResultSet rs = stmt.executeQuery(); ){
                if(rs.next()){
                    Clob clob = rs.getClob(1);
                    return clob.getSubString(1L, (int)clob.length());
                }                
            }
        }catch (SQLException err) {
            logger.log(Level.WARNING, err.getLocalizedMessage(), err);            
        }
        return null;
    }

    public List<Office> getOffices() throws SQLException {
        try (PreparedStatement stmt = conn.prepareStatement(ALL_OFFICES_QUERY); ResultSet rs = stmt.executeQuery();) {
            List<Office> offices = new ArrayList<>();
            while (rs.next()) {
                Office l = new Office(rs);
                offices.add(l);
            }
            return offices;
        } catch (SQLException err) {
            logger.log(Level.WARNING, err.getLocalizedMessage(), err);            
        }
        return null;
    }

	public Office getOfficeById(String office_id) {
        
        try(
            PreparedStatement stmt = conn.prepareStatement(SINGLE_OFFICE);
        ) {
            stmt.setString(1, office_id);
            try(
                ResultSet rs = stmt.executeQuery();
            ){ 
                if(rs.next()){
                    return new Office(rs);
                }
            }
        } catch( SQLException err ){
            logger.log(Level.WARNING,"Failed to process database request",err);
        }
		return null;
	}

	public String getRatings(String names, String format, String unit, String datum, String office, String start,
			String end, String timezone, String size) {
                try(
                    PreparedStatement stmt = conn.prepareStatement(ALL_RATINGS_QUERY);
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
                        ResultSet rs = stmt.executeQuery();
                    ){ 
                        if(rs.next()){
                            Clob clob = rs.getClob(1);
                            return clob.getSubString(1L, (int)clob.length());
                        }
                    }
                } catch( SQLException err ){
                    logger.warning("Failed to process database request" + err.getLocalizedMessage() );
                }
                return null;
	}
	public String getUnits(String format) {
		try (
            PreparedStatement stmt = conn.prepareStatement(ALL_UNITS_QUERY);
        ) {
            stmt.setString(1, format);
            try( ResultSet rs = stmt.executeQuery() ){
                if( rs.next() ){
                    Clob clob = rs.getClob(1);
                return clob.getSubString(1L, (int)clob.length());
                }                
            }
        } catch (SQLException err ){
            logger.log(Level.WARNING,"Failed to process database request",err);
        }
        return null;
	}

	public String getParameters(String format) {
		try (
            PreparedStatement stmt = conn.prepareStatement(ALL_PARAMETERS_QUERY);
        ) {
            stmt.setString(1, format);
            try( ResultSet rs = stmt.executeQuery() ){
                if( rs.next() ){
                    Clob clob = rs.getClob(1);
                return clob.getSubString(1L, (int)clob.length());
                }                
            }
        } catch (SQLException err ){
            logger.log(Level.WARNING,"Failed to process database request",err);
        }
        return null;
	}

	public String getTimeZones(String format) {
		try (
            PreparedStatement stmt = conn.prepareStatement(ALL_TIMEZONES_QUERY);
        ) {
            stmt.setString(1, format);
            try( ResultSet rs = stmt.executeQuery() ){
                if( rs.next() ){
                    Clob clob = rs.getClob(1);
                return clob.getSubString(1L, (int)clob.length());
                }                
            }
        } catch (SQLException err ){
            logger.log(Level.WARNING,"Failed to process database request",err);
        }
        return null;
	}

	

	public String getLocationLevels(String format, String names, String office, String unit, String datum, String begin,
			String end, String timezone) {
        try (
            PreparedStatement stmt = conn.prepareStatement(ALL_LOCATION_LEVELS_QUERY);
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
                if( rs.next() ){
                    Clob clob = rs.getClob(1);
                return clob.getSubString(1L, (int)clob.length());
                }                
            }
        } catch (SQLException err ){
            logger.log(Level.WARNING,"Failed to process database request",err);
        }
        return null;
    }
    
	public String getTimeseries(String format, String names, String office, String units, String datum, String begin,
			String end, String timezone) {                
                try( PreparedStatement stmt = conn.prepareStatement(ALL_TIMESERIES_QUERY); ) {
                    stmt.setString(1,names);
                    stmt.setString(2,format);
                    stmt.setString(3,units);
                    stmt.setString(4,datum);
                    stmt.setString(5,begin);
                    stmt.setString(6,end);
                    stmt.setString(7,timezone);
                    stmt.setString(8,office);            
                    try( ResultSet rs = stmt.executeQuery();){
                    
                    if(rs.next()){
                        Clob clob = rs.getClob(1);
                        return clob.getSubString(1L, (int)clob.length());
                    }
                }
                }catch (SQLException err) {
                    logger.log(Level.WARNING, err.getLocalizedMessage(), err);            
                } 
                return null;
	}
	
    
}
