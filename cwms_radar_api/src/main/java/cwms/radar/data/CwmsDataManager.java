package cwms.radar.data;

import java.util.logging.Level;
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
    public static final String ALL_OFFICES_QUERY = "select office_id,long_name,office_type,report_to_office_id from cwms_20.av_office"; // TODO:
                                                                                                                                        // put
                                                                                                                                        // a
                                                                                                                                        // where
                                                                                                                                        // clause
                                                                                                                                        // in
                                                                                                                                        // here
                                                                                                                                        // with
                                                                                                                                        // everything
    public static final String SINGLE_OFFICE = "select office_id,long_name,office_type,report_to_office_id from cwms_20.av_office where office_id=?";
    public static final String ALL_LOCATIONS_QUERY = "select cwms_loc.retrieve_locations_f(?,?,?,?,?) from dual";
    public static final String ALL_UNITS_QUERY = "select cwms_cat.retrieve_units_f(?) from dual";
    private static final String ALL_PARAMETERS_QUERY = "select cwms_cat.retrieve_parameters_f(?) from dual";
    private static final String ALL_TIMEZONES_QUERY = "select cwms_cat.retrieve_time_zones_f(?) from dual";

    private Connection conn;

    public CwmsDataManager(Context ctx) {
        conn = ctx.attribute("database");
    }

    @Override
    public void close() throws SQLException {
        conn.close();
    }

    public String getLocations(String names,String format, String units, String datum, String OfficeId) {
        ResultSet rs = null;
        try( PreparedStatement stmt = conn.prepareStatement(ALL_LOCATIONS_QUERY); ) {
            stmt.setString(1,names);
            stmt.setString(2,format);
            stmt.setString(3,units);
            stmt.setString(4,datum);
            stmt.setString(5,OfficeId);            
            rs = stmt.executeQuery();
            
            if(rs.next()){
                Clob clob = rs.getClob(1);
                return clob.getSubString(1L, (int)clob.length());
            }
            
        }catch (SQLException err) {
            logger.log(Level.WARNING, err.getLocalizedMessage(), err);            
        } finally {
            if( rs != null ){
                try{ rs.close(); } catch (Exception err) {logger.log(Level.WARNING, err.getLocalizedMessage(), err);}
            }
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
	
    
}
