package cwms.radar.api;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;

import cwms.radar.data.dto.Location;
import fixtures.RadarApiSetupCallback;
import fixtures.TestAccounts;
import mil.army.usace.hec.test.database.CwmsDatabaseContainer;

/**
 * Helper class to manage cycling tests multiple times against a database.
 * NOTE: Not thread safe, do not run parallel tests. That may be future work though.
 */
@Tag("integration")
@ExtendWith(RadarApiSetupCallback.class)
public class DataApiTestIT {
    /**
     * List of locations that will need to be deleted when tests are done.
     */
    private static ArrayList<Location> locationsCreated = new ArrayList<>();

    protected static String createLocationQuery = null;
    protected static String deleteLocationQuery = null;
    protected static String createTimeseriesQuery = null;
    protected static String registerApiKey = "insert into at_api_keys(userid,key_name,apikey) values(UPPER(?),?,?)";
    protected static String removeApiKey = "delete from at_api_keys where UPPER(userid) = UPPER(?) and key_name = ?";

    /**
     * Reads in SQL data and runs it as CWMS_20. Assumes single statement. That single statement
     * can be an anonymous function if more detail is required.
     * @param resource
     * @throws Exception
     */
    protected static void loadSqlDataFromResource(String resource) throws Exception {
        String sql = IOUtils.toString(
                    DataApiTestIT.class
                    .getClassLoader()
                    .getResourceAsStream(resource),"UTF-8");
        CwmsDatabaseContainer<?> db = RadarApiSetupCallback.getDatabaseLink();
        db.connection((c)-> {
            try(PreparedStatement stmt = c.prepareStatement(sql);) {
                stmt.execute();
            } catch (SQLException ex) {
                throw new RuntimeException("Unable to process SQL",ex);
            }
        }, "cwms_20");
    }

    @BeforeAll
    public static void load_queries() throws Exception {
        createLocationQuery = IOUtils.toString(
                                TimeseriesControllerTestIT.class
                                    .getClassLoader()
                                    .getResourceAsStream("cwms/radar/data/sql_templates/create_location.sql"),"UTF-8"
                            );
        createTimeseriesQuery = IOUtils.toString(
                                TimeseriesControllerTestIT.class
                                    .getClassLoader()
                                    .getResourceAsStream("cwms/radar/data/sql_templates/create_timeseries.sql"),"UTF-8"
                            );
        deleteLocationQuery = IOUtils.toString(
                                TimeseriesControllerTestIT.class
                                    .getClassLoader()
                                    .getResourceAsStream("cwms/radar/data/sql_templates/delete_location.sql"),"UTF-8"
                            );
    }

    /**
     * Register all known users credentials in the database as appropriate.
     * @throws Exception
     */
    @BeforeAll
    public static void register_users() throws Exception {
        try {
            CwmsDatabaseContainer<?> db = RadarApiSetupCallback.getDatabaseLink();
            for(TestAccounts.KeyUser user: TestAccounts.KeyUser.values()) {
                db.connection((c)-> {            
                    try(PreparedStatement stmt = c.prepareStatement(registerApiKey);) {
                        stmt.setString(1,user.getName());                
                        stmt.setString(2,user.getName()+"TestKey");
                        stmt.setString(3,user.getKey());
                        stmt.execute();
                    } catch (SQLException ex) {
                        throw new RuntimeException("Unable to register user",ex);
                    }
                },"cwms_20");
            }
        } catch(Exception ex) {
            throw ex;
        }
    }

    /**
     * Runs cascade delete on each locations. Location not existing is not an error.
     * All other errors will throw a runtime exception and may require manual database
     * cleanup.
     */
    @AfterAll
    public static void remove_data() {
        Iterator<Location> it = locationsCreated.iterator();
        while(it.hasNext()) {
            try {
                Location location = it.next();
                CwmsDatabaseContainer<?> db = RadarApiSetupCallback.getDatabaseLink();
                db.connection((c)-> {            
                    try(PreparedStatement stmt = c.prepareStatement(deleteLocationQuery);) {
                        stmt.setString(1,location.getName());                
                        stmt.setString(2,location.getOfficeId());
                        stmt.execute();
                    } catch (SQLException ex) {
                        if (ex.getErrorCode() != 20025 /*does not exist*/) {
                            throw new RuntimeException("Unable to remove location",ex);
                        }
                    }
                },"cwms_20");
                it.remove();
            } catch(SQLException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    /**
     * Removes all registered users' API keys from the database.
     * 
     * Future work will have this deleting all users/user credentials.
     * @throws Exception
     */
    @AfterAll
    public static void deregister_users() throws Exception {
        try {
            CwmsDatabaseContainer<?> db = RadarApiSetupCallback.getDatabaseLink();
            for(TestAccounts.KeyUser user: TestAccounts.KeyUser.values()) {
                db.connection((c)-> {            
                    try(PreparedStatement stmt = c.prepareStatement(removeApiKey);) {
                        stmt.setString(1,user.getName());                
                        stmt.setString(2,user.getName()+"TestKey");
                        stmt.execute();
                    } catch (SQLException ex) {
                        throw new RuntimeException("Unable to delete api key",ex);
                    }
                },"cwms_20");
            }
        } catch(Exception ex) {
            throw ex;
        }
    }

    /**
     * Creates location with all minimum required data.
     * Additional calls to this function with the same location name are noop. 
     * 
     * @param location Location name
     * @param active Is this location active (allows writing timeseries)
     * @param office Office ID
     * @param latitude 
     * @param longitude 
     * @param horizontalDatum horizontal reference for this location, such as WGS84
     * @param kind Arbitrary string define purpose of location
     */
    protected static void createLocation(String location, boolean active, String office, Double latitude, Double longitude, String horizontalDatum, String timeZone, String kind) throws SQLException {
        CwmsDatabaseContainer<?> db = RadarApiSetupCallback.getDatabaseLink();
        Location loc = new Location.Builder(location,
                                            kind,
                                            ZoneId.of(timeZone),
                                            latitude,
                                            longitude,
                                            horizontalDatum,
                                            office)
                                    .withActive(active)
                                    .build();
        if (locationsCreated.contains(loc)) {
            return; // we already have this location registered
        }
        
        db.connection((c)-> {
            try(PreparedStatement stmt = c.prepareStatement(createLocationQuery);) {
                stmt.setString(1,location);
                stmt.setString(2,active ? "T" : "F");
                stmt.setString(3,office);
                stmt.setString(4,timeZone);
                stmt.setDouble(5,latitude);
                stmt.setDouble(6,longitude);
                stmt.setString(7,horizontalDatum);
                stmt.setString(8,kind);
                stmt.execute();
                locationsCreated.add(loc);
            } catch (SQLException ex) {
                throw new RuntimeException("Unable to create location",ex);
            }
        }, db.getPdUser());
    }

    /**
     * Creates a location saving the data for later deletion. With the following defaults:
     * 
     * <table>
     * <th><td>Parameter</td><td>Value</td></th>
     * <tr><td>latitude</td><td>0.0</td></tr>
     * <tr><td>longitude</td><td>0.0</td></tr>
     * <tr><td>horizontalDatum</td><td>WGS84</td></tr>
     * <tr><td>timeZone</td><td>UTC</td></tr>
     * <tr><td>kind</td><td>STREAM</td></tr>
     * </table>
     * 
     * @param location CWMS Location Name.
     * @param active should this location be flagged active or not.
     * @param office owning office
     * @throws SQLException Any error saving the data
     */
    protected static void createLocation(String location, boolean active, String office) throws SQLException {
        createLocation(location,active,office,
                       0.0,0.0,"WGS84",
                       "UTC","STREAM");
    }

    /**
     * Create a timeseries (location must already exist), no data or other meta data will be set.
     * This only creates the timeseries name. Not data or other parameters are set.
     * 
     * @param office owning office
     * @param timeseries timeseries name
     * @throws SQLException
     */
    protected static void createTimeseries(String office, String timeseries) throws SQLException {
        CwmsDatabaseContainer<?> db = RadarApiSetupCallback.getDatabaseLink();
        db.connection((c)-> {
            try(PreparedStatement stmt = c.prepareStatement(createTimeseriesQuery);) {
                stmt.setString(1,office);
                stmt.setString(2,timeseries);
                stmt.execute();
            } catch (SQLException ex) {
                throw new RuntimeException("Unable to create timeseries",ex);
            }
        }, db.getPdUser());
    }

    /**
     * If necessary for a specific test add the TEST user to the appropriate office CWMS Group.
     * 
     * @param user CWMS User Name
     * @param group CWMS Group Name
     * @param office CWMS Office ID
     * @throws Exception Any errors running the sql command
     */
    protected static void addUserToGroup(String user, String group, String office) throws Exception {
        CwmsDatabaseContainer<?> db = RadarApiSetupCallback.getDatabaseLink();
        db.connection( (c) -> {
            try(PreparedStatement stmt = c.prepareStatement("begin cwms_sec.add_user_to_group(?,?,?); end;");) {
                stmt.setString(1,user);
                stmt.setString(2,group);
                stmt.setString(3,office);
                stmt.execute();
            } catch( SQLException ex) {
                throw new RuntimeException(ex);
            }
        },"cwms_20");
    }

    /**
     * If necessary for a specific test remove a user from a CWMS Group.
     * 
     * @param user CWMS User Name
     * @param group CWMS Group Name
     * @param office CWMS Office ID
     * @throws Exception Any errors running the sql command
     */
    protected static void removeUserFromGroup(String user, String group, String office) throws Exception {
        CwmsDatabaseContainer<?> db = RadarApiSetupCallback.getDatabaseLink();
        db.connection( (c) -> {
            try(PreparedStatement stmt = c.prepareStatement("begin cwms_sec.remove_user_from_group(?,?,?); end;");) {
                stmt.setString(1,user);
                stmt.setString(2,group);
                stmt.setString(3,office);
                stmt.execute();
            } catch( SQLException ex) {
                throw new RuntimeException(ex);
            }
        },"cwms_20");
    }
}
