package cwms.radar.api;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;

import cwms.radar.data.dto.Location;
import fixtures.RadarApiSetupCallback;
import mil.army.usace.hec.test.database.CwmsDatabaseContainer;

@Tag("integration")
@ExtendWith(RadarApiSetupCallback.class)
public class DataApiTestIT {
    private static ArrayList<Location> locationsCreated = new ArrayList<>();

    protected static String createLocationQuery = null;
    protected static String deleteLocationQuery = null;
    protected static String createTimeseriesQuery = null;

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

    @AfterAll
    public static void remove_data() {
        locationsCreated.forEach(location -> {
            try {
                CwmsDatabaseContainer<?> db = RadarApiSetupCallback.getDatabaseLink();
                db.connection((c)-> {            
                    try(PreparedStatement stmt = c.prepareStatement(deleteLocationQuery);) {
                        stmt.setString(1,location.getName());                
                        stmt.setString(2,location.getOfficeId());
                        stmt.execute();
                    } catch (SQLException ex) {
                        throw new RuntimeException("Unable to delete location",ex);
                    }
                });    
            } catch(SQLException ex) {
                throw new RuntimeException("Unable to delete location",ex);
            }
        
        });
    }

    /**
     * Creates a location saving the data for later deletion.
     * @param location CWMS Location Name.
     * @param active should this location be flagged active or not.
     * @param office owning office
     * @throws SQLException Any error saving the data
     */
    protected static void createLocation(String location, boolean active, String office) throws SQLException {
        CwmsDatabaseContainer<?> db = RadarApiSetupCallback.getDatabaseLink();
        Location loc = new Location.Builder(location,
                                            null,
                                            null,
                                            null,
                                            null,
                                            null,
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
                stmt.execute();
                locationsCreated.add(loc);
            } catch (SQLException ex) {
                throw new RuntimeException("Unable to create location",ex);
            }
        }, db.getPdUser());
    }

    /**
     * Create a timeseries (location must already exist), no data or other meta data will be set.
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
                throw new RuntimeException("Unable to create location",ex);
            }
        }, db.getPdUser());
    }
}
