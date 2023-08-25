package helpers;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLType;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.IOUtils;
import org.jooq.impl.SQLDataType;
/**
 * Helper for bulk TS and location data
 */
public class TsRandomSampler {

    private static Logger logger = Logger.getLogger(TsRandomSampler.class.getName());
    private static CSVFormat TS_SAMPLE_FORMAT = CSVFormat.Builder
                                                        .create(CSVFormat.DEFAULT)
                                                        .setNullString("null")
                                                        .setEscape('\\')
                                                        .setIgnoreHeaderCase(true)
                                                        .setSkipHeaderRecord(true)
                                                        .setCommentMarker('#')
                                                        .build();
    /**
     * Generate a 20% sample of timeseries from each office and get the associated location meta data.
     * NOTE: this is not intended to be run constantly so it doesn't save just prints to the screen.
     * To use in tests save data to test/resources/data/timeseries.csv. If you overwrite that file
     * expect to go tweak and fix a bunch of tests.
     * @param args jdbc URL, username, password
     * @throws IOException if the sample query can't be loaded.
     * 
     */
    public static void main(String args[]) throws IOException {
        if( args.length != 3 ) {
            throw new RuntimeException("you must pass db url, username, and password for the query");
        }
        String dbUrl = args[0];
        String dbUser = args[1];
        String dbPassword = args[2];

        String sample_query = IOUtils.toString(
                            TsRandomSampler.class.getResourceAsStream("/helpers/random_sample_query.sql"),
                            "UTF-8");
        String office_query = "select office_id from cwms_20.av_office where office_id not in ('UNK','HQ','CWMS') order by office_id asc";

        try(Connection conn = DriverManager.getConnection(dbUrl, dbUser, dbPassword);
            PreparedStatement setSession = conn.prepareStatement("begin cwms_20.cwms_env.set_session_office_id('HQ'); end;");            
            PreparedStatement officeQuery = conn.prepareStatement(office_query);
            PreparedStatement sampleQuery = conn.prepareStatement(sample_query);
            ResultSet dummy = setSession.executeQuery();
            ResultSet offices = officeQuery.executeQuery();
            CSVPrinter printer = TS_SAMPLE_FORMAT.print(System.out);        
        ) {
            boolean headerPrinted = false;
            while (offices.next()) {
                sampleQuery.setString(1,offices.getString("office_id")); 
                try (ResultSet sample = sampleQuery.executeQuery();) {
                    while (sample.next()) {
                        if (!headerPrinted) {
                            printer.printHeaders(sample);
                            headerPrinted = true;
                        }
                        printer.printRecords(sample);                        
                    }                    
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } 
    }

    public static class TsSample {
        String line;
        String officeId;
        String cwmsTsId;
        String location;
        String locationType;
        String tsActive;
        Double elevation;
        String units;
        String verticalDatum;
        Double latitude;
        Double longitude;
        String horizontalDatum;
        String publicName;
        String longName;
        String description;
        String timeZone;
        String countyName;
        String stateInitial;                          

        public TsSample(CSVRecord csv) {
            String tmp = null;
            line = csv.toString();
            officeId = csv.get("DB_OFFICE_ID");
            cwmsTsId = csv.get("CWMS_TS_ID");
            location = csv.get("LOCATION_ID");
            locationType = csv.get("LOCATION_TYPE");

            tsActive = csv.get("TS_ACTIVE_FLAG");
            
            tmp = csv.get("ELEVATION");
            elevation = tmp != null ? Double.parseDouble(tmp) : null;
            
            units = csv.get("UNIT_ID");
            verticalDatum = csv.get("VERTICAL_DATUM");
            
            tmp = csv.get("LATITUDE");
            latitude = tmp != null ? Double.parseDouble(tmp) : null;
            
            tmp = csv.get("LONGITUDE");
            longitude = tmp != null ? Double.parseDouble(tmp) :null;
            
            horizontalDatum = csv.get("HORIZONTAL_DATUM");
            publicName = csv.get("PUBLIC_NAME");
            longName = csv.get("LONG_NAME");
            description = csv.get("DESCRIPTION");
            timeZone = csv.get("TIME_ZONE_NAME");
            countyName = csv.get("COUNTY_NAME");
            stateInitial = csv.get("STATE_INITIAL");            
        }


        public String getLine() { return line; }
        public String getOfficeId() { return officeId; }
        public String getCwmsTsId() { return cwmsTsId; }
        public String getLocation() { return location; }
        public String getLocationType() { return locationType; }
        public String getTsActive() { return tsActive; }
        public Double getElevation() { return elevation; }
        public String getUnits() { return units; }
        public String getVerticalDatum() { return verticalDatum; }
        public Double getLatitude() { return latitude; }
        public Double getLongitude() { return longitude; }
        public String getHorizontalDatum() { return horizontalDatum; }
        public String getPublicName() { return publicName; }
        public String getLongName() { return longName; }
        public String getDescription() { return description; }
        public String getTimeZone() { return timeZone; }
        public String getCountyName() { return countyName; }
        public String getStateInitial() { return stateInitial; }  

    }

    public static List<TsSample> load_data(Reader csv) throws IOException {
        List<TsSample> list = new ArrayList<>();
        CSVParser parser = TS_SAMPLE_FORMAT.withHeader().parse(csv);
        
        for( CSVRecord rec: parser.getRecords()){ 
            list.add(new TsSample(rec));
        }
        return list;
    }

    public static void save_to_db(List<TsSample> samples, Connection c)  {        
        String lastSample = null;
        try(
            PreparedStatement createLocation = c.prepareStatement(
                                                    IOUtils.toString(
                                                        TsRandomSampler.class.getResourceAsStream("/helpers/create_location.sql"),
                                                        "UTF-8"
                                                    )
                                                );
            PreparedStatement createTimeseries = c.prepareStatement(
                IOUtils.toString(
                    TsRandomSampler.class.getResourceAsStream("/helpers/create_timeseries.sql"),
                    "UTF-8"
                )
            );
        ) {

            for (TsSample sample: samples) {
                lastSample = sample.getLine();
                createLocation.setString(1, sample.getLocation());
                createLocation.setString(2, sample.getLocationType());
                if( sample.getElevation() == null ){
                    createLocation.setNull(3,Types.DOUBLE);
                } else {
                    createLocation.setDouble(3, sample.getElevation());    
                }
                
                createLocation.setString(4, sample.getUnits());
                createLocation.setString(5, sample.getVerticalDatum());
                if( sample.getLatitude() == null ){
                    createLocation.setNull(6, Types.DOUBLE);
                } else {
                    createLocation.setDouble(6, sample.getLatitude());
                }
                
                if( sample.getLongitude() == null ) {
                    createLocation.setNull(7, Types.DOUBLE);
                } else {
                    createLocation.setDouble(7, sample.getLongitude());
                }
                

                createLocation.setString(8, sample.getHorizontalDatum());
                createLocation.setString(9, sample.getPublicName());
                createLocation.setString(10,sample.getLongName());
                createLocation.setString(11,sample.getDescription());
                createLocation.setString(12,sample.getTimeZone());
                createLocation.setString(13,sample.getCountyName());
                createLocation.setString(14,sample.getStateInitial());
                createLocation.setString(15,sample.getTsActive());
                createLocation.setString(16,sample.getOfficeId());
                
                createLocation.execute();
                
                createTimeseries.setString(1,sample.getOfficeId());
                createTimeseries.setString(2,sample.getCwmsTsId());
                
                createTimeseries.execute();
                
            }

            
        } catch(SQLException e) {
            logger.log(Level.WARNING,"failed to save timeseries and location base data at element " + lastSample,e);
        } catch(IOException e) {
            throw new RuntimeException("Unable to load creation query",e);
        }
    }
}
