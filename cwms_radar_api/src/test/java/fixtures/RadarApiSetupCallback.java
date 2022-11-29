package fixtures;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Scanner;

import org.apache.commons.io.IOUtils;
import org.apache.tomcat.jni.File;

import mil.army.usace.hec.test.database.CwmsDatabaseContainer;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import cwms.radar.ApiServlet;
import cwms.radar.security.KeyAccessManager;
import helpers.TsRandomSampler;
import io.restassured.RestAssured;


@SuppressWarnings("rawtypes")
public class RadarApiSetupCallback implements BeforeAllCallback,AfterAllCallback{

    private static TomcatServer radarInstance;
    private static CwmsDatabaseContainer cwmsDb;
<<<<<<< HEAD
    private static TestRealm realm;
    private static TestAuthValve authValve;
    private static final String ORACLE_IMAGE = System.getProperty("RADAR.oracle.database.image", CwmsDatabaseContainer.ORACLE_19C);
    private static final String ORACLE_VOLUME = System.getProperty("RADAR.oracle.database.volume", "cwmsdb_radar_volume");
    private static final String CWMS_DB_IMAGE = System.getProperty("RADAR.cwms.database.image", "registry.hecdev.net/cwms_schema_installer:latest-dev");
=======

    private static String DB_VERSION = System.getProperty("oracle.version", CwmsDatabaseContainer.ORACLE_19C);
    private static String DB_VOLUME = System.getProperty("oracle.volume", "cwmsdb_radar_volume");
    private static String CWMS_VERSION = System.getProperty("cwms.schema.version", "registry.hecdev.net/cwms/schema_installer:latest-dev");
    
>>>>>>> e050d46 (General workflow works, database itself needs tweaks.)

    @Override
    public void afterAll(ExtensionContext context) throws Exception {

        System.out.println("After all called");
        if( radarInstance != null ){
            //cwmsDb.stop();
            //cwmsDb.close();
            //radarInstance.stop();
        }

    }

    @Override
    @SuppressWarnings("unchecked")
    public void beforeAll(ExtensionContext context) throws Exception {
        System.out.println("Before all called");
        System.out.println(context.getDisplayName());
        if( radarInstance == null ){
            
            cwmsDb = new CwmsDatabaseContainer(ORACLE_IMAGE)
                            .withOfficeEroc("s0")
                            .withOfficeId("HQ")
                            .withVolumeName(ORACLE_VOLUME)
                            .withSchemaImage(CWMS_DB_IMAGE);                            
            cwmsDb.start();

            this.loadDefaultData(cwmsDb);
            this.loadTimeSeriesData(cwmsDb);
            System.setProperty("RADAR_JDBC_URL", cwmsDb.getJdbcUrl());
            System.setProperty("RADAR_JDBC_USERNAME",cwmsDb.getPdUser());
            System.setProperty("RADAR_JDBC_PASSWORD", cwmsDb.getPassword());
            //catalinaBaseDir = Files.createTempDirectory("", "integration-test");
            System.out.println("warFile property:" + System.getProperty("warFile"));

            // ADD TestPrincipal with real session key here; generate the session key using
            // the cwmsDb.connection functions to call get_user_credentials as appropriate.
            // OR... forcibly add the fake session_key to the at_sec_session table. dealers choice.
            cwmsDb.connection( (conn) -> {
                String digest = KeyAccessManager.getDigest("TestKey".getBytes());
                try(PreparedStatement insertApiKey = ((Connection)conn).prepareStatement("insert into apikeys(key,username) values (?,?)");){
                    insertApiKey.setString(1,digest);
                    insertApiKey.setString(2,"USER1");
                    insertApiKey.execute();
                }
                catch(SQLException ex) {
                    throw new RuntimeException("can't set fake user",ex);
                }
            },"CWMS_20");

            radarInstance = new TomcatServer("build/tomcat", 
                                             System.getProperty("warFile"),
                                             0,
                                             System.getProperty("warContext"), 
                                             null,
                                             null);
            radarInstance.start();
            System.out.println("Tomcat Listing on " + radarInstance.getPort());
            RestAssured.baseURI=RadarApiSetupCallback.httpUrl();
            RestAssured.port = RadarApiSetupCallback.httpPort();
            RestAssured.basePath = "/cwms-data";
            RestAssured.enableLoggingOfRequestAndResponseIfValidationFails();
        }
    }

    private void loadTimeSeriesData(CwmsDatabaseContainer cwmsDb2) {
        String csv = this.loadResourceAsString("/cwms/radar/data/timeseries.csv");
        StringReader reader = new StringReader(csv);        
        try {
            List<TsRandomSampler.TsSample> samples = TsRandomSampler.load_data(reader);
            cwmsDb2.connection( (c) -> {
                TsRandomSampler.save_to_db(samples,(Connection) c);
            },"cwms_20");
        } catch (IOException e) {
            throw new RuntimeException("Failed to load timeseries list",e);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to save timeseries list to db",e);
        }
        
        
    }

    private void loadDefaultData(CwmsDatabaseContainer cwmsDb) throws SQLException {
        ArrayList<String> defaultList = getDefaultList();
        for( String data: defaultList){
            String user_resource[] = data.split(":");
            String user = user_resource[0];
            if( user.equalsIgnoreCase("dba")){
                user = cwmsDb.getDbaUser();
            } else if( user.equalsIgnoreCase("user")) {
                user = cwmsDb.getUsername();
            }
            System.out.println("running switch with " + user);
            cwmsDb.executeSQL(loadResourceAsString(user_resource[1]).replace("&user.",cwmsDb.getPdUser()), user);
        }

    }

    private ArrayList<String> getDefaultList() {
        ArrayList<String> list = new ArrayList<>();
        InputStream listStream = getClass().getResourceAsStream("/cwms/radar/data/sql/defaultload.txt");
        try( BufferedReader br = new BufferedReader( new InputStreamReader(listStream) );) {
            String line = null;
            while( (line = br.readLine() ) != null){
                if( line.trim().startsWith("#") ) continue;
                list.add(line);
            }
        } catch ( IOException err ){
            System.err.println("Failed to load default data" + err.getLocalizedMessage());
        }
        return list;
    }

    public static String httpUrl(){
        return "http://localhost";
    }

    public static int httpPort() {
        return radarInstance.getPort();
    }

    public static CwmsDatabaseContainer getDatabaseLink() {
        return cwmsDb;
    }

    private String loadResourceAsString(String fileName) {        
        try {
            return IOUtils.toString(
                        getClass().getResourceAsStream(fileName),
                        "UTF-8"
                    );
        } catch (IOException e) {            
           throw new RuntimeException("Unable to load resource: " + fileName,e);
        }        
    }

}
