package fixtures;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Scanner;

import org.apache.tomcat.jni.File;

import mil.army.usace.hec.test.database.CwmsDatabaseContainer;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import io.restassured.RestAssured;


@SuppressWarnings("rawtypes")
public class RadarApiSetupCallback implements BeforeAllCallback,AfterAllCallback{

    private static TomcatServer radarInstance;
    private static CwmsDatabaseContainer cwmsDb;

    @Override
    public void afterAll(ExtensionContext context) throws Exception {

        System.out.println("After all called");
        if( radarInstance != null ){
            cwmsDb.stop();
            cwmsDb.close();
            radarInstance.stop();
        }

    }

    @Override
    @SuppressWarnings("unchecked")
    public void beforeAll(ExtensionContext context) throws Exception {
        System.out.println("Before all called");
        System.out.println(context.getDisplayName());
        if( radarInstance == null ){

            cwmsDb = new CwmsDatabaseContainer(CwmsDatabaseContainer.ORACLE_18XE)
                            .withOfficeEroc("s0")
                            .withOfficeId("HQ")
                            .withSchemaVersion("18-SNAPSHOT");
            cwmsDb.start();

            this.loadDefaultData(cwmsDb);
            System.setProperty("RADAR_JDBC_URL", cwmsDb.getJdbcUrl());
            System.setProperty("RADAR_JDBC_USERNAME",cwmsDb.getPdUser());
            System.setProperty("RADAR_JDBC_PASSWORD", cwmsDb.getPassword());
            //catalinaBaseDir = Files.createTempDirectory("", "integration-test");
            System.out.println(System.getProperty("warFile"));
            radarInstance = new TomcatServer("build/tomcat", System.getProperty("warFile"), 0, System.getProperty("warContext"));
            radarInstance.start();
            System.out.println("Tomcat Listing on " + radarInstance.getPort());
            RestAssured.baseURI=RadarApiSetupCallback.httpUrl();
            RestAssured.port = RadarApiSetupCallback.httpPort();
            RestAssured.basePath = "/cwms-data";
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

    private String loadResourceAsString(String fileName)
    {
        InputStream stream = getClass().getResourceAsStream(fileName);
        Scanner scanner = new Scanner(stream);
        String contents = scanner.useDelimiter("\\A").next();
        scanner.close();
        return contents;
    }

}
