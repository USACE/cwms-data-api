package fixtures;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.catalina.Manager;
import org.apache.commons.io.IOUtils;

import mil.army.usace.hec.test.database.CwmsDatabaseContainer;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import fixtures.tomcat.SingleSignOnWrapper;
import helpers.TsRandomSampler;
import io.restassured.RestAssured;
import io.restassured.config.JsonConfig;
import io.restassured.path.json.config.JsonPathConfig;


@SuppressWarnings("rawtypes")
public class CwmsDataApiSetupCallback implements BeforeAllCallback,AfterAllCallback {

    private static TomcatServer cdaInstance;
    private static CwmsDatabaseContainer<?> cwmsDb;

    private static final String ORACLE_IMAGE = System.getProperty("CDA.oracle.database.image",System.getProperty("RADAR.oracle.database.image", CwmsDatabaseContainer.ORACLE_19C));
    private static final String ORACLE_VOLUME = System.getProperty("CDA.oracle.database.volume",System.getProperty("RADAR.oracle.database.volume", "cwmsdb_data_api_volume"));
    private static final String CWMS_DB_IMAGE = System.getProperty("CDA.cwms.database.iamge",System.getProperty("RADAR.cwms.database.image", "registry.hecdev.net/cwms/schema_installer:23.03.16"));


    @Override
    public void afterAll(ExtensionContext context) throws Exception {

        System.out.println("After all called");
        if( cdaInstance != null ){
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
        if( cdaInstance == null ){
            
            cwmsDb = new CwmsDatabaseContainer(ORACLE_IMAGE)
                            .withOfficeEroc("s0")
                            .withOfficeId("HQ")
                            .withVolumeName(ORACLE_VOLUME)
                            .withSchemaImage(CWMS_DB_IMAGE);                            
            cwmsDb.start();

            this.loadDefaultData(cwmsDb);
            this.loadTimeSeriesData(cwmsDb);
            final String jdbcUrl = cwmsDb.getJdbcUrl();
            final String user = cwmsDb.getPdUser().replace("hectest_pu","webtest");
            final String pw = cwmsDb.getPassword();
            System.setProperty("RADAR_JDBC_URL", jdbcUrl);
            System.setProperty("RADAR_JDBC_USERNAME", user);
            System.setProperty("RADAR_JDBC_PASSWORD", pw);

            System.setProperty("CDA_JDBC_URL", jdbcUrl);
            System.setProperty("CDA_JDBC_USERNAME", user);
            System.setProperty("CDA_JDBC_PASSWORD", pw);


            //catalinaBaseDir = Files.createTempDirectory("", "integration-test");
            System.out.println("warFile property:" + System.getProperty("warFile"));            
            
            cdaInstance = new TomcatServer("build/tomcat", 
                                             System.getProperty("warFile"),
                                             0,
                                             System.getProperty("warContext"));
            cdaInstance.start();
            System.out.println("Tomcat Listing on " + cdaInstance.getPort());
            RestAssured.baseURI=CwmsDataApiSetupCallback.httpUrl();
            RestAssured.port = CwmsDataApiSetupCallback.httpPort();
            RestAssured.basePath = "/cwms-data";
            RestAssured.enableLoggingOfRequestAndResponseIfValidationFails();
            // we only use doubles
            RestAssured.config()
                       .jsonConfig(
                            JsonConfig.jsonConfig()
                                      .numberReturnType(JsonPathConfig.NumberReturnType.DOUBLE));
        }
    }    

    private void loadTimeSeriesData(CwmsDatabaseContainer<?> cwmsDb2) {
        String csv = this.loadResourceAsString("/cwms/radar/data/timeseries.csv");
        StringReader reader = new StringReader(csv);        
        try {
            List<TsRandomSampler.TsSample> samples = TsRandomSampler.load_data(reader);
            cwmsDb2.connection( (c) -> {
                TsRandomSampler.save_to_db(samples, c);
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
            System.out.println(String.format("Running %s as %s %s",data,user,cwmsDb.getPassword()));
            cwmsDb.executeSQL(loadResourceAsString(user_resource[1]).replace("&user.",cwmsDb.getPdUser()
                                                                    .replace("&password.",cwmsDb.getPassword())), user);
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
        return cdaInstance.getPort();
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

    public static Manager getTestSessionManager() {
        return cdaInstance.getTestSessionManager();
    }

    public static SingleSignOnWrapper getSsoValve() {
        return cdaInstance.getSsoValve();
    }

}
