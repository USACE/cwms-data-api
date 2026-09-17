package fixtures;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.apache.catalina.Manager;
import org.apache.commons.io.IOUtils;

import mil.army.usace.hec.test.database.CwmsDatabaseContainer;
import mil.army.usace.hec.test.database.CwmsDatabaseContainers;
import mil.army.usace.hec.test.database.TeamCityUtilities;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;

import org.slf4j.bridge.SLF4JBridgeHandler;

import com.google.common.flogger.FluentLogger;

import cwms.cda.data.dao.Dao;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.security.OpenIdConnectIdentityProvider;
import fixtures.tomcat.SingleSignOnWrapper;
import helpers.TsRandomSampler;
import io.restassured.RestAssured;
import io.restassured.config.EncoderConfig;
import io.restassured.config.JsonConfig;
import io.restassured.filter.log.LogDetail;
import io.restassured.path.json.config.JsonPathConfig;
import javax.servlet.http.HttpServletResponse;
import org.testcontainers.images.PullPolicy;

import static cwms.cda.helpers.DatabaseHelpers.LATEST_SCHEMA;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.is;


@SuppressWarnings("rawtypes")
@ExtendWith(KeyCloakExtension.class)
public class CwmsDataApiSetupCallback implements BeforeAllCallback,AfterAllCallback {

    private static final FluentLogger logger = FluentLogger.forEnclosingClass();

    private static TomcatServer cdaInstance;
    private static CwmsDatabaseContainer<?> cwmsDb;

    private static final String ORACLE_IMAGE =
        System.getProperty("CDA.oracle.database.image",
                           "ghcr.io/hydrologicengineeringcenter/cwms-database/cwms/database-ready-ora-23.5:latest-dev"
                       );
    private static final String ORACLE_VOLUME =
        System.getProperty("CDA.oracle.database.volume",
                           "cwmsdb_data_api_volume"
                          );
    static final String CWMS_DB_IMAGE =
        System.getProperty("CDA.cwms.database.image",
                           "ghcr.io/hydrologicengineeringcenter/cwms-database/cwms/schema_installer:latest-dev"
                          );


    private static String webUser = null;

    public static final String VERSION_STRING;
    public static final int VERSION_INT;

    static {
        VERSION_STRING = schemaVersion();
        VERSION_INT = versionInt();
    }

    static {
        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();
    }

    private static String schemaVersion() {
        String ret;
        if (!System.getProperty(CwmsDatabaseContainers.BYPASS_URL,"").isEmpty())
        {
            ret = System.getProperty("testcontainer.cwms.bypass.version","Bypass");
        }
        else if (ORACLE_IMAGE.contains("database-ready"))
        {
            ret = ORACLE_IMAGE.split(":")[1];
        }
        else
        {
            ret = CWMS_DB_IMAGE.split(":")[1];
        }
        return ret;
    }

    private static int versionInt()
    {
        int ret;
        String tmp = schemaVersion();
        if (tmp.equalsIgnoreCase("latest-dev")) {
            ret = LATEST_SCHEMA;
        } else if (tmp.equalsIgnoreCase("Bypass")) {
            ret = -1;
        } else if(tmp.toLowerCase().endsWith("staging")) {
            ret = 1009999;
        } else {
            ret = Dao.versionAsInteger(tmp.replaceAll("-RC.*", "").replace("-","."));
        }
        return ret;
    }

    public static int getSchemaVersion() {
        if (cwmsDb == null) {
            // Class-level execution conditions run before the database container starts.
            return VERSION_INT;
        }
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        try {
            return db.connection((c) -> {
                var ctx = JooqDao.getDslContext(c, db.getOfficeId());
                return Dao.versionAsInteger(Dao.getVersion(ctx));
            }, webUser);
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void afterAll(ExtensionContext context) throws Exception {
        if (cdaInstance != null) {
            // test-containers will handle stopping everything
        }
    }

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        if (cdaInstance == null ) {
            cwmsDb = CwmsDatabaseContainers.createDatabaseContainer(ORACLE_IMAGE)
                            .withOfficeEroc("s0")
                            .withOfficeId("HQ")
                            .withVolumeName(TeamCityUtilities.cleanupBranchName(ORACLE_VOLUME))
                            .withSchemaImage(CWMS_DB_IMAGE);
            cwmsDb.withImagePullPolicy(PullPolicy.ageBased(Duration.ofDays(1)));
            cwmsDb.start();

            final String jdbcUrl = cwmsDb.getJdbcUrl();
            webUser = cwmsDb.getPdUser().substring(0,2)+"webtest";
            final String pw = cwmsDb.getPassword();
            this.loadDefaultData(cwmsDb);
            this.loadTimeSeriesData(cwmsDb);

            System.setProperty("RADAR_JDBC_URL", jdbcUrl);
            System.setProperty("RADAR_JDBC_USERNAME", webUser);
            System.setProperty("RADAR_JDBC_PASSWORD", pw);

            System.setProperty("CDA_JDBC_URL", jdbcUrl);
            System.setProperty("CDA_JDBC_USERNAME", webUser);
            System.setProperty("CDA_JDBC_PASSWORD", pw);

            // OIDC properties
            System.setProperty("cwms.dataapi.access.providers","KeyAccessManager,OpenID,CwmsAccessManager");
            System.setProperty(OpenIdConnectIdentityProvider.CREATE_USERS_KEY,"true");
            System.setProperty(OpenIdConnectIdentityProvider.WELL_KNOWN_PROPERTY,KeyCloakExtension.getOidcWellKnown());
            System.setProperty(OpenIdConnectIdentityProvider.ISSUER_PROPERTY,KeyCloakExtension.getIssuer());
            System.setProperty(OpenIdConnectIdentityProvider.TIMEOUT_PROPERTY, "1"); // to force a reload at least once.
            logger.atInfo().log("warFile property:" + System.getProperty("warFile"));

            cdaInstance = new TomcatServer("build/tomcat",
                                             System.getProperty("warFile"),
                                             0,
                                             System.getProperty("warContext"));
            cdaInstance.start();
            logger.atInfo().log("Tomcat Listing on " + cdaInstance.getPort());
            RestAssured.baseURI=CwmsDataApiSetupCallback.httpUrl();
            RestAssured.port = CwmsDataApiSetupCallback.httpPort();
            RestAssured.basePath = System.getProperty("warContext");
            // actually assign the new config to the global configuration. just running this here without
            // the assignment apparently does nothing.
            RestAssured.config = RestAssured.config()
                        // we only use doubles (NOTE: this is commend out because this config was
                        // never originally active and will be addressed in a followup)
                    //    .jsonConfig(
                    //         JsonConfig.jsonConfig()
                    //                   .numberReturnType(JsonPathConfig.NumberReturnType.DOUBLE))
                        // our content type processing is a bit more picky now.
                        // I also don't recal seeing any default COntent-Type or Accept header
                        // defaults from browsers that include this much.
                        // if we start seeing it we need to add explicity @FormattableWith annotations
                        // per character as that is a distinct content-type.
                       .encoderConfig(
                            EncoderConfig.encoderConfig()
                                         .appendDefaultContentCharsetToContentTypeIfUndefined(
                                            false
                                         ));
            healthCheck();
        }
    }

    private static void healthCheck() throws InterruptedException {
        int attempts = 0;
        int maxAttempts = 30;
        for (; attempts < maxAttempts; attempts++) {
            try {
                given()
                .when()
                    .get("/offices/SPK")
                .then()
                    .log().ifValidationFails(LogDetail.ALL)
                    .assertThat()
                    .statusCode(is(HttpServletResponse.SC_OK));
                logger.atInfo().log("Server is up!");
                break;
            } catch (Throwable e) {
                logger.atInfo().log("Waiting for the server to start...");
                // yes, 100 millis *should* be fine. But at least my machine keeps lagging.
                Thread.sleep(300);
            }
        }
        if (attempts == maxAttempts) {
            throw new IllegalStateException("Server didn't start in time...");
        }
    }

    private void loadTimeSeriesData(CwmsDatabaseContainer<?> cwmsDb2) {
        String csv = this.loadResourceAsString("/cwms/cda/data/timeseries.csv");
        StringReader reader = new StringReader(csv);
        try {
            List<TsRandomSampler.TsSample> samples = TsRandomSampler.load_data(reader);
            cwmsDb2.connection( c -> {
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
            String[] user_resource = data.split(":");
            String user = user_resource[0];
            if( user.equalsIgnoreCase("dba")){
                user = cwmsDb.getDbaUser();
            } else if( user.equalsIgnoreCase("user")) {
                user = cwmsDb.getUsername();
            }
            logger.atInfo().log(String.format("Running %s as %s %s", data, user, cwmsDb.getPassword()));
            logger.atInfo().log("Webuser = " + webUser);
            cwmsDb.executeSQL(loadResourceAsString(user_resource[1]).replace("&pduser", cwmsDb.getPdUser())
                                                                    .replace("&user", cwmsDb.getUsername())
                                                                    .replace("&webuser", webUser)
                                                                    .replace("&password", cwmsDb.getPassword()), user);
        }
    }

    private ArrayList<String> getDefaultList() {
        ArrayList<String> list = new ArrayList<>();
        InputStream listStream = getClass().getResourceAsStream("/cwms/cda/data/sql/defaultload.txt");
        try( BufferedReader br = new BufferedReader(new InputStreamReader(listStream))) {
            String line = null;
            while( (line = br.readLine() ) != null){
                if( line.trim().startsWith("#") ) continue;
                list.add(line);
            }
        } catch ( IOException err ){
            logger.atWarning().withCause(err).log("Failed to load default data");
        }
        return list;
    }

    public static String httpUrl(){
        return "http://localhost";
    }

    public static int httpPort() {
        return cdaInstance.getPort();
    }

    public static CwmsDatabaseContainer<?> getDatabaseLink() {
        return cwmsDb;
    }

    public static void shutdown() throws Exception {
        Exception failure = null;
        if (cdaInstance != null) {
            try {
                cdaInstance.stop();
            } catch (Exception e) {
                failure = e;
            } finally {
                cdaInstance = null;
            }
        }

        if (cwmsDb != null) {
            try {
                cwmsDb.stop();
            } catch (Exception e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            } finally {
                cwmsDb = null;
            }
        }

        webUser = null;

        if (failure != null) {
            throw failure;
        }
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

    public static String getWebUser() {
        if (webUser == null) {
            throw new IllegalStateException("This method should not be called before CwmsDataAPiSetupCallback::beforeAll.");
        }
        return webUser;
    }
}
