/*
 * MIT License
 *
 * Copyright (c) 2025 Hydrologic Engineering Center
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package cwms.cda.api;

import static cwms.cda.data.dao.JooqDao.REQUIRE_NEW_LRTS_ID_FORMAT;
import static cwms.cda.data.dao.JooqDao.SESSION_USE_LRTS_ID_FORMAT;

import com.atlassian.oai.validator.restassured.OpenApiValidationFilter;
import com.google.common.flogger.FluentLogger;
import cwms.cda.data.dao.AuthDao;
import cwms.cda.data.dao.DeleteRule;
import cwms.cda.data.dao.StreamDao;
import cwms.cda.data.dao.VerticalDatum;
import cwms.cda.data.dao.basin.BasinDao;
import cwms.cda.data.dto.Location;
import cwms.cda.data.dto.LocationCategory;
import cwms.cda.data.dto.LocationGroup;
import cwms.cda.data.dto.auth.ApiKey;
import cwms.cda.data.dto.basin.Basin;
import cwms.cda.data.dto.stream.Stream;
import cwms.cda.helpers.ZoneIdHelper;
import cwms.cda.security.DataApiPrincipal;
import fixtures.CwmsDataApiSetupCallback;
import fixtures.IntegrationTestNameGenerator;
import fixtures.KeyCloakExtension;
import fixtures.MinIOExtension;
import fixtures.TestAccounts;
import fixtures.users.MockCwmsUserPrincipalImpl;
import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;
import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import mil.army.usace.hec.test.database.CwmsDatabaseContainer;
import org.apache.catalina.Manager;
import org.apache.catalina.SessionEvent;
import org.apache.catalina.SessionListener;
import org.apache.catalina.session.StandardSession;
import org.apache.commons.io.IOUtils;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;
import usace.cwms.db.jooq.codegen.packages.CWMS_ENV_PACKAGE;
import usace.cwms.db.jooq.codegen.packages.CWMS_LOC_PACKAGE;
import usace.cwms.db.jooq.codegen.packages.CWMS_UTIL_PACKAGE;

/**
 * Helper class to manage cycling tests multiple times against a database.
 * NOTE: Not thread safe, do not run parallel tests. That may be future work though.
 */
@DisplayNameGeneration(IntegrationTestNameGenerator.class)
@Tag("integration")
@ExtendWith(KeyCloakExtension.class)
@ExtendWith(MinIOExtension.class)
@ExtendWith(CwmsDataApiSetupCallback.class)
public class DataApiTestIT {
    private static final FluentLogger logger = FluentLogger.forEnclosingClass();

    protected static String createLocationQuery = null;
    protected static String deleteLocationQuery = null;
    protected static String createTimeseriesQuery = null;
    protected static String createTimeseriesOffsetQuery = null;
    protected static final String removeApiKeys = "delete from at_api_keys where UPPER(userid) = UPPER(?) and key_name = ?";

    protected static final Configuration freemarkerConfig = new Configuration(Configuration.VERSION_2_3_32);

    private ArrayList<LocationGroup> groupsCreated = new ArrayList<>();
    private ArrayList<LocationCategory> categoriesCreated = new ArrayList<>();
    private static List<Stream> streamsCreated = new ArrayList<>();
    private static List<Basin> basinsCreated = new ArrayList<>();

    static {
        freemarkerConfig.setClassForTemplateLoading(DataApiTestIT.class, "/");
    }

    private static String OPEN_API_SPEC_URL = null;
    private static OpenApiValidationFilter validationFilter = null;

    public static OpenApiValidationFilter getOpenApiValidationFilter() {
        if (validationFilter == null) {
            OPEN_API_SPEC_URL = String.format("%s:%s%s/swagger-docs", CwmsDataApiSetupCallback.httpUrl(), CwmsDataApiSetupCallback.httpPort(), System.getProperty("warContext"));
            validationFilter = new OpenApiValidationFilter(OPEN_API_SPEC_URL);
        }
        return validationFilter;
    }

    /**
     * Reads in SQL data and runs it as CWMS_20. Assumes single statement. That single statement
     * can be an anonymous function if more detail is required.
     *
     * @param resource Resource path to SQL file.  Example: "cwms/cda/data/sql/create_location.sql"
     * @throws Exception
     */
    protected static void loadSqlDataFromResource(String resource) throws Exception {
        String sql = IOUtils.toString(
                DataApiTestIT.class
                        .getClassLoader()
                        .getResourceAsStream(resource), "UTF-8");
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection((c) -> {
            try (PreparedStatement stmt = c.prepareStatement(sql)) {
                stmt.execute();
            } catch (SQLException ex) {
                throw new RuntimeException("Unable to process SQL", ex);
            }
        }, "cwms_20");
    }

    private static Template loadTemplateFromResource(String resource) throws Exception {
        return freemarkerConfig.getTemplate(resource);
    }

    @BeforeAll
    public static void load_queries() throws Exception {
        createLocationQuery = IOUtils.toString(
                TimeseriesControllerTestIT.class
                        .getClassLoader()
                        .getResourceAsStream("cwms/cda/data/sql_templates/create_location.sql"), "UTF-8"
        );
        deleteLocationQuery = IOUtils.toString(
                TimeseriesControllerTestIT.class
                        .getClassLoader()
                        .getResourceAsStream("cwms/cda/data/sql_templates/delete_location.sql"), "UTF-8"
        );
        createTimeseriesQuery = IOUtils.toString(
                TimeseriesControllerTestIT.class
                        .getClassLoader()
                        .getResourceAsStream("cwms/cda/data/sql_templates/create_timeseries.sql"), "UTF-8"
        );

        createTimeseriesOffsetQuery = IOUtils.toString(
                TimeseriesControllerTestIT.class
                        .getClassLoader()
                        .getResourceAsStream("cwms/cda/data/sql_templates/create_timeseries_offset.sql"), "UTF-8"
        );

    }

    /**
     * Register all known users credentials in the database as appropriate.
     *
     * @throws Exception
     */
    @BeforeAll
    public static void register_users() throws Exception {
        try {
            final Manager tsm = CwmsDataApiSetupCallback.getTestSessionManager();
            CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
            for (TestAccounts.KeyUser user : TestAccounts.KeyUser.values()) {
                if (user.getKeyName() == null) {
                    continue;
                }
                if (user == TestAccounts.KeyUser.SPK_OTHER_NORMAL_SAME_ROLES || user == TestAccounts.KeyUser.SPK_CAC_BUT_NOT_CWMS_USER) {
                    String name = user.getName();
                    String officeId = user.getOperatingOffice(); //"SPK";  // We want user office not db office.

                    logger.atInfo().log("Adding user %s in %s to groups", name, officeId);
                    try {
                        addNewUser(name);
                    } catch (RuntimeException ex) {
                        Throwable cause = ex.getCause();
                        if (cause == null || !cause.getMessage().contains("already exists")) {
                            throw ex;
                        } else {
                            logger.atInfo().log("User %s already exists", name);
                        }
                    }

                    if(user != TestAccounts.KeyUser.SPK_CAC_BUT_NOT_CWMS_USER)
                    {
                        addUserToGroup(name, "CWMS Users", officeId);
                    }
                    addUserToGroup(name, "All Users", officeId);
                    addUserToGroup(name, "TS ID Creator", officeId);
                }

                db.connection((c) -> {
                    String key = AuthDao.getInstance(DSL.using(c), null)
                        .createApiKey(new DataApiPrincipal(user.getName(), Set.of()),
                            new ApiKey(user.getName(), user.getKeyName(), null,
                                ZonedDateTime.now(), null)).getApiKey();
                    user.setApiKey(key);
                }, "cwms_20");

                StandardSession session = (StandardSession) tsm.createSession(user.getJSessionId());
                if (session == null) {
                    throw new RuntimeException("Test Session Manager is unusable.");
                }
                MockCwmsUserPrincipalImpl mcup = new MockCwmsUserPrincipalImpl(user.getName(), user.getEdipi(), user.getRoles());
                session.setAuthType("CLIENT-CERT");
                session.setPrincipal(mcup);
                session.activate();
                session.addSessionListener(new SessionListener() {

                    @Override
                    public void sessionEvent(SessionEvent event) {
                        logger.atInfo().log("Got event of type: %s", event.getType());
                        logger.atInfo().log("Session is: %s", event.getSession().toString());
                    }

                });
                CwmsDataApiSetupCallback.getSsoValve()
                        .wrappedRegister(user.getJSessionId(), mcup, "CLIENT-CERT", null, null);
            }
        } catch (RuntimeException ex) {
            throw new Exception("User registration failed", ex);
        }
    }


    /**
     * Removes all registered users' API keys from the database.
     * <p>
     * Future work will have this deleting all users/user credentials.
     *
     * @throws Exception
     */
    @AfterAll
    public static void deregister_users() throws Exception {
        try {
            CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
            for (TestAccounts.KeyUser user : TestAccounts.KeyUser.values()) {
                db.connection((c) -> {
                    try (PreparedStatement stmt = c.prepareStatement(removeApiKeys)) {
                        stmt.setString(1, user.getName());
                        stmt.setString(2, user.getKeyName());
                        stmt.execute();
                    } catch (SQLException ex) {
                        throw new RuntimeException("Unable to delete api key", ex);
                    }
                }, "cwms_20");
            }
        } catch (Exception ex) {
            throw ex;
        }
    }

    protected static void addNewUser(String username) throws SQLException {
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        String insertUserSql = "INSERT INTO AT_SEC_CWMS_USERS (USERID, CREATEDBY) VALUES (?, ?)";

        db.connection((c) -> {
            try (PreparedStatement stmt = c.prepareStatement(insertUserSql)) {
                stmt.setString(1, username);  // USERID
                stmt.setString(2, "CWMS_20");       // CREATEDBY
                stmt.executeUpdate();
            } catch (SQLException ex) {
                if (!ex.getMessage().contains("unique constraint")) {
                    throw new RuntimeException("Unable to insert user: " + username, ex);
                }
            }
        }, "cwms_20");
    }

    protected static void addVerticalDatumOffsetForExistingLocation(String location, String officeId, VerticalDatum from, VerticalDatum to, double offset, boolean isEstimate) throws SQLException {
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        String desc = isEstimate ? "ESTIMATE" : "";
        final String insertSql =
                "INSERT INTO AT_VERT_DATUM_OFFSET " +
                        " (LOCATION_CODE, VERTICAL_DATUM_ID_1, VERTICAL_DATUM_ID_2, EFFECTIVE_DATE, OFFSET, DESCRIPTION) " +
                        " VALUES (?, ?, ?, ?, ?, ?)";

        db.connection(c -> {
            String sql = "SELECT LOCATION_CODE FROM AV_LOC2 WHERE DB_OFFICE_ID = ? AND LOCATION_ID = ?";
            Long locationCode = null;
            try (PreparedStatement ps = c.prepareStatement(sql)) {
                ps.setString(1, officeId);
                ps.setString(2, location);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        locationCode = rs.getLong(1);
                    }
                }
            } catch (SQLException ex) {
                throw new RuntimeException("Unable to verify location exists for offset insert", ex);
            }
            if (locationCode == null) {
                throw new IllegalArgumentException("Location not found for office=" + officeId + ", id=" + location);
            }
            try (PreparedStatement ps = c.prepareStatement(insertSql)) {
                ps.setLong(1, locationCode);              // LOCATION_CODE
                ps.setString(2, from.toString());       // VERTICAL_DATUM_ID_1
                ps.setString(3, to.toString());         // VERTICAL_DATUM_ID_2
                ps.setDate(4, Date.valueOf("1000-01-01")); //EFFECTIVE_DATE
                ps.setDouble(5, offset);                // OFFSET
                ps.setString(6, desc);                  // DESCRIPTION ("" if not estimate)
                ps.executeUpdate();
            } catch (SQLException ex) {
                throw new RuntimeException("Unable to insert vertical datum offset", ex);
            }
        }, "cwms_20");
    }

    /**
     * Creates location with all minimum required data.
     * Additional calls to this function with the same location name are noop.
     *
     * @param location        Location name
     * @param active          Is this location active (allows writing timeseries)
     * @param office          Office ID
     * @param latitude
     * @param longitude
     * @param horizontalDatum horizontal reference for this location, such as WGS84
     * @param kind            Arbitrary string define purpose of location
     */
    protected static void createLocation(String location, boolean active, String office, Double latitude, Double longitude, String horizontalDatum, String timeZone, String kind) throws SQLException {
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        Location loc = new Location.Builder(location,
                kind,
                ZoneIdHelper.parseZoneIdWithAliases(timeZone),
                latitude,
                longitude,
                horizontalDatum,
                office)
                .withActive(active)
                .build();
        if (LocationCleanup.locationsCreated.contains(loc)) {
            return; // we already have this location registered
        }

        db.connection((c) -> {
            try (PreparedStatement stmt = c.prepareStatement(createLocationQuery)) {
                stmt.setString(1, location);
                stmt.setString(2, active ? "T" : "F");
                stmt.setString(3, office);
                stmt.setString(4, timeZone);
                stmt.setDouble(5, latitude);
                stmt.setDouble(6, longitude);
                stmt.setString(7, horizontalDatum);
                stmt.setString(8, kind);
                stmt.execute();
                LocationCleanup.locationsCreated.add(loc);
            } catch (SQLException ex) {
                throw new RuntimeException("Unable to create location", ex);
            }
        }, "cwms_20");
    }

    protected static void createLocationWithVerticalDatum(String location, boolean active, String office, VerticalDatum verticalDatum) throws SQLException
    {
        createLocation(location, active, office);
        updateLocation(location, active, office, verticalDatum);
    }

    private static void updateLocation(String location, boolean active, String officeId, VerticalDatum verticalDatum) throws SQLException {

        String P_LOCATION_ID = location;
        String P_LOCATION_TYPE = "SITE";
        Number P_ELEVATION = 11;
        String P_ELEV_UNIT_ID = "m";

        // Pretty sure this isn't supposed to have a dash.  The create doesn't check.  The default create just passes null.
        // If it has a dash then the offsets don't work.
        // select VERTICAL_DATUM, count(*) as COUNT
        //  from AT_PHYSICAL_LOCATION
        //  group by VERTICAL_DATUM
        //  order by COUNT desc
        // has no entries with a dash in the name (unless we've run this test with a dash).
        String P_VERTICAL_DATUM = verticalDatum.toString();
        Number P_LATITUDE = 38.5757;   // pretty sure that if these are 0,0 then its not inside the navd88 bounds and the offsets come back []
        Number P_LONGITUDE = -121.4789;
        String P_HORIZONTAL_DATUM = "WGS84";
        String P_PUBLIC_NAME = "Integration Test Sac Dam";
        String P_LONG_NAME= null;
        String P_DESCRIPTION = "for testing";
        String P_TIME_ZONE_ID = "UTC";
        String P_COUNTY_NAME = "Sacramento";
        String P_STATE_INITIAL = "CA";
        String P_ACTIVE = active ? "T" : "F";
        String P_DB_OFFICE_ID = officeId;

        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection(c -> {
            DSLContext dslContext = getDslContext(c, officeId);

            //            CWMS_LOC_PACKAGE.call_DELETE_LOCATION(dslContext.configuration(), P_LOCATION_ID, String.valueOf(DeleteRule.DELETE_LOC_CASCADE), P_DB_OFFICE_ID);
            //            CWMS_LOC_PACKAGE.call_CREATE_LOCATION(dslContext.configuration(),
            //                    P_LOCATION_ID, P_LOCATION_TYPE, P_ELEVATION, P_ELEV_UNIT_ID, P_VERTICAL_DATUM, P_LATITUDE, P_LONGITUDE,
            //                    P_HORIZONTAL_DATUM, P_PUBLIC_NAME, P_LONG_NAME, P_DESCRIPTION, P_TIME_ZONE_ID, P_COUNTY_NAME, P_STATE_INITIAL,
            //                    P_ACTIVE, P_DB_OFFICE_ID);

            String P_IGNORENULLS = "F";
            CWMS_LOC_PACKAGE.call_UPDATE_LOCATION(dslContext.configuration(),
                                                  P_LOCATION_ID, P_LOCATION_TYPE, P_ELEVATION, P_ELEV_UNIT_ID, P_VERTICAL_DATUM, P_LATITUDE, P_LONGITUDE,
                                                  P_HORIZONTAL_DATUM, P_PUBLIC_NAME, P_LONG_NAME, P_DESCRIPTION, P_TIME_ZONE_ID, P_COUNTY_NAME, P_STATE_INITIAL,
                                                  P_ACTIVE, P_IGNORENULLS, P_DB_OFFICE_ID );

        });

    }

    private static DSLContext getDslContext(Connection database, String officeId)
    {
        DSLContext dsl =  DSL.using(database, SQLDialect.ORACLE18C);
        CWMS_ENV_PACKAGE.call_SET_SESSION_OFFICE_ID(dsl.configuration(), officeId);
        return dsl;
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
     * @param active   should this location be flagged active or not.
     * @param office   owning office
     * @throws SQLException Any error saving the data
     */
    protected static void createLocation(String location, boolean active, String office) throws SQLException {
        createLocation(location, active, office, "STREAM");
    }

    protected static void createLocation(String location, boolean active, String office, String kind) throws SQLException {
        createLocation(location, active, office,
                0.0, 0.0, "WGS84",
                "UTC", kind);
    }

    protected static void deleteLocation(String location, String office) throws SQLException {
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection((c) -> {
            try (PreparedStatement stmt = c.prepareStatement(deleteLocationQuery)) {
                stmt.setString(1, location);
                stmt.setString(2, office);
                stmt.execute();
            } catch (SQLException ex) {
                throw new RuntimeException("Unable to delete location", ex);
            }
        }, "cwms_20");
    }

    /**
     * Create a timeseries (location must already exist), no data or other meta data will be set.
     * This only creates the timeseries name. Not data or other parameters are set.
     *
     * @param office     owning office
     * @param timeseries timeseries name
     * @throws SQLException
     */
    protected static void createTimeseries(String office, String timeseries) throws SQLException {
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection((c) -> {
            try (PreparedStatement stmt = c.prepareStatement(createTimeseriesQuery)) {
                stmt.setString(1, office);
                stmt.setString(2, timeseries);
                stmt.execute();
            } catch (SQLException ex) {
                if (ex.getErrorCode() == 20003) {
                    return; // TS already exists. that's fine for these tests.
                }
                throw new RuntimeException("Unable to create timeseries", ex);
            }
        }, "cwms_20");
    }


    protected static void createTimeseries(String office, String timeseries, int offset) throws SQLException {
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection((c) -> {
            try (PreparedStatement stmt = c.prepareStatement(createTimeseriesOffsetQuery)) {
                stmt.setString(1, office);
                stmt.setString(2, timeseries);
                stmt.setInt(3, offset);
                stmt.execute();
            } catch (SQLException ex) {
                if (ex.getErrorCode() == 20003) {
                    return; // TS already exists. that's fine for these tests.
                }
                throw new RuntimeException("Unable to create timeseries", ex);
            }
        }, "cwms_20");
    }

    protected static void createTimeseriesWithNewLRTSInterval(String office, String timeseries, int offset) throws SQLException {
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection((c) -> {
            org.jooq.Configuration configuration = DSL.using(c).configuration();
            CWMS_UTIL_PACKAGE.call_SET_SESSION_INFO(configuration,
                    SESSION_USE_LRTS_ID_FORMAT, "T", REQUIRE_NEW_LRTS_ID_FORMAT);
            try (PreparedStatement stmt = c.prepareStatement(createTimeseriesOffsetQuery)) {
                stmt.setString(1, office);
                stmt.setString(2, timeseries);
                stmt.setInt(3, offset);
                stmt.execute();
            } catch (SQLException ex) {
                if (ex.getErrorCode() == 20003) {
                    return; // TS already exists. that's fine for these tests.
                }
                throw new RuntimeException("Unable to create timeseries", ex);
            }
        }, "cwms_20");
    }

    /**
     * Create a stream, saving the data for later deletion.
     *
     * @param stream Stream to create
     * @throws SQLException Any error saving the data
     */
    public static void createStream(Stream stream) throws SQLException {
        CwmsDataApiSetupCallback.getDatabaseLink().connection(c -> {
            DSLContext dsl = dslContext(c, stream.getOfficeId());
            StreamDao streamDao = new StreamDao(dsl);
            streamDao.storeStream(stream, true);
            streamsCreated.add(stream);
        });
    }

    /**
     * Create a basin, saving the data for later deletion.
     *
     * @param basin Basin to create
     * @throws SQLException Any error saving the data
     */
    public static void createBasin(Basin basin) throws SQLException {
        CwmsDataApiSetupCallback.getDatabaseLink().connection(c -> {
            DSLContext dsl = dslContext(c, basin.getBasinId().getOfficeId());
            BasinDao basinDao = new BasinDao(dsl);
            basinDao.storeBasin(basin);
            basinsCreated.add(basin);
        });
    }

    /**
     * If necessary for a specific test add the TEST user to the appropriate office CWMS Group.
     *
     * @param user   CWMS User Name
     * @param group  CWMS Group Name
     * @param office CWMS Office ID
     * @throws Exception Any errors running the sql command
     */
    protected static void addUserToGroup(String user, String group, String office) throws Exception {
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection((c) -> {
            try (PreparedStatement stmt = c.prepareStatement("begin cwms_sec.add_user_to_group(?,?,?); end;")) {
                stmt.setString(1, user);
                stmt.setString(2, group);
                stmt.setString(3, office);
                stmt.execute();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        }, "cwms_20");
    }

    /**
     * If necessary for a specific test remove a user from a CWMS Group.
     *
     * @param user   CWMS User Name
     * @param group  CWMS Group Name
     * @param office CWMS Office ID
     * @throws Exception Any errors running the sql command
     */
    protected static void removeUserFromGroup(String user, String group, String office) throws Exception {
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection((c) -> {
            try (PreparedStatement stmt = c.prepareStatement("begin cwms_sec.remove_user_from_group(?,?,?); end;")) {
                stmt.setString(1, user);
                stmt.setString(2, group);
                stmt.setString(3, office);
                stmt.execute();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        }, "cwms_20");
    }

    /**
     * Get context, setting a specific session office.
     */
    protected static DSLContext dslContext(Connection connection, String officeId) {
        DSLContext dsl = DSL.using(connection, SQLDialect.ORACLE18C);
        CWMS_ENV_PACKAGE.call_SET_SESSION_OFFICE_ID(dsl.configuration(), officeId);
        return dsl;
    }

    /**
     * Get context without setting office
     */
    protected static DSLContext dslContext(Connection connection) {
        DSLContext dsl = DSL.using(connection, SQLDialect.ORACLE18C);
        return dsl;
    }

    protected static int getSchemaVersion() {
        return CwmsDataApiSetupCallback.getSchemaVersion();
    }

    protected static String readResourceFile(String resourcePath) throws IOException {
        URL resource = DataApiTestIT.class.getClassLoader().getResource(resourcePath);
        if (resource == null) {
            throw new IOException("Resource not found: " + resourcePath);
        }
        Path path = new File(resource.getFile()).toPath();
        return String.join("\n", Files.readAllLines(path));
    }

    /**
     * Let the infrastructure know a group is getting created so it can
     * be deleted in cases of test failure.
     *
     * @param group
     */
    protected void registerGroup(LocationGroup group) {
        if (!groupsCreated.contains(group)) {
            groupsCreated.add(group);
        }
    }

    /**
     * Let the infrastructure know a category is getting created so it can
     * be deleted in cases of test failure.
     *
     * @param category
     */
    protected void registerCategory(LocationCategory category) {
        if (!categoriesCreated.contains(category)) {
            categoriesCreated.add(category);
        }
    }

    @AfterEach
    public void cleanupLocationGroups() throws Exception {
        if (this.groupsCreated.isEmpty()) {
            logger.atFine().log("No groups to cleanup.");
            return;
        }
        logger.atInfo().log("Cleaning up groups that tests did not remove.");
        CwmsDatabaseContainer<?> cwmsDb = CwmsDataApiSetupCallback.getDatabaseLink();
        cwmsDb.connection(c -> {
            try (PreparedStatement delGroup = c.prepareStatement("begin cwms_loc.delete_loc_group(?, ?,'T',?); end;")) {
                for (LocationGroup g : groupsCreated) {
                    delGroup.clearParameters();
                    delGroup.setString(1, g.getLocationCategory().getId());
                    delGroup.setString(2, g.getId());
                    delGroup.setString(3, g.getOfficeId());
                    delGroup.executeUpdate();
                }
            } catch (SQLException ex) {
                if (!ex.getLocalizedMessage().toLowerCase().contains("not exist")) {
                    throw new RuntimeException("Failed to remove group in test cleanup/", ex);
                } // otherwise we don't get it was successfully deleted in the test
            }
        });
    }

    @AfterEach
    public void cleanupLocationCategories() throws Exception {
        if (this.categoriesCreated.isEmpty()) {
            logger.atFine().log("No location categories to cleanup.");
            return;
        }
        logger.atInfo().log("Cleaning up location categories that tests did not remove.");
        CwmsDatabaseContainer<?> cwmsDb = CwmsDataApiSetupCallback.getDatabaseLink();
        cwmsDb.connection(c -> {
            try (PreparedStatement delGroup = c.prepareStatement("begin cwms_loc.delete_loc_cat(?,'T',?); end;")) {
                for (LocationCategory cat : categoriesCreated) {
                    delGroup.clearParameters();
                    delGroup.setString(1, cat.getId());
                    delGroup.setString(2, cat.getOfficeId());
                    delGroup.executeUpdate();
                }
            } catch (SQLException ex) {
                if (!ex.getLocalizedMessage().toLowerCase().contains("not exist")) {
                    throw new RuntimeException("Failed to remove group in test cleanup/", ex);
                } // otherwise we don't get it was successfully deleted in the test
            }
        });
    }

    /**
     * Cleanup all basins created by tests that did not remove them.
     * This is a static method so it can be called from the static cleanup methods.
     * This is not assigned an @AfterEach or @AfterAll because the order can be important for test teardown
     *
     * @throws Exception
     */
    public static void cleanupBasins() throws Exception {
        if (basinsCreated.isEmpty()) {
            logger.atFine().log("No basins to cleanup.");
            return;
        }
        logger.atInfo().log("Cleaning up basins test did not remove.");
        CwmsDatabaseContainer<?> cwmsDb = CwmsDataApiSetupCallback.getDatabaseLink();
        cwmsDb.connection(c -> {
            for (Basin basin : basinsCreated) {
                BasinDao basinDao = new BasinDao(dslContext(c, basin.getBasinId().getOfficeId()));
                basinDao.deleteBasin(basin.getBasinId(), DeleteRule.DELETE_ALL);
            }
        });
    }

    /**
     * Cleanup all streams created by tests that did not remove them.
     * This is a static method so it can be called from the static cleanup methods.
     * This is not assigned an @AfterEach or @AfterAll because the order can be important for test teardown
     *
     * @throws Exception
     */
    public static void cleanupStreams() throws Exception {
        if (streamsCreated.isEmpty()) {
            logger.atInfo().log("No streams to cleanup.");
            return;
        }
        logger.atInfo().log("Cleaning up streams test did not remove.");
        CwmsDatabaseContainer<?> cwmsDb = CwmsDataApiSetupCallback.getDatabaseLink();
        cwmsDb.connection(c -> {
            for (Stream stream : streamsCreated) {
                StreamDao streamDao = new StreamDao(dslContext(c, stream.getOfficeId()));
                streamDao.deleteStream(stream.getOfficeId(), stream.getId().getName(), DeleteRule.DELETE_ALL);
            }
        });
    }

    // Resource Template operations

    /**
     * Get a FluentTemplate to handle our operations of setting up the data model before rendering.
     * The non static version provides a default data model based on the active CDA and Database Instance
     *
     * @param resource
     * @return A fluent template that can have its data model be expanded, if needed, and then rendered.
     * @throws Exception
     */
    public FluentTemplate getResourceTemplate(String resource) throws Exception {
        final Template template = loadTemplateFromResource(resource);
        final CwmsDatabaseContainer<?> cwmsDb = CwmsDataApiSetupCallback.getDatabaseLink();
        return new FluentTemplate(template)
                .with("office", cwmsDb.getOfficeId())
                .with("boundingOffice", cwmsDb.getOfficeId())
                .with("dbOffice", cwmsDb.getOfficeId())
                .with("dbTestUser", cwmsDb.getUsername())
                .with("cdaUrl", CwmsDataApiSetupCallback.httpUrl());

    }

    /**
     * Get a FluentTemplate for use outside the integration test system.
     * The default model uses HQ for the office and otherwise in valid values for
     *
     * @param resource
     * @return
     * @throws Exception
     */
    public static FluentTemplate getResourceTemplateStatic(String resource) throws Exception {
        final Template template = loadTemplateFromResource(resource);
        return new FluentTemplate(template)
                .with("office", "HQ")
                .with("boundingOffice", "HQ")
                .with("dbOffice", "HQ")
                .with("dbTestUser", "not-active")
                .with("cdaUrl", "no-url");
    }


    public String getResourceFromTemplate(String resource, Map<String, Object> dataModel) throws Exception {
        return getResourceTemplate(resource).render();
    }

    /**
     * A simple helper to make setting up models to render easier.
     */
    public static class FluentTemplate {
        final Map<String, Object> dataModel = new HashMap<>();
        final Template template;

        public FluentTemplate(Template template) {
            this.template = template;
        }

        /**
         * Add a value to the data model
         *
         * @param fieldName
         * @param field
         * @return
         */
        public FluentTemplate with(String fieldName, Object field) {
            dataModel.put(fieldName, field);
            return this;
        }

        /**
         * Add additional values, they may overwrite defaults to the data model.
         *
         * @param model
         * @return
         */
        public FluentTemplate with(Map<String, Object> model) {
            dataModel.putAll(model);
            return this;
        }

        public FluentTemplate withUser(TestAccounts.KeyUser user) {
            dataModel.put("user", user);
            return this;
        }

        /**
         * Render the template to string using the internal data model
         *
         * @return
         * @throws TemplateException
         * @throws IOException
         */
        public String render() throws TemplateException, IOException {
            final StringWriter out = new StringWriter();
            template.process(dataModel, out);
            return out.toString();
        }

        /**
         * Get a copy of the Data Model. The returned Map is read only.
         *
         * @return
         */
        public Map<String, Object> getModel() {
            return Collections.unmodifiableMap(dataModel);
        }
    }

    /**
     * Many Integration Tests want to use a web user connection in order to setup or verify
     *
     * @param function
     * @throws SQLException
     */
    public static void connectionAsWebUser(Consumer<Connection> function) throws SQLException {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(function, CwmsDataApiSetupCallback.getWebUser());
    }
}
