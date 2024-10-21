/*
 * MIT License
 *
 * Copyright (c) 2023 Hydrologic Engineering Center
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

import com.google.common.flogger.FluentLogger;
import cwms.cda.data.dto.Location;
import cwms.cda.data.dto.LocationCategory;
import cwms.cda.data.dto.LocationGroup;
import fixtures.CwmsDataApiSetupCallback;
import fixtures.IntegrationTestNameGenerator;
import fixtures.TestAccounts;
import fixtures.users.MockCwmsUserPrincipalImpl;
import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import mil.army.usace.hec.test.database.CwmsDatabaseContainer;
import org.apache.catalina.Manager;
import org.apache.catalina.SessionEvent;
import org.apache.catalina.SessionListener;
import org.apache.catalina.session.StandardSession;
import org.apache.commons.io.IOUtils;
import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;

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
/**
 * Helper class to manage cycling tests multiple times against a database.
 * NOTE: Not thread safe, do not run parallel tests. That may be future work though.
 */
@DisplayNameGeneration(IntegrationTestNameGenerator.class)
@Tag("integration")
@ExtendWith(CwmsDataApiSetupCallback.class)
public class DataApiTestIT {
    private static FluentLogger logger = FluentLogger.forEnclosingClass();

    protected static String createLocationQuery = null;
    protected static String createTimeseriesQuery = null;
    protected static String createTimeseriesOffsetQuery = null;
    protected final static String registerApiKey = "insert into at_api_keys(userid,key_name,apikey) values(UPPER(?),?,?)";
    protected final static String removeApiKeys = "delete from at_api_keys where UPPER(userid) = UPPER(?) and apikey = ?";

    protected final static Configuration freemarkerConfig = new Configuration(Configuration.VERSION_2_3_32);

    private ArrayList<LocationGroup> groupsCreated = new ArrayList<>();
    private ArrayList<LocationCategory> categoriesCreated = new ArrayList<>();

    static {
        freemarkerConfig.setClassForTemplateLoading(DataApiTestIT.class, "/");
    }

    /**
     * Reads in SQL data and runs it as CWMS_20. Assumes single statement. That single statement
     * can be an anonymous function if more detail is required.
     * @param resource Resource path to SQL file.  Example: "cwms/cda/data/sql/create_location.sql"
     * @throws Exception
     */
    protected static void loadSqlDataFromResource(String resource) throws Exception {
        String sql = IOUtils.toString(
                    DataApiTestIT.class
                    .getClassLoader()
                    .getResourceAsStream(resource),"UTF-8");
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection((c)-> {
            try(PreparedStatement stmt = c.prepareStatement(sql)) {
                stmt.execute();
            } catch (SQLException ex) {
                throw new RuntimeException("Unable to process SQL",ex);
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
                                    .getResourceAsStream("cwms/cda/data/sql_templates/create_location.sql"),"UTF-8"
                            );
        createTimeseriesQuery = IOUtils.toString(
                                TimeseriesControllerTestIT.class
                                    .getClassLoader()
                                    .getResourceAsStream("cwms/cda/data/sql_templates/create_timeseries.sql"),"UTF-8"
                            );

        createTimeseriesOffsetQuery = IOUtils.toString(
                TimeseriesControllerTestIT.class
                        .getClassLoader()
                        .getResourceAsStream("cwms/cda/data/sql_templates/create_timeseries_offset.sql"),"UTF-8"
        );

    }

    /**
     * Register all known users credentials in the database as appropriate.
     * @throws Exception
     */
    @BeforeAll
    public static void register_users() throws Exception {
        try {
            final Manager tsm = CwmsDataApiSetupCallback.getTestSessionManager();
            CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
            for(TestAccounts.KeyUser user: TestAccounts.KeyUser.values()) {
                if(user.getApikey() == null) {
                    continue;
                }
                db.connection((c)-> {
                    try(PreparedStatement stmt = c.prepareStatement(registerApiKey)) {
                        stmt.setString(1,user.getName());
                        stmt.setString(2,user.getName()+"TestKey");
                        stmt.setString(3,user.getApikey());
                        stmt.execute();
                    } catch (SQLException ex) {
                        throw new RuntimeException("Unable to register user:" + user.getName() ,ex);
                    }
                },"cwms_20");

                StandardSession session = (StandardSession)tsm.createSession(user.getJSessionId());
                if(session == null) {
                    throw new RuntimeException("Test Session Manager is unusable.");
                }
                MockCwmsUserPrincipalImpl mcup = new MockCwmsUserPrincipalImpl(user.getName(), user.getEdipi(), user.getRoles());
                session.setAuthType("CLIENT-CERT");
                session.setPrincipal(mcup);
                session.activate();
                session.addSessionListener(new SessionListener() {

                    @Override
                    public void sessionEvent(SessionEvent event) {
                        logger.atInfo().log("Got event of type: %s",event.getType());
                        logger.atInfo().log("Session is:",event.getSession().toString());
                    }

                });
                CwmsDataApiSetupCallback.getSsoValve()
                                     .wrappedRegister(user.getJSessionId(), mcup, "CLIENT-CERT", null,null);
            }
        } catch(RuntimeException ex) {
            throw new Exception("User registration failed",ex);
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
            CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
            for(TestAccounts.KeyUser user: TestAccounts.KeyUser.values()) {
                db.connection((c)-> {
                    try(PreparedStatement stmt = c.prepareStatement(removeApiKeys)) {
                        stmt.setString(1,user.getName());
                        stmt.setString(2,user.getApikey());
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
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        Location loc = new Location.Builder(location,
                                            kind,
                                            ZoneId.of(timeZone),
                                            latitude,
                                            longitude,
                                            horizontalDatum,
                                            office)
                                    .withActive(active)
                                    .build();
        if (LocationCleanup.locationsCreated.contains(loc)) {
            return; // we already have this location registered
        }

        db.connection((c)-> {
            try(PreparedStatement stmt = c.prepareStatement(createLocationQuery)) {
                stmt.setString(1,location);
                stmt.setString(2,active ? "T" : "F");
                stmt.setString(3,office);
                stmt.setString(4,timeZone);
                stmt.setDouble(5,latitude);
                stmt.setDouble(6,longitude);
                stmt.setString(7,horizontalDatum);
                stmt.setString(8,kind);
                stmt.execute();
                LocationCleanup.locationsCreated.add(loc);
            } catch (SQLException ex) {
                throw new RuntimeException("Unable to create location",ex);
            }
        }, "cwms_20");
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
        createLocation(location,active,office, "STREAM");
    }

    protected static void createLocation(String location, boolean active, String office, String kind) throws SQLException {
        createLocation(location,active,office,
                       0.0,0.0,"WGS84",
                       "UTC", kind);
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
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection((c)-> {
            try(PreparedStatement stmt = c.prepareStatement(createTimeseriesQuery)) {
                stmt.setString(1,office);
                stmt.setString(2,timeseries);
                stmt.execute();
            } catch (SQLException ex) {
                if (ex.getErrorCode() == 20003) {
                    return; // TS already exists. that's fine for these tests.
                }
                throw new RuntimeException("Unable to create timeseries",ex);
            }
        }, "cwms_20");
    }


    protected static void createTimeseries(String office, String timeseries, int offset) throws SQLException {
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection((c)-> {
            try(PreparedStatement stmt = c.prepareStatement(createTimeseriesOffsetQuery)) {
                stmt.setString(1, office);
                stmt.setString(2, timeseries);
                stmt.setInt(3, offset);
                stmt.execute();
            } catch (SQLException ex) {
                if (ex.getErrorCode() == 20003) {
                    return; // TS already exists. that's fine for these tests.
                }
                throw new RuntimeException("Unable to create timeseries",ex);
            }
        }, "cwms_20");
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
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection( (c) -> {
            try(PreparedStatement stmt = c.prepareStatement("begin cwms_sec.add_user_to_group(?,?,?); end;")) {
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
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection( (c) -> {
            try(PreparedStatement stmt = c.prepareStatement("begin cwms_sec.remove_user_from_group(?,?,?); end;")) {
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
            logger.atInfo().log("No groups to cleanup.");
            return;
        }
        logger.atInfo().log("Cleaning up groups test did not remove.");
        CwmsDatabaseContainer<?> cwmsDb = CwmsDataApiSetupCallback.getDatabaseLink();
        cwmsDb.connection( c-> {
            try (PreparedStatement delGroup = c.prepareStatement("begin cwms_loc.delete_loc_group(?,'T',?); end;")) {
                for (LocationGroup g: groupsCreated) {
                    delGroup.clearParameters();
                    delGroup.setString(1,g.getId());
                    delGroup.setString(2,g.getOfficeId());
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
            logger.atInfo().log("No location categories to cleanup.");
            return;
        }
        logger.atInfo().log("Cleaning up location categories that tests did not remove.");
        CwmsDatabaseContainer<?> cwmsDb = CwmsDataApiSetupCallback.getDatabaseLink();
        cwmsDb.connection( c-> {
            try (PreparedStatement delGroup = c.prepareStatement("begin cwms_loc.delete_loc_cat(?,'T',?); end;")) {
                for (LocationCategory cat: categoriesCreated) {
                    delGroup.clearParameters();
                    delGroup.setString(1,cat.getId());
                    delGroup.setString(2,cat.getOfficeId());
                    delGroup.executeUpdate();
                }
            } catch (SQLException ex) {
                if (!ex.getLocalizedMessage().toLowerCase().contains("not exist")) {
                    throw new RuntimeException("Failed to remove group in test cleanup/", ex);
                } // otherwise we don't get it was successfully deleted in the test
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
                    .with("dbTestUser",cwmsDb.getUsername())
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
         * @return
         */
        public Map<String, Object> getModel() {
            return Collections.unmodifiableMap(dataModel);
        }
    }

    /**
     * Many Integration Tests want to use a web user connection in order to setup or verify
     * @param function
     * @throws SQLException
     */
    public static void connectionAsWebUser(Consumer<Connection> function) throws SQLException {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(function, CwmsDataApiSetupCallback.getWebUser());
    }
}
