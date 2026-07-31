package cwms.cda.api.users;

import static cwms.cda.helpers.DatabaseHelpers.SCHEMA_VERSION.V2026_07_16;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import cwms.cda.api.DataApiTestIT;
import fixtures.CwmsDataApiSetupCallback;
import fixtures.KeyCloakExtension;
import fixtures.TestAccounts;
import fixtures.users.UserSpecSource;
import fixtures.users.annotation.AuthType;
import io.javalin.http.HttpCode;
import io.restassured.http.ContentType;
import io.restassured.specification.RequestSpecification;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Locale;
import mil.army.usace.hec.test.database.CwmsDatabaseContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

@Tag("integration")
@ExtendWith(KeyCloakExtension.class)
public final class UserListControllerTestIT extends DataApiTestIT {
    private static final String OFFICE = "SPK";
    private static final String OTHER_OFFICE = "SWT";
    private static final String USER_LIST_ID = "USER_LIST_CONTROLLER_TEST";
    private static final String SECONDARY_LIST_ID = USER_LIST_ID + "_SECONDARY";
    private static final String USER_LIST_DESC = "Integration test user list";
    private static final String OWNER = "L2HECTEST_VT";

    @BeforeAll
    static void ensureUserListSchema() throws SQLException {
        if (!schemaSupportsUserLists()) {
            return;
        }
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection(c -> {
            try {
                executeIgnoreObjectMissing(c, "DROP VIEW AV_USER_LIST_MEMBERS");
                executeIgnoreObjectMissing(c,
                        "DROP TABLE AT_USER_LIST_MEMBERS CASCADE CONSTRAINTS");
                executeIgnoreObjectMissing(c, "DROP TABLE AT_USER_LISTS CASCADE CONSTRAINTS");
                execute(c,
                        "CREATE TABLE AT_USER_LISTS ("
                                + "DB_OFFICE_CODE NUMBER NOT NULL, "
                                + "USER_LIST_ID VARCHAR2(128) NOT NULL, "
                                + "USER_LIST_DESC VARCHAR2(1024), "
                                + "OWNED_BY_USERID VARCHAR2(128) NOT NULL, "
                                + "CREATED_AT TIMESTAMP DEFAULT current_timestamp NOT NULL, "
                                + "UPDATED_AT TIMESTAMP, "
                                + "CONSTRAINT AT_USER_LISTS_PK PRIMARY KEY "
                                + "(DB_OFFICE_CODE, USER_LIST_ID), "
                                + "CONSTRAINT AT_USER_LISTS_FK1 FOREIGN KEY (DB_OFFICE_CODE) "
                                + "REFERENCES CWMS_OFFICE (OFFICE_CODE), "
                                + "CONSTRAINT AT_USER_LISTS_FK2 FOREIGN KEY (OWNED_BY_USERID) "
                                + "REFERENCES AT_SEC_CWMS_USERS (USERID))");
                execute(c,
                        "CREATE OR REPLACE TRIGGER AT_USER_LISTS_TRIG "
                                + "BEFORE INSERT OR UPDATE ON AT_USER_LISTS "
                                + "REFERENCING NEW AS new OLD AS old FOR EACH ROW BEGIN "
                                + ":new.user_list_id := UPPER(:new.user_list_id); "
                                + ":new.owned_by_userid := UPPER(:new.owned_by_userid); "
                                + ":new.updated_at := current_timestamp; END;");
                execute(c,
                        "CREATE TABLE AT_USER_LIST_MEMBERS ("
                                + "DB_OFFICE_CODE NUMBER NOT NULL, "
                                + "USER_LIST_ID VARCHAR2(128) NOT NULL, "
                                + "USERID VARCHAR2(128) NOT NULL, "
                                + "ADD_DATE TIMESTAMP DEFAULT current_timestamp NOT NULL, "
                                + "ADDED_BY_USERID VARCHAR2(128), "
                                + "CONSTRAINT AT_USER_LIST_MEMBERS_PK PRIMARY KEY "
                                + "(DB_OFFICE_CODE, USER_LIST_ID, USERID), "
                                + "CONSTRAINT AT_USER_LIST_MEMBERS_FK1 "
                                + "FOREIGN KEY (DB_OFFICE_CODE, USER_LIST_ID) "
                                + "REFERENCES AT_USER_LISTS (DB_OFFICE_CODE, USER_LIST_ID), "
                                + "CONSTRAINT AT_USER_LIST_MEMBERS_FK2 FOREIGN KEY (USERID) "
                                + "REFERENCES AT_SEC_CWMS_USERS (USERID))");
                execute(c,
                        "CREATE OR REPLACE VIEW AV_USER_LIST_MEMBERS (OFFICE_ID, DB_OFFICE_CODE, "
                                + "USER_LIST_ID, USER_LIST_DESC, OWNED_BY_USERID, USER_ID, FULL_NAME, "
                                + "EMAIL, OFFICE_SYMBOL, MEMBER_OFFICE_ID, ADD_DATE, ADDED_BY_USERID) AS "
                                + "SELECT o.office_id, l.db_office_code, l.user_list_id, l.user_list_desc, "
                                + "l.owned_by_userid, u.user_id, u.full_name, u.email, u.office_symbol, "
                                + "u.office_id AS member_office_id, m.add_date, m.added_by_userid "
                                + "FROM at_user_lists l "
                                + "JOIN cwms_office o ON o.office_code = l.db_office_code "
                                + "JOIN at_user_list_members m "
                                + "ON m.db_office_code = l.db_office_code "
                                + "AND m.user_list_id = l.user_list_id "
                                + "JOIN av_cwms_user u ON u.user_id = m.userid");
                executeIgnoreInsufficientPrivilege(c,
                        "GRANT SELECT, INSERT, UPDATE, DELETE ON AT_USER_LISTS TO CWMS_USER");
                executeIgnoreInsufficientPrivilege(c,
                        "GRANT SELECT, INSERT, UPDATE, DELETE ON AT_USER_LIST_MEMBERS TO CWMS_USER");
                executeIgnoreInsufficientPrivilege(c,
                        "GRANT SELECT ON AV_USER_LIST_MEMBERS TO CWMS_USER");
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        }, "cwms_20");
    }

    @BeforeEach
    void createUserList() throws SQLException {
        if (!schemaSupportsUserLists()) {
            return;
        }
        cleanUserLists();
        insertUserList(OFFICE, USER_LIST_ID, OWNER);
    }

    @AfterEach
    void deleteUserLists() throws SQLException {
        if (!schemaSupportsUserLists()) {
            return;
        }
        cleanUserLists();
    }

    @ParameterizedTest
    @ArgumentsSource(UserSpecSource.class)
    @AuthType(user = TestAccounts.KeyUser.SPK_NORMAL2)
    void test_full_user_list_crud(String authType, TestAccounts.KeyUser user,
            RequestSpecification authSpec) {
        assumeSupportedSchema();
        given().spec(authSpec).queryParam("office", OFFICE)
        .when().get("/user/list/{user-list-id}", USER_LIST_ID)
        .then().statusCode(HttpCode.OK.getStatus())
                .body("office-id", equalTo(OFFICE))
                .body("user-list-id", equalTo(USER_LIST_ID))
                .body("description", equalTo(USER_LIST_DESC))
                .body("owned-by-user-id", equalTo(OWNER));

        given().spec(authSpec).queryParam("office", OFFICE)
            .when().get("/user/list")
            .then().statusCode(HttpCode.OK.getStatus())
                .body("user-lists.user-list-id", hasItem(USER_LIST_ID));

        given().spec(authSpec).contentType(ContentType.JSON)
                .body("{\"office-id\":\"SPK\",\"user-list-id\":\""
                        + SECONDARY_LIST_ID + "\",\"description\":\"Created through CDA\"}")
            .when().post("/user/list")
            .then().statusCode(HttpCode.CREATED.getStatus())
                .body("owned-by-user-id",
                        equalTo(user.getName().toUpperCase(Locale.ROOT)));

        given().spec(authSpec).contentType(ContentType.JSON).queryParam("office", OFFICE)
                .body("{\"description\":\"Updated through CDA\"}")
        .when().patch("/user/list/{user-list-id}", SECONDARY_LIST_ID)
        .then().statusCode(HttpCode.OK.getStatus())
                .body("description", equalTo("Updated through CDA"));

        given().spec(authSpec).queryParam("office", OFFICE)
        .when().delete("/user/list/{user-list-id}", SECONDARY_LIST_ID)
        .then().statusCode(HttpCode.NO_CONTENT.getStatus());
    }

    @ParameterizedTest
    @ArgumentsSource(UserSpecSource.class)
    @AuthType(user = TestAccounts.KeyUser.SPK_NORMAL2)
    void test_membership_crud(String authType, TestAccounts.KeyUser user,
            RequestSpecification authSpec) {
        assumeSupportedSchema();
        given().spec(authSpec).queryParam("search", "l2hec")
        .when().get("/user/list-member-candidates")
        .then().statusCode(HttpCode.OK.getStatus())
                .body("candidates.user-id", hasItem("L2HECTEST"));

        given().spec(authSpec).contentType(ContentType.JSON).queryParam("office", OFFICE)
                .body("{\"user-id\":\"L2HECTEST\"}")
        .when().post("/user/list/{user-list-id}/members", USER_LIST_ID)
        .then().statusCode(HttpCode.CREATED.getStatus())
                .body("user-id", equalTo("L2HECTEST"));

        given().spec(authSpec).queryParam("office", OFFICE)
        .when().get("/user/list/{user-list-id}/members", USER_LIST_ID)
        .then().statusCode(HttpCode.OK.getStatus())
                .body("members.user-id", hasItem("L2HECTEST"));

        given().spec(authSpec).queryParam("office", OFFICE)
        .when().delete("/user/list/{user-list-id}/members/{user-id}",
                USER_LIST_ID, "L2HECTEST")
        .then().statusCode(HttpCode.NO_CONTENT.getStatus());
    }

    @ParameterizedTest
    @ArgumentsSource(UserSpecSource.class)
    @AuthType(user = TestAccounts.KeyUser.SPK_NORMAL2)
    void test_duplicate_and_validation_errors(String authType, TestAccounts.KeyUser user,
            RequestSpecification authSpec) {
        assumeSupportedSchema();
        given().spec(authSpec).contentType(ContentType.JSON)
                .body("{\"office-id\":\"SPK\",\"user-list-id\":\""
                        + USER_LIST_ID + "\"}")
        .when().post("/user/list")
        .then().statusCode(HttpCode.CONFLICT.getStatus());

        given().spec(authSpec).contentType(ContentType.JSON)
                .body("{\"office-id\":\"SPK\",\"user-list-id\":\"bad list id\"}")
        .when().post("/user/list")
        .then().statusCode(HttpCode.BAD_REQUEST.getStatus());
    }

    @ParameterizedTest
    @ArgumentsSource(UserSpecSource.class)
    @AuthType(user = TestAccounts.KeyUser.SPK_NORMAL)
    void test_non_admin_cannot_mutate(String authType, TestAccounts.KeyUser user,
            RequestSpecification authSpec) {
        assumeSupportedSchema();
        given().spec(authSpec).contentType(ContentType.JSON)
                .body("{\"office-id\":\"SPK\",\"user-list-id\":\""
                        + SECONDARY_LIST_ID + "\"}")
        .when().post("/user/list")
        .then().statusCode(HttpCode.FORBIDDEN.getStatus());
    }

    @ParameterizedTest
    @ArgumentsSource(UserSpecSource.class)
    @AuthType(user = TestAccounts.KeyUser.SPK_NORMAL2)
    void test_same_list_id_is_available_in_multiple_offices(String authType,
            TestAccounts.KeyUser user, RequestSpecification authSpec) throws SQLException {
        assumeSupportedSchema();
        insertUserList(OTHER_OFFICE, USER_LIST_ID, "M5HECTEST");

        given().spec(authSpec).queryParam("office", OTHER_OFFICE)
        .when().get("/user/list/{user-list-id}", USER_LIST_ID)
        .then().statusCode(HttpCode.OK.getStatus())
                .body("office-id", equalTo(OTHER_OFFICE))
                .body("user-list-id", equalTo(USER_LIST_ID));
    }

    private void insertUserList(String office, String listId, String owner) throws SQLException {
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection(c -> {
            try (PreparedStatement insertList = c.prepareStatement(
                    "INSERT INTO AT_USER_LISTS "
                            + "(DB_OFFICE_CODE, USER_LIST_ID, USER_LIST_DESC, OWNED_BY_USERID) "
                            + "SELECT OFFICE_CODE, ?, ?, ? FROM CWMS_OFFICE WHERE OFFICE_ID = ?")) {
                insertList.setString(1, listId);
                insertList.setString(2, USER_LIST_DESC);
                insertList.setString(3, owner);
                insertList.setString(4, office);
                insertList.executeUpdate();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        }, "cwms_20");
    }

    private void cleanUserLists() throws SQLException {
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection(c -> {
            try {
                execute(c, "DELETE FROM AT_USER_LIST_MEMBERS WHERE USER_LIST_ID LIKE '"
                        + USER_LIST_ID + "%'");
                execute(c, "DELETE FROM AT_USER_LISTS WHERE USER_LIST_ID LIKE '"
                        + USER_LIST_ID + "%'");
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        }, "cwms_20");
    }

    private static boolean schemaSupportsUserLists() {
        return getSchemaVersion() >= V2026_07_16.numeric();
    }

    private static void assumeSupportedSchema() {
        assumeTrue(schemaSupportsUserLists());
    }

    private static void execute(java.sql.Connection connection, String sql) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.execute();
        }
    }

    private static void executeIgnoreObjectMissing(java.sql.Connection connection, String sql)
            throws SQLException {
        try {
            execute(connection, sql);
        } catch (SQLException ex) {
            if (ex.getMessage() == null || !ex.getMessage().contains("ORA-00942")) {
                throw ex;
            }
        }
    }

    private static void executeIgnoreInsufficientPrivilege(java.sql.Connection connection,
            String sql) throws SQLException {
        try {
            execute(connection, sql);
        } catch (SQLException ex) {
            if (ex.getMessage() == null || !ex.getMessage().contains("ORA-01031")) {
                throw ex;
            }
        }
    }
}
