package cwms.cda.api.users;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import cwms.cda.api.DataApiTestIT;
import fixtures.CwmsDataApiSetupCallback;
import fixtures.KeyCloakExtension;
import fixtures.TestAccounts;
import fixtures.users.UserSpecSource;
import fixtures.users.annotation.AuthType;
import io.javalin.http.HttpCode;
import io.restassured.filter.log.LogDetail;
import io.restassured.specification.RequestSpecification;
import java.sql.PreparedStatement;
import java.sql.SQLException;
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
    private static final String USER_LIST_ID = "USER_LIST_CONTROLLER_TEST";
    private static final String USER_LIST_DESC = "Integration test user list";

    @BeforeAll
    static void ensureUserListSchema() throws SQLException {
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection(c -> {
            try {
                executeIgnoreObjectExists(c,
                        "CREATE TABLE AT_USER_LISTS ("
                                + "DB_OFFICE_CODE NUMBER NOT NULL, "
                                + "USER_LIST_ID VARCHAR2(128) NOT NULL, "
                                + "USER_LIST_DESC VARCHAR2(1024), "
                                + "OWNED_BY_USERID VARCHAR2(128), "
                                + "CREATED_AT TIMESTAMP DEFAULT current_timestamp NOT NULL, "
                                + "UPDATED_AT TIMESTAMP)");
                executeIgnoreObjectExists(c,
                        "CREATE UNIQUE INDEX AT_USER_LISTS_PK ON AT_USER_LISTS (USER_LIST_ID)");
                executeIgnoreObjectExists(c,
                        "ALTER TABLE AT_USER_LISTS ADD CONSTRAINT AT_USER_LISTS_PK "
                                + "PRIMARY KEY (USER_LIST_ID) USING INDEX AT_USER_LISTS_PK");
                executeIgnoreObjectExists(c,
                        "ALTER TABLE AT_USER_LISTS ADD CONSTRAINT AT_USER_LISTS_FK1 "
                                + "FOREIGN KEY (DB_OFFICE_CODE) REFERENCES CWMS_OFFICE (OFFICE_CODE)");
                executeIgnoreObjectExists(c,
                        "CREATE OR REPLACE TRIGGER AT_USER_LISTS_TRIG "
                                + "BEFORE INSERT OR UPDATE ON AT_USER_LISTS "
                                + "REFERENCING NEW AS new OLD AS old FOR EACH ROW BEGIN "
                                + ":new.user_list_id := UPPER(:new.user_list_id); "
                                + ":new.owned_by_userid := UPPER(:new.owned_by_userid); "
                                + ":new.updated_at := current_timestamp; END;");
                executeIgnoreObjectExists(c,
                        "CREATE TABLE AT_USER_LIST_MEMBERS ("
                                + "USER_LIST_ID VARCHAR2(128) NOT NULL, "
                                + "USERID VARCHAR2(128) NOT NULL, "
                                + "ADD_DATE TIMESTAMP DEFAULT current_timestamp NOT NULL, "
                                + "ADDED_BY_USERID VARCHAR2(128))");
                executeIgnoreObjectExists(c,
                        "CREATE UNIQUE INDEX AT_USER_LIST_MEMBERS_PK "
                                + "ON AT_USER_LIST_MEMBERS (USER_LIST_ID, USERID)");
                executeIgnoreObjectExists(c,
                        "ALTER TABLE AT_USER_LIST_MEMBERS ADD CONSTRAINT AT_USER_LIST_MEMBERS_PK "
                                + "PRIMARY KEY (USER_LIST_ID, USERID) USING INDEX AT_USER_LIST_MEMBERS_PK");
                executeIgnoreObjectExists(c,
                        "ALTER TABLE AT_USER_LIST_MEMBERS ADD CONSTRAINT AT_USER_LIST_MEMBERS_FK1 "
                                + "FOREIGN KEY (USER_LIST_ID) REFERENCES AT_USER_LISTS (USER_LIST_ID)");
                executeIgnoreObjectExists(c,
                        "CREATE OR REPLACE VIEW AV_USER_LIST_MEMBERS (OFFICE_ID, DB_OFFICE_CODE, "
                                + "USER_LIST_ID, USER_LIST_DESC, OWNED_BY_USERID, USER_ID, FULL_NAME, "
                                + "EMAIL, OFFICE_SYMBOL, MEMBER_OFFICE_ID, ADD_DATE, ADDED_BY_USERID) AS "
                                + "SELECT o.office_id, l.db_office_code, l.user_list_id, l.user_list_desc, "
                                + "l.owned_by_userid, u.user_id, u.full_name, u.email, u.office_symbol, "
                                + "u.office_id AS member_office_id, m.add_date, m.added_by_userid "
                                + "FROM at_user_lists l "
                                + "JOIN cwms_office o ON o.office_code = l.db_office_code "
                                + "JOIN at_user_list_members m ON m.user_list_id = l.user_list_id "
                                + "JOIN av_cwms_user u ON u.user_id = m.userid");
                executeIgnoreInsufficientPrivilege(c, "GRANT SELECT ON AT_USER_LISTS TO CWMS_USER");
                executeIgnoreInsufficientPrivilege(c, "GRANT SELECT ON AT_USER_LIST_MEMBERS TO CWMS_USER");
                executeIgnoreInsufficientPrivilege(c, "GRANT SELECT ON AV_USER_LIST_MEMBERS TO CWMS_USER");
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        }, "cwms_20");
    }

    @BeforeEach
    void createUserList() throws SQLException {
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection(c -> {
            try {
                try (PreparedStatement deleteMembers = c.prepareStatement(
                        "DELETE FROM AT_USER_LIST_MEMBERS WHERE USER_LIST_ID = ?");
                        PreparedStatement deleteList = c.prepareStatement(
                                "DELETE FROM AT_USER_LISTS WHERE USER_LIST_ID = ?");
                        PreparedStatement insertList = c.prepareStatement(
                                "INSERT INTO AT_USER_LISTS (DB_OFFICE_CODE, USER_LIST_ID, USER_LIST_DESC) "
                                        + "SELECT OFFICE_CODE, ?, ? FROM CWMS_OFFICE WHERE OFFICE_ID = ?")) {
                    deleteMembers.setString(1, USER_LIST_ID);
                    deleteMembers.executeUpdate();
                    deleteList.setString(1, USER_LIST_ID);
                    deleteList.executeUpdate();
                    insertList.setString(1, USER_LIST_ID);
                    insertList.setString(2, USER_LIST_DESC);
                    insertList.setString(3, OFFICE);
                    insertList.executeUpdate();
                }
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        }, "cwms_20");
    }

    @AfterEach
    void deleteUserList() throws SQLException {
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection(c -> {
            try {
                try (PreparedStatement deleteMembers = c.prepareStatement(
                        "DELETE FROM AT_USER_LIST_MEMBERS WHERE USER_LIST_ID = ?");
                        PreparedStatement deleteList = c.prepareStatement(
                                "DELETE FROM AT_USER_LISTS WHERE USER_LIST_ID = ?")) {
                    deleteMembers.setString(1, USER_LIST_ID);
                    deleteMembers.executeUpdate();
                    deleteList.setString(1, USER_LIST_ID);
                    deleteList.executeUpdate();
                }
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        }, "cwms_20");
    }

    @ParameterizedTest
    @ArgumentsSource(UserSpecSource.class)
    @AuthType(user = TestAccounts.KeyUser.SPK_NORMAL2)
    void test_get_user_list(String authType, TestAccounts.KeyUser theUser, RequestSpecification authSpec) {
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .spec(authSpec)
            .queryParam("office", OFFICE)
        .when()
            .get("/user/list/{user-list-id}", USER_LIST_ID)
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
            .statusCode(is(HttpCode.OK.getStatus()))
            .body("office-id", equalTo(OFFICE))
            .body("user-list-id", equalTo(USER_LIST_ID))
            .body("description", equalTo(USER_LIST_DESC));
    }

    private static void executeIgnoreObjectExists(java.sql.Connection c, String sql) throws SQLException {
        try (PreparedStatement stmt = c.prepareStatement(sql)) {
            stmt.execute();
        } catch (SQLException ex) {
            String message = ex.getMessage();
            if (message == null || !(message.contains("ORA-00955") || message.contains("ORA-02260")
                    || message.contains("ORA-02261") || message.contains("ORA-02275"))) {
                throw ex;
            }
        }
    }

    private static void executeIgnoreInsufficientPrivilege(java.sql.Connection c, String sql) throws SQLException {
        try (PreparedStatement stmt = c.prepareStatement(sql)) {
            stmt.execute();
        } catch (SQLException ex) {
            String message = ex.getMessage();
            if (message == null || !message.contains("ORA-01031")) {
                throw ex;
            }
        }
    }
}
