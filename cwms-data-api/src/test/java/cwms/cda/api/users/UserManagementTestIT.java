package cwms.cda.api.users;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import cwms.cda.api.Controllers;
import cwms.cda.formatters.Formats;
import fixtures.users.UserSpecSource;
import fixtures.CwmsDataApiSetupCallback;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

import cwms.cda.api.DataApiTestIT;
import cwms.cda.data.dto.auth.users.User;
import cwms.cda.data.dto.auth.users.Users;
import fixtures.KeyCloakExtension;
import fixtures.TestAccounts;
import fixtures.users.annotation.AuthType;
import io.javalin.http.HttpCode;
import io.restassured.filter.log.LogDetail;
import io.restassured.specification.RequestSpecification;

@Tag("integration")
@ExtendWith(KeyCloakExtension.class)
public class UserManagementTestIT extends DataApiTestIT {

    private static final String LOCATION = "SOME_LOCATION";
    private static final String MISSING_USER = "DOES_NOT_EXIST";
    private static final String SWT = "SWT";
    private static final String SPK = "SPK";

    @ParameterizedTest
    @ArgumentsSource(UserSpecSource.class)
    @AuthType(user = TestAccounts.KeyUser.SPK_NORMAL2)
    void test_contact_fields_in_list_detail_and_profile(String authType, TestAccounts.KeyUser theUser,
            RequestSpecification authSpec) throws Exception {
        String[] original = CwmsDataApiSetupCallback.getDatabaseLink().connection(c -> {
            try (PreparedStatement stmt = c.prepareStatement(
                    "select fullname, email, principle_name from cwms_20.at_sec_cwms_users where userid = upper(?)")) {
                stmt.setString(1, theUser.getName());
                try (ResultSet rs = stmt.executeQuery()) {
                    assertTrue(rs.next());
                    return new String[]{rs.getString(1), rs.getString(2), rs.getString(3)};
                }
            } catch (SQLException ex) {
                throw new RuntimeException("Unable to read synthetic user contact details", ex);
            }
        }, "cwms_20");
        try {
            for (boolean populated : new boolean[]{true, false}) {
                setContactDetails(theUser.getName(), populated ? "Alex Example" : null,
                        populated ? "alex.example@example.com" : null, "test-contact-principal");
                for (String path : new String[]{"/users/" + theUser.getName(), "/users/test-contact-principal",
                        "/user/profile", "/users"}) {
                    String prefix = path.equals("/users") ? "users[0]." : "";
                    given().spec(authSpec).accept(Formats.JSON)
                            .queryParam(Controllers.USERNAME_LIKE, "^" + theUser.getName() + "$")
                            .queryParam("page-size", 1)
                    .when().get(path)
                    .then().log().ifValidationFails(LogDetail.ALL, true)
                            .statusCode(HttpCode.OK.getStatus())
                            .body(prefix + "user-name", equalToIgnoringCase(theUser.getName()))
                            .body(prefix + "principal", equalTo("test-contact-principal"))
                            .body(prefix + "first-name", populated ? equalTo("Alex") : nullValue())
                            .body(prefix + "last-name", populated ? equalTo("Example") : nullValue())
                            .body(prefix + "email", populated ? equalTo("alex.example@example.com") : nullValue());
                }
            }
        } finally {
            setContactDetails(theUser.getName(), original[0], original[1], original[2]);
        }
    }

    private static void setContactDetails(String userName, String fullName, String email, String principal)
            throws Exception {
        CwmsDataApiSetupCallback.getDatabaseLink().connection(c -> {
            try (PreparedStatement stmt = c.prepareStatement(
                    "update cwms_20.at_sec_cwms_users set fullname = ?, email = ?, principle_name = ? "
                            + "where userid = upper(?)")) {
                stmt.setString(1, fullName);
                stmt.setString(2, email);
                stmt.setString(3, principal);
                stmt.setString(4, userName);
                assertEquals(1, stmt.executeUpdate());
                if (!c.getAutoCommit()) {
                    c.commit();
                }
            } catch (SQLException ex) {
                throw new RuntimeException("Unable to update synthetic user contact details", ex);
            }
        }, "cwms_20");
    }

    @BeforeAll
    public static void setupLocations() throws Exception {
        createLocation(LOCATION, true, SWT);
        createLocation(LOCATION, true, SPK);
    }

    @AfterAll
    public static void tearDownLocations() {
        try {
            deleteLocation(LOCATION, SWT);
        } catch (Exception e) {
            // ignore
        }
        try {
            deleteLocation(LOCATION, SPK);
        } catch (Exception e) {
            // ignore
        }
    }
    
    @ParameterizedTest
	@ArgumentsSource(UserSpecSource.class)
	@AuthType(user = TestAccounts.KeyUser.SPK_NORMAL)
    void test_get_my_info(String authType, TestAccounts.KeyUser theUser, RequestSpecification authSpec) {

        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .spec(authSpec)
        .when()
            .get("/user/profile")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
            .statusCode(is(HttpCode.OK.getStatus()))
            .body("user-name", equalTo(theUser.getName().toUpperCase()))
            .body("cac-auth", equalTo(true))
            .body("roles.SPK",contains("All Users", "CWMS Users", "TS ID Creator"))
            ;
    }


    @ParameterizedTest
	@ArgumentsSource(UserSpecSource.class)
	@AuthType(user = TestAccounts.KeyUser.SPK_NORMAL2)
    void test_manage_user(String authType, TestAccounts.KeyUser theUser, RequestSpecification authSpec) {
        final TestAccounts.KeyUser userUnderTest = TestAccounts.KeyUser.SPK_NORMAL;
        // we can get a specific user
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .spec(authSpec)
        .when()
            .get("/users/{user-name}", userUnderTest.getName())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
            .statusCode(is(HttpCode.OK.getStatus()))
            .body("user-name", equalTo(userUnderTest.getName().toUpperCase()))
            .body("roles.SPK",hasItems("All Users", "CWMS Users", "TS ID Creator"))
            ;
        // we can add a role
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .spec(authSpec)
            .body("[\"CCP Mgr\"]")
        .when()
            .post("/user/{user-name}/roles/{office-id}", userUnderTest.getName(), theUser.getOperatingOffice())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
            .statusCode(is(HttpCode.NO_CONTENT.getStatus()))
        ;

        // the role was added
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .spec(authSpec)
        .when()
            .get("/users/{user-name}", userUnderTest.getName())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
            .statusCode(is(HttpCode.OK.getStatus()))
            .body("user-name", equalTo(userUnderTest.getName().toUpperCase()))
            .body("roles.SPK",hasItem("CCP Mgr"))
        ;

        // remove the role
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .spec(authSpec)
            .body("[\"CCP Mgr\"]")
        .when()
            .delete("/user/{user-name}/roles/{office-id}", userUnderTest.getName(), theUser.getOperatingOffice())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
            .statusCode(is(HttpCode.NO_CONTENT.getStatus()))
        ;

        // the role was removed
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .spec(authSpec)
        .when()
            .get("/users/{user-name}", userUnderTest.getName())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
            .statusCode(is(HttpCode.OK.getStatus()))
            .body("user-name", equalTo(userUnderTest.getName().toUpperCase()))
            .body("roles.SPK",not(hasItem("CCP Mgr")))
        ;
    }

    @ParameterizedTest
    @ArgumentsSource(UserSpecSource.class)
    @AuthType(user = TestAccounts.KeyUser.SPK_NORMAL2)
    void test_get_missing_user_not_found(String authType, TestAccounts.KeyUser theUser,
            RequestSpecification authSpec) {
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .spec(authSpec)
        .when()
            .get("/users/{user-name}", MISSING_USER)
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
            .statusCode(is(HttpCode.NOT_FOUND.getStatus()))
            .body("message", equalTo("User not found: " + MISSING_USER))
        ;
    }

    @ParameterizedTest
	@ArgumentsSource(UserSpecSource.class)
	@AuthType(user = TestAccounts.KeyUser.SPK_NORMAL2)
    void test_list_roles(String authType, TestAccounts.KeyUser theUser, RequestSpecification authSpec) {
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .spec(authSpec)
        .when()
            .get("/roles")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
            .statusCode(is(HttpCode.OK.getStatus()))
            .body("", hasItem("VT Mgr"))
        ;
    }


    @ParameterizedTest
	@ArgumentsSource(UserSpecSource.class)
	@AuthType(user = TestAccounts.KeyUser.SPK_NORMAL2)
    void test_list_users(String authType, TestAccounts.KeyUser theUser, RequestSpecification authSpec) {
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .spec(authSpec)
        .when()
            .get("/users")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
            .statusCode(is(HttpCode.OK.getStatus()))
            .body("users.find { it.'user-name' == 'M5HECTEST' }.roles.SWT", hasItem("TS ID Creator"))
        ;
    }

    @ParameterizedTest
	@ArgumentsSource(UserSpecSource.class)
	@AuthType(user = TestAccounts.KeyUser.SPK_NORMAL2)
    void test_list_users_pagination(String authType, TestAccounts.KeyUser theUser, RequestSpecification authSpec) {
        final ArrayList<User> users = new ArrayList<User>();
        Users tmp = given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .spec(authSpec)
            .queryParam("page-size", 2)
        .when()
            .get("/users")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
            .statusCode(is(HttpCode.OK.getStatus()))
            //.body("users.find { it.'user-name' == 'M5HECTEST' }.roles.SWT", hasItem("TS ID Creator"))
            .extract().as(Users.class);
        ;

        users.addAll(tmp.getUsers());
        while (tmp.getNextPage() != null) {
            tmp = given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .spec(authSpec)
                .queryParam("page",tmp.getNextPage())
            .when()
                .get("/users")
            .then()
                .log().ifValidationFails(LogDetail.ALL,true)
                .statusCode(is(HttpCode.OK.getStatus()))
                //.body("users.find { it.'user-name' == 'M5HECTEST' }.roles.SWT", hasItem("TS ID Creator"))
                .extract().as(Users.class);
            ;
            users.addAll(tmp.getUsers());
        }

        assertEquals(tmp.getTotal(), users.size(), "Returned user size does not match provided total.");
        final User m5hectest = users.stream().filter(u -> u.getUserName().equals("M5HECTEST")).findFirst().orElse(null);
        assertNotNull(m5hectest, "Could not retrieve expected user.");
        assertTrue(m5hectest.getRoles().get("SWT").contains("TS ID Creator"));
    }

    @ParameterizedTest
    @ArgumentsSource(UserSpecSource.class)
    @AuthType(user = TestAccounts.KeyUser.SPK_NORMAL2)
    void test_list_users_pagination_regex_filter(String authType, TestAccounts.KeyUser theUser, RequestSpecification authSpec) {
        final ArrayList<User> users = new ArrayList<User>();
        Users tmp = given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .spec(authSpec)
                .queryParam(Controllers.USERNAME_LIKE, "l2hectest.*")
                .queryParam("page-size", 2)
        .when()
                .get("/users")
        .then()
                .log().ifValidationFails(LogDetail.ALL,true)
                .statusCode(is(HttpCode.OK.getStatus()))
                .extract().as(Users.class);

        users.addAll(tmp.getUsers());
        assertNotNull(tmp.getNextPage(), "Expected multiple pages of results for pagination test with regex filter.");

        while (tmp.getNextPage() != null) {
            tmp = given()
                    .log().ifValidationFails(LogDetail.ALL, true)
                    .spec(authSpec)
                    .queryParam("page",tmp.getNextPage())
            .when()
                    .get("/users")
            .then()
                    .log().ifValidationFails(LogDetail.ALL,true)
                    .statusCode(is(HttpCode.OK.getStatus()))
                    .extract().as(Users.class);
            users.addAll(tmp.getUsers());
        }

        assertEquals(tmp.getTotal(), users.size(), "Returned user size does not match provided total.");
        final User l2hectest = users.stream().filter(u -> u.getUserName().equals("L2HECTEST")).findFirst().orElse(null);
        assertNotNull(l2hectest, "Could not retrieve expected user.");
        assertTrue(l2hectest.getRoles().get("SPK").contains("TS ID Creator"));
    }

    @ParameterizedTest
    @ArgumentsSource(UserSpecSource.class)
    @AuthType(user = TestAccounts.KeyUser.SPK_NORMAL2)
    void test_list_users_username_regex_filter(String authType, TestAccounts.KeyUser theUser, RequestSpecification authSpec) {

        Users users = given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .spec(authSpec)
            .queryParam(Controllers.USERNAME_LIKE, "*")
        .when()
            .get("/users")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
            .statusCode(is(HttpCode.OK.getStatus()))
            .extract().as(Users.class);

        assertNotNull(users);
        assertNotNull(users.getUsers());
        assertFalse(users.getUsers().isEmpty(), "Expected at least one user returned for regex filter");


        users = given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .spec(authSpec)
            .queryParam(Controllers.USERNAME_LIKE, "^M5HECTEST$")
        .when()
            .get("/users")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
            .statusCode(is(HttpCode.OK.getStatus()))
            .extract().as(Users.class);

        assertNotNull(users);
        assertNotNull(users.getUsers());
        assertFalse(users.getUsers().isEmpty(), "Expected at least one user returned for regex filter");
        // Ensure the filtered list contains only the expected username
        users.getUsers().forEach(u -> assertEquals("M5HECTEST", u.getUserName()));

        users = given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .spec(authSpec)
            .queryParam(Controllers.USERNAME_LIKE, "M3DOESNTEXIST")
        .when()
            .get("/users")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
            .statusCode(is(HttpCode.OK.getStatus()))
            .extract().as(Users.class);

        assertNotNull(users);
        assertNotNull(users.getUsers());
        assertTrue(users.getUsers().isEmpty(), "Expected no users returned for regex filter");
    }


    @ParameterizedTest
    @ArgumentsSource(UserSpecSource.class)
    @AuthType(user = TestAccounts.KeyUser.SPK_NO_ROLES)
    void test_get_my_info_no_roles_forbidden(String authType, TestAccounts.KeyUser theUser, RequestSpecification authSpec) {

        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .spec(authSpec)
        .when()
            .get("/user/profile")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
            .statusCode(is(HttpCode.FORBIDDEN.getStatus()))
            ;
    }

    @ParameterizedTest
    @ArgumentsSource(UserSpecSource.class)
    @AuthType(user = TestAccounts.KeyUser.SPK_CAC_BUT_NOT_CWMS_USER)
    void test_get_my_info_not_cwms_user_succeeds(String authType, TestAccounts.KeyUser theUser, RequestSpecification authSpec) {
        // SPK_NOT_CWMS_USER has cac_auth, but not "CWMS Users" role.
        // It should still be able to retrieve its own profile.
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .spec(authSpec)
            .accept(Formats.JSON)
        .when()
            .get("/user/profile")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
            .statusCode(is(HttpCode.OK.getStatus()))
            .body("user-name", equalToIgnoringCase(theUser.getName()))
        ;
    }

    @ParameterizedTest
    @ArgumentsSource(UserSpecSource.class)
    @AuthType(user = TestAccounts.KeyUser.SPK_NORMAL)
    void test_cross_office_update_unauthorized(String authType, TestAccounts.KeyUser theUser, RequestSpecification authSpec) throws Exception {
        // SPK_NORMAL has roles in SPK, but not in SWT.
        // Try to delete a location in SWT
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .spec(authSpec)
            .queryParam("office", SWT)
        .when()
            .delete("/locations/" + LOCATION)
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
            .statusCode(is(HttpCode.UNAUTHORIZED.getStatus()))
        ;
    }

    @ParameterizedTest
    @ArgumentsSource(UserSpecSource.class)
    @AuthType(user = TestAccounts.KeyUser.SPK_NO_ROLES)
    void test_roleless_update_forbidden(String authType, TestAccounts.KeyUser theUser, RequestSpecification authSpec) throws Exception {
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .spec(authSpec)
            .queryParam("office", SPK)
        .when()
            .delete("/locations/" + LOCATION)
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
            .statusCode(is(HttpCode.FORBIDDEN.getStatus()))
        ;
    }

    @ParameterizedTest
    @ArgumentsSource(UserSpecSource.class)
    @AuthType(user = TestAccounts.KeyUser.SPK_NORMAL)
    void test_unauthorized_administrative_actions(String authType, TestAccounts.KeyUser theUser, RequestSpecification authSpec) {
        // SPK_NORMAL has CWMS Users but NOT CWMS User Admins.
        // Try to list all users (requires CWMS User Admins)
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .spec(authSpec)
        .when()
            .get("/users")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
            .statusCode(is(HttpCode.FORBIDDEN.getStatus()))
        ;
    }

    @Test
    void test_list_users_fails_if_no_auth() {
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
        .when()
            .get("/users")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
            .statusCode(is(HttpCode.UNAUTHORIZED.getStatus()))
        ;
    }

}
