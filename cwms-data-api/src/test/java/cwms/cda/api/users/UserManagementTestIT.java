package cwms.cda.api.users;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;
import java.util.logging.Logger;


import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

import cwms.cda.ApiServlet;
import cwms.cda.api.DataApiTestIT;
import cwms.cda.data.dto.auth.users.User;
import fixtures.KeyCloakExtension;
import fixtures.TestAccounts;
import fixtures.users.UserSpecSource;
import fixtures.users.annotation.AuthType;
import io.javalin.http.HttpCode;
import io.restassured.filter.log.LogDetail;
import io.restassured.specification.RequestSpecification;

@Tag("integration")
@ExtendWith(KeyCloakExtension.class)
public class UserManagementTestIT extends DataApiTestIT {
    private static final Logger logger = Logger.getLogger(UserManagementTestIT.class.getName());
    
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
            .body("roles.SPK",contains("All Users", "CWMS Users", "TS ID Creator"))
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
}
