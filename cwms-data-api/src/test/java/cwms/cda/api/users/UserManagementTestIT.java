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



}
