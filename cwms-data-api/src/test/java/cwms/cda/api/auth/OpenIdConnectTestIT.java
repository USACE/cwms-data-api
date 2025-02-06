package cwms.cda.api.auth;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;
import java.util.logging.Logger;


import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import cwms.cda.api.DataApiTestIT;
import fixtures.KeyCloakExtension;
import io.javalin.http.HttpCode;
import io.restassured.filter.log.LogDetail;
import io.restassured.http.ContentType;

@Tag("integration")
@ExtendWith(KeyCloakExtension.class)
public class OpenIdConnectTestIT extends DataApiTestIT {
    private static final Logger logger = Logger.getLogger(OpenIdConnectTestIT.class.getName());
        

    @Test
    void test_keycloak_user_is_created() {

        Optional<String> token = KeyCloakExtension.tokenForUser("q0hecoidc", "q0hecoidc");
        assertTrue(token.isPresent());

        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .header("Authorization", "Bearer " + token.get())
            .queryParam("name-mask","asdf")
        .when()
            .get("/properties")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
            // 403 here means the user was created, but has no privileges as the user was just created.
            .statusCode(is(HttpCode.FORBIDDEN.getStatus()));
    }

    @Test
    void test_keycloak_user_can_operate() {
        Optional<String> token = KeyCloakExtension.tokenForUser("m5hectest", "m5hectest");
        assertTrue(token.isPresent());

        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .header("Authorization", "Bearer " + token.get())
            .queryParam("name-mask","asdf")
        .when()
            .get("/properties")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
            // 403 here means the user was created, but has no privileges as the user was just created.
            .statusCode(is(HttpCode.OK.getStatus()));
    }
}
