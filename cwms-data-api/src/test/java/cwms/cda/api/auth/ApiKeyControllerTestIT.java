package cwms.cda.api.auth;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

import cwms.cda.api.DataApiTestIT;
import cwms.cda.data.dto.auth.ApiKey;
import fixtures.TestAccounts;
import fixtures.users.UserSpecSource;
import fixtures.users.annotation.AuthType;
import io.javalin.http.HttpCode;
import io.restassured.specification.RequestSpecification;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.is;

import java.util.ArrayList;
import java.util.List;

/**
 * Forced order is used here to allow better error reporting
 * but to let all the tests run and make sure 
 */
@Tag("integration")
@TestMethodOrder(OrderAnnotation.class)
@TestInstance(Lifecycle.PER_CLASS)
public class ApiKeyControllerTestIT extends DataApiTestIT {

    List<ApiKey> realKeys = new ArrayList<ApiKey>();

    // Create API key, no expiration
    @Order(1)
    @ParameterizedTest
	@ArgumentsSource(UserSpecSource.class)
	@AuthType(user = TestAccounts.KeyUser.SPK_NORMAL)
    public void test_api_key_creation_and_usage(String authType, TestAccounts.KeyUser theUser, RequestSpecification authSpec) {
        final String KEY_NAME = "TestKey1";
        
        final ApiKey key = new ApiKey(theUser.getName(),KEY_NAME);

        ApiKey returnedKey = given()
            .spec(authSpec)
            .contentType("application/json")
            .body(key)
        .when()
            .post("/auth/keys")
        .then().assertThat()
            .log().ifValidationFails()
            .log().everything(true)
            .statusCode(is(HttpCode.CREATED.getStatus()))
            .body("user-id",is(key.getUserId()))
            .body("key-name",is(key.getKeyName()))
            .body("api-key.size()",is(256))
            .extract().as(ApiKey.class);
        realKeys.add(returnedKey);
    }

    // Create API key with expiration

    // List API keys

    // use api key

    // delete api key

    // use api key, see failure
}
