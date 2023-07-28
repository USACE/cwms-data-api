package cwms.cda.api.auth;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

import cwms.cda.data.dto.auth.ApiKey;
import fixtures.TestAccounts;
import fixtures.users.UserSpecSource;
import fixtures.users.annotation.AuthType;
import io.javalin.http.HttpCode;
import io.restassured.specification.RequestSpecification;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.is;

public class ApiKeyControllerTestIT {
    // Create API key, no expiration

    // Create API key with expiration

    // List API keys

    // delete api key

    // use api key

    @ParameterizedTest
	@ArgumentsSource(UserSpecSource.class)
	@AuthType(user = TestAccounts.KeyUser.SPK_NORMAL)
    public void test_api_key_creation_and_usage(String authType, TestAccounts.KeyUser theUser, RequestSpecification authSpec) {
        final String KEY_NAME = "TestKey1";
        
        final ApiKey key = new ApiKey(theUser.getName(),KEY_NAME);

        given()
            .spec(authSpec)
            .contentType("application/json")
            .body(key)
        .when()
            .post("/auth/key")
        .then().assertThat()        
            .statusCode(is(HttpCode.CREATED));
    }
}
