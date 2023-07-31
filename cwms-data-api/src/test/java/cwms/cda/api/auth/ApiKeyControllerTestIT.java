package cwms.cda.api.auth;

import org.junit.Test;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

import cwms.cda.api.DataApiTestIT;
import cwms.cda.api.LocationController;
import cwms.cda.data.dto.Location;
import cwms.cda.data.dto.auth.ApiKey;
import cwms.cda.formatters.Formats;
import fixtures.TestAccounts;
import fixtures.users.UserSpecSource;
import fixtures.users.annotation.AuthType;
import io.javalin.http.HttpCode;
import io.restassured.specification.RequestSpecification;

import static cwms.cda.data.dao.JsonRatingUtilsTest.loadResourceAsString;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;

import java.time.ZonedDateTime;
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

    
    private final String KEY_NAME = "TestKey1";

    private static List<ApiKey> realKeys = new ArrayList<ApiKey>();

    // Create API key, no expiration
    @Order(1)
    @ParameterizedTest
	@ArgumentsSource(UserSpecSource.class)
	@AuthType(user = TestAccounts.KeyUser.SPK_NORMAL)
    public void test_api_key_creation_no_expiration(String authType, TestAccounts.KeyUser theUser, RequestSpecification authSpec) {
        final ApiKey key = new ApiKey(theUser.getName(),KEY_NAME);

        ApiKey returnedKey = given()
            .spec(authSpec)
            .contentType("application/json")
            .body(key)
        .when()
            .post("/auth/keys")
        .then()
            .log().ifValidationFails()
            .log().everything(true)
            .statusCode(is(HttpCode.CREATED.getStatus()))
            .body("user-id",is(key.getUserId().toUpperCase()))
            .body("key-name",is(key.getKeyName()))
            .body("api-key.size()",is(256))
            .body("created",not(equalTo(null)))
            .body("expires",is(equalTo(null)))
            .extract().as(ApiKey.class);
        realKeys.add(returnedKey);
    }

    // Create API key with expiration
    @Order(2)
    @ParameterizedTest
	@ArgumentsSource(UserSpecSource.class)
	@AuthType(user = TestAccounts.KeyUser.SPK_NORMAL)
    public void test_api_key_creation_with_expiration(String authType, TestAccounts.KeyUser theUser, RequestSpecification authSpec) {
        final String KEY_NAME = "TestKey1-Expires";
        
        final ApiKey key = new ApiKey(theUser.getName(),KEY_NAME,null,null,ZonedDateTime.now());

        ApiKey returnedKey = given()
            .spec(authSpec)
            .contentType("application/json")
            .body(key)
        .when()
            .post("/auth/keys")
        .then()
            .log().ifValidationFails()
            .log().everything(true)
            .statusCode(is(HttpCode.CREATED.getStatus()))
            .body("user-id",is(key.getUserId().toUpperCase()))
            .body("key-name",is(key.getKeyName()))
            .body("api-key.size()",is(256))            
            .body("created",not(equalTo(null)))
            .body("expires",not(equalTo(null)))
            .extract().as(ApiKey.class);
        realKeys.add(returnedKey);
    }

    // List API keys
    @Order(3)
    @ParameterizedTest
	@ArgumentsSource(UserSpecSource.class)
	@AuthType(user = TestAccounts.KeyUser.SPK_NORMAL)
    public void test_api_key_listing(String authType, TestAccounts.KeyUser theUser, RequestSpecification authSpec) {
        List<ApiKey> keys = 
            given()
                .spec(authSpec)
                .accept(Formats.JSON)
            .when()
                .get("/auth/keys/")
            .then()
                .log().ifValidationFails()
                .log().everything(true)
                .statusCode(is(HttpCode.OK.getStatus()))
            .extract()
                .body()
                .jsonPath()
                .getList(".", ApiKey.class);
        assertFalse(keys.isEmpty(), "No keys were returned.");
        for(ApiKey key: keys) {
            System.out.println(key);
            assertNull(key.getApiKey(), "Api Key not null for key name = " + key.getKeyName());
        }
        /** There may be other keys so we just scan for the keys we know about */
        for(ApiKey createdKey: realKeys) {
            assertContainsKey(createdKey,keys);
        }

        given()
            .spec(authSpec)
            .accept(Formats.JSON)
        .when()
            .get("/auth/keys/{key-name}",KEY_NAME)
        .then()
            .statusCode(HttpCode.OK.getStatus());
    }

    /*
    // use api key
    @Test
    public void test_key_usage() throws Exception {
        createLocation("ApiKey-Test Location",true,"SPK");
        String json = loadResourceAsString("cwms/cda/api/location_create.json");
        Location location = new Location.Builder(LocationController.deserializeLocation(json, Formats.JSON))
                .withOfficeId("SPK")
                .withName(getClass().getSimpleName())
                .build();
        String serializedLocation = JsonV1.buildObjectMapper().writeValueAsString(location);

        KeyUser user = KeyUser.SPK_NORMAL;
        // create location
        given()
            .log().everything(true)
            .accept(Formats.JSON)
            .contentType(Formats.JSON)
            .body(serializedLocation)
            .header("Authorization", user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/locations")
        .then()
            .log().body().log().everything(true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_ACCEPTED));

    }*/

    // delete api key

    // use api key, see failure

    private void assertContainsKey(ApiKey expectedKey, List<ApiKey> returnedSet) {
        for (ApiKey expected: returnedSet) {
            if ( expected.getKeyName().equals(expectedKey.getKeyName())
              && expected.getUserId().equals(expectedKey.getUserId())
              // Don't compare the ApiKey itself, it's not returned
              && expected.getCreated().equals(expectedKey.getCreated())
            ) {
                ZonedDateTime expectedKeyExpires = expectedKey.getExpires();
                ZonedDateTime expectedExpires = expected.getExpires();
                if(expectedKeyExpires == null && expectedExpires == null) {
                    return;
                } else if((expectedKeyExpires != null && expectedExpires != null) 
                        && expectedExpires.isEqual(expectedKeyExpires)) {
                    return;
                }
            }
        }
        fail("Expected key (" + expectedKey.toString() + ") was not found in the returned set of keys.");
    }
}
