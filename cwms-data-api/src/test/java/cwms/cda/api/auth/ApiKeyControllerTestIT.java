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
import cwms.cda.data.dao.AuthDao;
import cwms.cda.data.dto.Location;
import cwms.cda.data.dto.auth.ApiKey;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.json.JsonV1;
import fixtures.TestAccounts;
import fixtures.TestAccounts.KeyUser;
import fixtures.users.UserSpecSource;
import fixtures.users.annotation.AuthType;
import io.javalin.http.HttpCode;
import io.restassured.filter.log.LogDetail;
import io.restassured.specification.RequestSpecification;

import static cwms.cda.data.dao.JsonRatingUtilsTest.loadResourceAsString;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
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
    private final String EXPIRED_KEY_NAME = "TestKey2-Expired";

    private static List<ApiKey> realKeys = new ArrayList<>();
    private static List<ApiKey> firstReturnedKeys = new ArrayList<>();

    // Create API key, no expiration
    @Order(1)
    @ParameterizedTest
	@ArgumentsSource(UserSpecSource.class)
	@AuthType(user = TestAccounts.KeyUser.SPK_NORMAL)
    public void test_api_key_creation_no_expiration(String authType, TestAccounts.KeyUser theUser, RequestSpecification authSpec) {
        final ApiKey key = new ApiKey(theUser.getName(),KEY_NAME);

        ApiKey returnedKey =
            given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .spec(authSpec)
                .contentType("application/json")
                .body(key)
            .when()
                .post("/auth/keys")
            .then()
                .log().ifValidationFails(LogDetail.ALL,true)
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
        final ApiKey expiredKey = new ApiKey(key.getUserId(),EXPIRED_KEY_NAME,null,null,ZonedDateTime.now().minusMinutes(1L));

        ApiKey returnedKey =
            given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .spec(authSpec)
                .contentType("application/json")
                .body(key)
            .when()
                .post("/auth/keys")
            .then()
                .log().ifValidationFails(LogDetail.ALL,true)
                .statusCode(is(HttpCode.CREATED.getStatus()))
                .body("user-id",is(key.getUserId().toUpperCase()))
                .body("key-name",is(key.getKeyName()))
                .body("api-key.size()",is(256))            
                .body("created",not(equalTo(null)))
                .body("expires",not(equalTo(null)))
                .extract().as(ApiKey.class);
        realKeys.add(returnedKey);

        returnedKey =
            given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .spec(authSpec)
                .contentType("application/json")
                .body(expiredKey)
            .when()
                .post("/auth/keys")
            .then()
                .log().ifValidationFails(LogDetail.ALL,true)
                .statusCode(is(HttpCode.CREATED.getStatus()))
                .body("user-id",is(expiredKey.getUserId().toUpperCase()))
                .body("key-name",is(expiredKey.getKeyName()))
                .body("api-key.size()",is(256))            
                .body("created",not(equalTo(null)))
                .body("expires",not(equalTo(null)))
                .extract().as(ApiKey.class);
        realKeys.add(returnedKey);


        final String bodyWithSpecificExpiresFormat = "{\"user-id\": \"" + theUser.getName() + "\",\"key-name\": \"foo\",\"api-key\": \"string\",\"expires\": \"2023-09-23T14:20:00.908Z\"}";
        returnedKey = 
            given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .spec(authSpec)
                .contentType("application/json")
                .body(bodyWithSpecificExpiresFormat)
            .when()
                .post("/auth/keys")
            .then()
                .log().ifValidationFails(LogDetail.ALL,true)
                .statusCode(is(HttpCode.CREATED.getStatus()))
                .body("user-id",is(expiredKey.getUserId().toUpperCase()))
                .body("key-name",is("foo"))
                .body("api-key.size()",is(256))
                .body("created",not(equalTo(null)))
                .body("expires",not(equalTo(null)))
                .extract().as(ApiKey.class);
        realKeys.add(returnedKey);
    }

    @Order(3)
    @ParameterizedTest
	@ArgumentsSource(UserSpecSource.class)
	@AuthType(user = TestAccounts.KeyUser.SPK_NORMAL)
    public void test_api_key_creation_not_other_user(String authType, TestAccounts.KeyUser theUser, RequestSpecification authSpec) {
        final String KEY_NAME = "TestKey1-Expires";

        // This doesn't need to be a user in the database, the check is done before it gets there
        final ApiKey key = new ApiKey("Bob",KEY_NAME,null,null,ZonedDateTime.now());

        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .spec(authSpec)
            .contentType("application/json")
            .body(key)
        .when()
            .post("/auth/keys")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
            .statusCode(is(HttpCode.UNAUTHORIZED.getStatus()))
            .body("message",is(AuthDao.ONLY_OWN_KEY_MESSAGE));
    }

    // List API keys
    @Order(4)
    @ParameterizedTest
	@ArgumentsSource(UserSpecSource.class)
	@AuthType(user = TestAccounts.KeyUser.SPK_NORMAL)
    public void test_api_key_listing(String authType, TestAccounts.KeyUser theUser, RequestSpecification authSpec) {
        List<ApiKey> keys = 
            given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .spec(authSpec)
                .accept(Formats.JSON)
            .when()
                .get("/auth/keys/")
            .then()
                .log().ifValidationFails(LogDetail.ALL,true)
                .statusCode(is(HttpCode.OK.getStatus()))
            .extract()
                .body()
                .jsonPath()
                .getList(".", ApiKey.class);
        assertFalse(keys.isEmpty(), "No keys were returned.");
        firstReturnedKeys.addAll(keys);
        /** There may be other keys so we just scan for the keys we know about */
        for(ApiKey createdKey: realKeys) {
            assertContainsKey(createdKey,keys);
        }

        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .spec(authSpec)
            .accept(Formats.JSON)
        .when()
            .get("/auth/keys/{key-name}",KEY_NAME)
        .then()
            .statusCode(HttpCode.OK.getStatus());
    }

    
    // use api key
    @Test
    @Order(5)
    public void test_key_usage() throws Exception {
        createLocation("ApiKey-Test Location",true,"SPK");
        String json = loadResourceAsString("cwms/cda/api/location_create.json");
        Location location = new Location.Builder(Formats.parseContent(Formats.parseHeader(Formats.JSON), json, Location.class))
                .withOfficeId("SPK")
                .withName(getClass().getSimpleName())
                .build();
        String serializedLocation = JsonV1.buildObjectMapper().writeValueAsString(location);

        final KeyUser user = KeyUser.SPK_NORMAL;
        // create location
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSON)
            .contentType(Formats.JSON)
            .body(serializedLocation)
            .header("Authorization", user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/locations")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
            .assertThat()
            .statusCode(is(HttpCode.ACCEPTED.getStatus()));

        final ApiKey expiredKey = realKeys.stream()
                                          .filter(k -> k.getKeyName().equals(EXPIRED_KEY_NAME))
                                          .findFirst()
                                          .orElseThrow(() -> new Exception("expired key not in real keys list."));
        final Location updateLocation = new Location.Builder(location)
                        .withCountyName("Sacramento")
                        .build();
        final String serializedUpdateLocation = JsonV1.buildObjectMapper().writeValueAsString(updateLocation);
        // fail to use expired key
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSON)
            .contentType(Formats.JSON)
            .body(serializedUpdateLocation)
            .header("Authorization", "apikey " + expiredKey.getApiKey())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .put("/locations/{location-id}",updateLocation.getName())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
            .assertThat()
            .statusCode(is(HttpCode.UNAUTHORIZED.getStatus()));
        // fail to use no existent key
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSON)
            .contentType(Formats.JSON)
            .body(serializedUpdateLocation)
            .header("Authorization", "apikey This_Key_doesn't_exist")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .put("/locations/{location-id}",updateLocation.getName())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
            .assertThat()
            .statusCode(is(HttpCode.UNAUTHORIZED.getStatus()));
    }

    // delete api keys
    // List API keys
    @Order(6)
    @ParameterizedTest
	@ArgumentsSource(UserSpecSource.class)
	@AuthType(user = TestAccounts.KeyUser.SPK_NORMAL)
    public void test_api_key_delete_key(String authType, TestAccounts.KeyUser theUser, RequestSpecification authSpec) {
        for(ApiKey key: realKeys) {
            given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .spec(authSpec)
                .accept(Formats.JSON)
            .when()
                .delete("/auth/keys/{key-name}",key.getKeyName())
            .then()
                .log().ifValidationFails(LogDetail.ALL,true)
                .statusCode(is(HttpCode.NO_CONTENT.getStatus()));

            // try to retrieve the key
            given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .spec(authSpec)
                .accept(Formats.JSON)
            .when()
                .get("/auth/keys/{key-name}",key.getKeyName())
            .then()
                .log().ifValidationFails(LogDetail.ALL,true)
                .statusCode(is(HttpCode.NOT_FOUND.getStatus()));
        }

        List<ApiKey> keys =
            given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .spec(authSpec)
                .accept(Formats.JSON)
            .when()
                .get("/auth/keys/")
            .then()
                .log().ifValidationFails(LogDetail.ALL,true)
                .statusCode(is(HttpCode.OK.getStatus()))
            .extract()
                .body()
                .jsonPath()
                .getList(".", ApiKey.class);
        assertTrue(keys.size() < firstReturnedKeys.size(), "Keys were not deleted.");
    }

    private void assertContainsKey(ApiKey expectedKey, List<ApiKey> returnedSet) {
        for (ApiKey expected: returnedSet) {
            if ( expected.getKeyName().equals(expectedKey.getKeyName())
              && expected.getUserId().equals(expectedKey.getUserId())
              // Don't compare the ApiKey itself, it's not returned
              && expected.getCreated().equals(expectedKey.getCreated())
            ) {
                ZonedDateTime expectedKeyExpires = expectedKey.getExpires();
                ZonedDateTime expectedExpires = expected.getExpires();
                if (expectedKeyExpires == null && expectedExpires == null) {
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
