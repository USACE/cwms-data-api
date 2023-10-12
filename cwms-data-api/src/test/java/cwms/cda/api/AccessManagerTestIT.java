package cwms.cda.api;

import java.io.IOException;

import javax.servlet.http.HttpServletResponse;

import fixtures.TestAccounts;
import fixtures.users.UserSpecSource;
import fixtures.users.annotation.AuthType;
import io.restassured.specification.RequestSpecification;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

import static cwms.cda.data.dao.JsonRatingUtilsTest.loadResourceAsString;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@Tag("integration")
public class AccessManagerTestIT extends DataApiTestIT {

    @ParameterizedTest
    @ArgumentsSource(UserSpecSource.class)
    @AuthType(userTypes = {AuthType.UserType.GUEST_AND_PRIVS})
    public void can_getOne_with_user(String authType, TestAccounts.KeyUser user, RequestSpecification authSpec) {
        given()
            .spec(authSpec)
            .contentType("application/json")
            .queryParam("office", "SPK")
            .queryParam("names", "AR*")
            .queryParam("unit", "EN")
        .when()
            .get(  "/locations")
        .then().assertThat()
            .statusCode(is(200));
    }

    @ParameterizedTest
    @ArgumentsSource(UserSpecSource.class)
    @AuthType(user = TestAccounts.KeyUser.GUEST)
    public void cant_create_without_user(String authType, TestAccounts.KeyUser user, RequestSpecification authSpec) throws IOException {
        String json = loadResourceAsString("cwms/cda/api/location_create.json");
        assertNotNull(json);

        given()
                .spec(authSpec)
                .contentType("application/json")
                .queryParam("office", "SPK")
                .body(json)
                .when()
                .post(  "/locations")
                .then()
                .assertThat().statusCode(is(401));
    }

    @ParameterizedTest
    @ArgumentsSource(UserSpecSource.class)
    @AuthType(userTypes = { AuthType.UserType.PRIVS })
    public void can_create_with_user(String authType, TestAccounts.KeyUser user, RequestSpecification authSpec) throws IOException {
        String json = loadResourceAsString("cwms/cda/api/location_create_spk.json");
        assertNotNull(json);

        given()
            .contentType("application/json")
            .queryParam("office", "SPK")
            .spec(authSpec)
            .body(json)
        .when()
            .post(  "/locations/")
        .then()
            .assertThat().statusCode(HttpServletResponse.SC_ACCEPTED);
    }

    @ParameterizedTest
    @ArgumentsSource(UserSpecSource.class)
    @AuthType(userTypes = { AuthType.UserType.NO_PRIVS })
    public void cant_create_with_user_without_role(String authType, TestAccounts.KeyUser user, RequestSpecification authSpec) throws IOException {
        String json = loadResourceAsString("cwms/cda/api/location_create.json");
        assertNotNull(json);

        given()
            .contentType("application/json")
            .queryParam("office", "SPK")
            .spec(authSpec)
            .body(json)
        .when()
            .post(  "/locations")
        .then()
            .assertThat().statusCode(is(403));
    }
}
