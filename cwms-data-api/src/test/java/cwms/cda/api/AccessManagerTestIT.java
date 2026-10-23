package cwms.cda.api;

import javax.servlet.http.HttpServletResponse;

import fixtures.TestAccounts;
import fixtures.users.UserSpecSource;
import fixtures.users.annotation.AuthType;
import fixtures.users.annotation.AuthType.UserType;
import io.restassured.filter.log.LogDetail;
import io.restassured.specification.RequestSpecification;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@Tag("integration")
final class AccessManagerTestIT extends DataApiTestIT {

    @ParameterizedTest
    @ArgumentsSource(UserSpecSource.class)
    @AuthType(userTypes = {AuthType.UserType.GUEST_AND_PRIVS})
    void can_getOne_with_user(String authType, TestAccounts.KeyUser user, RequestSpecification authSpec) {
        given()
			.log().ifValidationFails(LogDetail.ALL,true)
            .spec(authSpec)
            .contentType("application/json")
            .queryParam("office", "SPK")
            .queryParam("names", "AR*")
            .queryParam("unit", "EN")
        .when()
            .get(  "/locations")
        .then().assertThat()
			.log().ifValidationFails(LogDetail.ALL,true)
            .statusCode(is(HttpServletResponse.SC_OK));
    }

    @ParameterizedTest
    @ArgumentsSource(UserSpecSource.class)
    @AuthType(user = TestAccounts.KeyUser.GUEST)
    void cant_create_without_user(String authType, TestAccounts.KeyUser user, RequestSpecification authSpec) throws Exception {
        final String json  = getResourceTemplate("cwms/cda/api/location_create.json.freemarker")
                                .withUser(user)
                                .render();
        assertNotNull(json);

        given()
			.log().ifValidationFails(LogDetail.ALL,true)
			.spec(authSpec)
			.contentType("application/json")
			.queryParam("office", "SPK")
			.body(json)
		.when()
			.post(  "/locations")
		.then()
			.assertThat()
			.log().ifValidationFails(LogDetail.ALL,true)
			.statusCode(is(HttpServletResponse.SC_UNAUTHORIZED));
    }

    @ParameterizedTest
    @ArgumentsSource (UserSpecSource.class)
    @AuthType(userTypes = {UserType.GUEST_AND_PRIVS})
    void test_ts_auth_not_in_dmz_list(String authType, TestAccounts.KeyUser user, RequestSpecification authSpec) throws Exception
    {

        final String tsName = "Restricted.Stage.Inst.1Hour.0.AuthTest";
        final String office = user.getOperatingOffice() == null ? "SPK" : user.getOperatingOffice();
        createLocation("Restricted", true, office);
        createTimeseries(office, tsName, false);
        given()
			.log().ifValidationFails(LogDetail.ALL,true)
			.spec(authSpec)
			.contentType("application/json")
			.queryParam("office", office)
            .queryParam("name", tsName)
		.when()
			.get(  "/timeseries")
		.then()
			.assertThat()
			.log().ifValidationFails(LogDetail.ALL,true)
			.statusCode(is(
                user == TestAccounts.KeyUser.GUEST ||
                user == TestAccounts.KeyUser.NONE ||
                user == TestAccounts.KeyUser.SPK_CAC_BUT_NOT_CWMS_USER
                     ? HttpServletResponse.SC_UNAUTHORIZED : HttpServletResponse.SC_OK));
    }

    @ParameterizedTest
    @ArgumentsSource (UserSpecSource.class)
    @AuthType(userTypes = {UserType.GUEST_AND_PRIVS})
    void test_ts_auth_in_dmz_list(String authType, TestAccounts.KeyUser user, RequestSpecification authSpec) throws Exception
    {

        final String tsName = "NotRestricted.Stage.Inst.1Hour.0.AuthTest";
        final String office = user.getOperatingOffice() == null ? "SPK" : user.getOperatingOffice();
        createLocation("NotRestricted", true, office);
        createTimeseries(office, tsName, true);
        given()
			.log().ifValidationFails(LogDetail.ALL,true)
			.spec(authSpec)
			.contentType("application/json")
			.queryParam("office", office)
            .queryParam("name", tsName)
		.when()
			.get(  "/timeseries")
		.then()
			.assertThat()
			.log().ifValidationFails(LogDetail.ALL,true)
			.statusCode(is(HttpServletResponse.SC_OK));
    }

    @ParameterizedTest
    @ArgumentsSource(UserSpecSource.class)
    @AuthType(userTypes = { AuthType.UserType.PRIVS })
    void can_create_with_user(String authType, TestAccounts.KeyUser user, RequestSpecification authSpec) throws Exception {
        final String json  = getResourceTemplate("cwms/cda/api/location_create.json.freemarker")
                                .withUser(user)
                                .render();
        assertNotNull(json);

        //ensure the location doesn't exist before creation
        try {
            given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .contentType("application/json")
                .queryParam("office", user.getOperatingOffice())
                .spec(authSpec)
            .when()
                .delete(  "/locations/LOC_TEST")
            .then()
                .log().ifValidationFails(LogDetail.ALL,true);
        } catch (Exception ex) {}

        //try to create the location
        given()
			.log().ifValidationFails(LogDetail.ALL,true)
            .contentType("application/json")
            .queryParam("office", user.getOperatingOffice())
            .queryParam("fail-if-exists", "false")
            .spec(authSpec)
            .body(json)
        .when()
            .post(  "/locations/")
        .then()
			.log().ifValidationFails(LogDetail.ALL,true)
            .assertThat()
			.statusCode(is(HttpServletResponse.SC_CREATED));

        //cleanup location after creation
        try {
            given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .contentType("application/json")
                .queryParam("office", user.getOperatingOffice())
                .spec(authSpec)
            .when()
                .delete(  "/locations/LOC_TEST")
            .then()
                .log().ifValidationFails(LogDetail.ALL,true);
        } catch (Exception ex) {}

    }

    @ParameterizedTest
    @ArgumentsSource(UserSpecSource.class)
    @AuthType(userTypes = { AuthType.UserType.NO_PRIVS })
    void cant_create_with_user_without_role(String authType, TestAccounts.KeyUser user, RequestSpecification authSpec) throws Exception {
        final String json  = getResourceTemplate("cwms/cda/api/location_create.json.freemarker")
                                .with("user", user).render();

        given()
			.log().ifValidationFails(LogDetail.ALL,true)
            .contentType("application/json")
            .queryParam("office", "SPK")
            .spec(authSpec)
            .body(json)
        .when()
            .post(  "/locations")
        .then()
			.log().ifValidationFails(LogDetail.ALL,true)
            .assertThat()
			.statusCode(is(HttpServletResponse.SC_FORBIDDEN));
    }
}
