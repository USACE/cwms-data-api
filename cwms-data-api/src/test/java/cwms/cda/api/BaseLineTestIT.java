package cwms.cda.api;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import javax.servlet.http.HttpServletResponse;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import fixtures.CwmsDataApiSetupCallback;
import io.restassured.filter.log.LogDetail;

/**
 * Location for tests that aren't specifically related to a given endpoint.
 */
@Tag("integration")
@ExtendWith(CwmsDataApiSetupCallback.class)
class BaseLineTestIT extends DataApiTestIT {

    @ParameterizedTest
    @ValueSource(strings = {"/blobs/", "/timeseries", "/levels"})
    void test_options_handling_known_url(String url) throws Exception {
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .options(url)
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .header("Access-Control-Allow-Methods", equalTo("GET, POST, PUT, DELETE, OPTIONS"))
            .header("Access-Control-Allow-Headers", equalTo("Content-Type, Authorization"));
    }
    
    @ParameterizedTest
    @ValueSource(strings = {"/flurgle/", "/blah/", "/levels-i-do-not-exist"})
    void test_options_handling_unknown_url(String url) throws Exception {
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .options(url)
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .header("Access-Control-Allow-Methods", nullValue())
            .header("Access-Control-Allow-Headers", nullValue());
    }
}
