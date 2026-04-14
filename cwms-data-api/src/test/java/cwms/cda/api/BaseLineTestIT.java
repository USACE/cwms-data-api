package cwms.cda.api;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.is;

import java.util.UUID;

import javax.servlet.http.HttpServletResponse;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import cwms.cda.logging.TraceIdFilter;
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
            .statusCode(is(HttpServletResponse.SC_OK));
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
            .statusCode(is(HttpServletResponse.SC_OK));
    }


    @Test
    void test_bad_trace_id_value() {
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .header("X-Trace-Id", "I'm a bad value")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .options("/levels")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_BAD_REQUEST))
            .body("message", is(TraceIdFilter.MSG));
    }


    @Test
    void test_provided_uuid_returned_on_error() {
        final var traceId = UUID.randomUUID().toString();
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .header("X-Trace-Id", traceId)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/levels/Bob")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_BAD_REQUEST))
            .body("incidentIdentifier", is(traceId));
    }
}
