package cwms.cda.api;

import static cwms.cda.api.Controllers.BEGIN;
import static cwms.cda.api.Controllers.END;
import static cwms.cda.api.Controllers.NAME;
import static cwms.cda.api.Controllers.OFFICE;
import static cwms.cda.api.Controllers.STORE_RULE;
import static cwms.cda.api.Controllers.UNIT;
import static cwms.cda.api.Controllers.DUPLICATE_DATE_TIME_RESOLUTION;
import static io.restassured.RestAssured.given;
import static io.restassured.config.JsonConfig.jsonConfig;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

import cwms.cda.data.dao.StoreRule;
import cwms.cda.formatters.Formats;
import fixtures.TestAccounts;
import io.restassured.RestAssured;
import io.restassured.filter.log.LogDetail;
import io.restassured.path.json.config.JsonPathConfig;
import io.restassured.response.ValidatableResponse;
import io.restassured.specification.RequestSpecification;
import jakarta.servlet.http.HttpServletResponse;
import java.time.Instant;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

@Tag("integration")
final class TimeSeriesDuplicateDateTimeResolutionTestIT extends DataApiTestIT {

    private static final String OFFICE_ID = "SPK";
    private static final String LOCATION = "DuplicateDateTime";
    private static final TestAccounts.KeyUser USER = TestAccounts.KeyUser.SPK_NORMAL;
    private static final long NOON = Instant.parse("2024-01-01T12:00:00Z").toEpochMilli();
    private static final long ONE_PM = Instant.parse("2024-01-01T13:00:00Z").toEpochMilli();

    @BeforeAll
    static void createTestLocation() throws Exception {
        createLocation(LOCATION, true, OFFICE_ID);
    }

    private static String tsId(String suffix) {
        return LOCATION + ".Flow.Inst.0.0." + suffix;
    }

    /**
     * Three values in the 12:00 minute (two at sub-minute offsets) supplied out of time order,
     * plus a non-duplicate value at 13:00.
     */
    private static String duplicatePayload(String tsId) {
        return "{\"name\": \"" + tsId + "\", \"office-id\": \"" + OFFICE_ID + "\", \"units\": \"cfs\","
                + "\"values\": ["
                + "[" + (NOON + 40_000) + ", 10.0, 3],"
                + "[" + NOON + ", 20.0, 3],"
                + "[" + ONE_PM + ", 50.0, 0],"
                + "[" + (NOON + 10_000) + ", 60.0, 5]"
                + "]}";
    }

    private static String singlePayload(String tsId, double value) {
        return "{\"name\": \"" + tsId + "\", \"office-id\": \"" + OFFICE_ID + "\", \"units\": \"cfs\","
                + "\"values\": [[" + NOON + ", " + value + ", 0]]}";
    }

    private static RequestSpecification write(String body) {
        return given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(Formats.JSONV2)
                .contentType(Formats.JSONV2)
                .body(body)
                .header("Authorization", USER.toHeaderValue())
                .queryParam(OFFICE, OFFICE_ID);
    }

    private static ValidatableResponse retrieve(String tsId) {
        return given()
                .config(RestAssured.config().jsonConfig(
                        jsonConfig().numberReturnType(JsonPathConfig.NumberReturnType.DOUBLE)))
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(Formats.JSONV2)
                .header("Authorization", USER.toHeaderValue())
                .queryParam(OFFICE, OFFICE_ID)
                .queryParam(UNIT, "cfs")
                .queryParam(NAME, tsId)
                .queryParam(BEGIN, "2024-01-01T11:00:00Z")
                .queryParam(END, "2024-01-01T14:00:00Z")
            .when()
                .get("/timeseries/")
            .then()
                .log().ifValidationFails(LogDetail.ALL, true);
    }

    @Test
    void test_default_rejects_duplicates_and_stores_nothing() {
        String tsId = tsId("dupe-default");
        write(duplicatePayload(tsId))
            .when()
                .post("/timeseries/")
            .then()
                .log().ifValidationFails(LogDetail.ALL, true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_BAD_REQUEST))
                .body("details.message", containsString("2024-01-01T12:00:00Z"));

        retrieve(tsId)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_NOT_FOUND));
    }

    @Test
    void test_invalid_value_is_bad_request() {
        write(duplicatePayload(tsId("dupe-invalid")))
                .queryParam(DUPLICATE_DATE_TIME_RESOLUTION, "median")
            .when()
                .post("/timeseries/")
            .then()
                .log().ifValidationFails(LogDetail.ALL, true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_BAD_REQUEST));
    }

    @ParameterizedTest
    @CsvSource({
        "first,   10.0, 3",
        "LAST,    60.0, 5",
        // mean of 10, 20, 60; quality 3 is most common
        "average, 30.0, 3",
    })
    void test_post_resolves_duplicates(String resolution, double expectedValue, int expectedQuality)
            throws Exception {
        String tsId = tsId("dupe-post-" + resolution.toLowerCase());
        write(duplicatePayload(tsId))
                .queryParam(DUPLICATE_DATE_TIME_RESOLUTION, resolution)
            .when()
                .post("/timeseries/")
            .then()
                .log().ifValidationFails(LogDetail.ALL, true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK));

        assignTsToDmzInclude(OFFICE_ID, tsId);

        retrieve(tsId)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
                .body("values.size()", is(2))
                .body("values[0][0]", is(NOON))
                .body("values[0][1]", closeTo(expectedValue, 0.0001))
                .body("values[0][2]", is(expectedQuality))
                .body("values[1][1]", closeTo(50.0, 0.0001));
    }

    @Test
    void test_patch_resolves_duplicates() throws Exception {
        String tsId = tsId("dupe-patch");
        write(singlePayload(tsId, 1.0))
            .when()
                .post("/timeseries/")
            .then()
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK));
        assignTsToDmzInclude(OFFICE_ID, tsId);

        write(duplicatePayload(tsId))
            .when()
                .patch("/timeseries/" + tsId)
            .then()
                .log().ifValidationFails(LogDetail.ALL, true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_BAD_REQUEST));

        write(duplicatePayload(tsId))
                .queryParam(DUPLICATE_DATE_TIME_RESOLUTION, "last")
            .when()
                .patch("/timeseries/" + tsId)
            .then()
                .log().ifValidationFails(LogDetail.ALL, true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK));

        retrieve(tsId)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
                .body("values.size()", is(2))
                .body("values[0][1]", closeTo(60.0, 0.0001));
    }

    @Test
    void test_store_rule_applies_after_resolution() throws Exception {
        String tsId = tsId("dupe-store-rule");
        write(singlePayload(tsId, 1.0))
            .when()
                .post("/timeseries/")
            .then()
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK));
        assignTsToDmzInclude(OFFICE_ID, tsId);

        write(duplicatePayload(tsId))
                .queryParam(DUPLICATE_DATE_TIME_RESOLUTION, "first")
                .queryParam(STORE_RULE, StoreRule.DO_NOT_REPLACE.toString())
            .when()
                .post("/timeseries/")
            .then()
                .log().ifValidationFails(LogDetail.ALL, true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK));

        // Existing 12:00 value is kept; the new 13:00 value is added.
        retrieve(tsId)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
                .body("values.size()", is(2))
                .body("values[0][1]", closeTo(1.0, 0.0001))
                .body("values[1][1]", closeTo(50.0, 0.0001));
    }
}
