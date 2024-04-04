package cwms.cda.api;

import static cwms.cda.security.KeyAccessManager.AUTH_HEADER;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import cwms.cda.formatters.Formats;
import fixtures.TestAccounts;
import io.restassured.filter.log.LogDetail;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("integration")
public class ForecastInstanceControllerTestIT extends DataApiTestIT {
    private static final String OFFICE = "SPK";
    private static final String SPEC_ID = "test-spec";
    private static final String locationId = "FcstInstTestLoc";
    private static final String forecastDate = "2021-06-21T14:00:10+00:00";
    private static final String issueDate = "2022-05-22T12:03:40+00:00";
    public static final String PATH = "/forecast-instance/";

    @BeforeAll
    public static void create() throws Exception {
        createLocation(locationId, true, OFFICE);
    }

    @Test
    void test_get_create_get() throws IOException {

        // Structure of test:
        // 1)Retrieve a ForecastInstance and assert that it does not exist
        // 2)Create the ForecastInstance
        // 3)Retrieve the ForecastInstance and assert that it exists

        // Step 1)
        // Retrieve a ForecastInstance and assert that it does not exist
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV2)
            .queryParam(Controllers.OFFICE, OFFICE)
            .queryParam(Controllers.NAME, SPEC_ID)
            .queryParam(Controllers.LOCATION_ID, locationId)
            .queryParam(Controllers.FORECAST_DATE, forecastDate)
            .queryParam(Controllers.ISSUE_DATE, issueDate)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get(PATH)
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NOT_FOUND))
        ;

        // Step 2)
        // Create the ForecastInstance
        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/api/spk/forecast_inst_create.json");
        assertNotNull(resource);
        String tsData = IOUtils.toString(resource, StandardCharsets.UTF_8);
        assertNotNull(tsData);

        // The json has these:
        //  "date-time": 1624284010000,
        //  "issue-date-time": 1653221020000,
        //  "first-date-time": 1692702150000,
        //  "last-date-time": 1727017260000,
        // which are:
        //1624284010	2021-06-21T14:00:10+00:00
        //1653221020	2022-05-22T12:03:40+00:00
        //1692702150	2023-08-22T11:02:30+00:00
        //1727017260	2024-09-22T15:01:00+00:00

        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .body(tsData)
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post(PATH)
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED));

        // Step 3)
        // Retrieve the inst and assert that it exists
        given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(Formats.JSONV2)
                .queryParam(Controllers.OFFICE, OFFICE)
                .queryParam(Controllers.NAME, SPEC_ID)
                .queryParam(Controllers.LOCATION_ID, locationId)
                .queryParam(Controllers.FORECAST_DATE, forecastDate)
                .queryParam(Controllers.ISSUE_DATE, issueDate)
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get(PATH)
            .then()
                .log().ifValidationFails(LogDetail.ALL,true)
            .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
                .body("spec-id", equalTo("test-spec"))
                .body("date-time", equalTo(1624284010000L))
                .body("issue-date-time", equalTo(1653221020000L))
                .body("first-date-time", equalTo(1692702150000L))
                .body("last-date-time", equalTo(1727017260000L))
                .body("max-age", equalTo(5))
                .body("time-series-count", equalTo(3))
                .body("notes", equalTo("test notes"))
                .body("metadata.key1", equalTo("value1"))
                .body("metadata.key2", equalTo("value2"))
                .body("metadata.key3", equalTo("value3"))
                .body("filename", equalTo("testFilename.txt"))
                .body("file-description", equalTo( "test file description"))
                .body("file-data", equalTo("dGVzdCBmaWxlIGNvbnRlbnQ="))
        ;


    }

    @Test
    void test_create_get_delete_get() throws IOException {

        // Structure of test:
        //
        // 1)Create the inst
        // 2)Retrieve the inst and assert that it exists
        // 3)Delete the inst
        // 4)Retrieve the inst and assert that it does not exist


        // Step 1)
        // Create the inst
        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/api/spk/forecast_inst_create.json");
        assertNotNull(resource);
        String tsData = IOUtils.toString(resource, StandardCharsets.UTF_8);
        assertNotNull(tsData);

        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

        given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(Formats.JSONV2)
                .contentType(Formats.JSONV2)
                .body(tsData)
                .header(AUTH_HEADER, user.toHeaderValue())
                .when()
                .redirects().follow(true)
                .redirects().max(3)
                .post(PATH)
                .then()
                .log().ifValidationFails(LogDetail.ALL,true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_CREATED));

        // Step 2)
        // Retrieve the inst and assert that it exists
        given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(Formats.JSONV2)
                .queryParam(Controllers.OFFICE, OFFICE)
                .queryParam(Controllers.NAME, SPEC_ID)
                .queryParam(Controllers.LOCATION_ID, locationId)
                .queryParam(Controllers.FORECAST_DATE, forecastDate)
                .queryParam(Controllers.ISSUE_DATE, issueDate)
                .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get(PATH)
                .then()
                .log().ifValidationFails(LogDetail.ALL,true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
                .body("spec-id", equalTo("test-spec"))
                .body("date-time", equalTo(1624284010000L))
                .body("issue-date-time", equalTo(1653221020000L))
                .body("first-date-time", equalTo(1692702150000L))
                .body("last-date-time", equalTo(1727017260000L))
                .body("max-age", equalTo(5))
                .body("time-series-count", equalTo(3))
                .body("notes", equalTo("test notes"))
                .body("metadata.key1", equalTo("value1"))
                .body("metadata.key2", equalTo("value2"))
                .body("metadata.key3", equalTo("value3"))
                .body("filename", equalTo("testFilename.txt"))
                .body("file-description", equalTo( "test file description"))
                .body("file-data", equalTo("dGVzdCBmaWxlIGNvbnRlbnQ="))
        ;

        // Step 3)
        // Delete the inst
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV2)
            .header(AUTH_HEADER, user.toHeaderValue())
                .queryParam(Controllers.OFFICE, OFFICE)
                .queryParam(Controllers.NAME, SPEC_ID)
                .queryParam(Controllers.LOCATION_ID, locationId)
                .queryParam(Controllers.FORECAST_DATE, forecastDate)
                .queryParam(Controllers.ISSUE_DATE, issueDate)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete(PATH + SPEC_ID)
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NO_CONTENT));

        // Step 4)
        // Retrieve the inst and assert that it does not exist
        given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(Formats.JSONV2)
                .queryParam(Controllers.OFFICE, OFFICE)
                .queryParam(Controllers.NAME, SPEC_ID)
                .queryParam(Controllers.LOCATION_ID, locationId)
                .queryParam(Controllers.FORECAST_DATE, forecastDate)
                .queryParam(Controllers.ISSUE_DATE, issueDate)
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get(PATH)
            .then()
                .log().ifValidationFails(LogDetail.ALL,true)
            .assertThat()
                .statusCode(is(HttpServletResponse.SC_NOT_FOUND))
        ;
    }

    @Test
    void test_create_get_update_get() throws IOException {

        // Structure of test:
        // 1)Retrieve inst
        // 2)Create the inst
        // 3)Retrieve the inst and assert that it exists
        // 4)Update the inst
        // 5)Retrieve the inst and assert that its changed


        // Step 1)
        // Retrieve a inst and assert that it does not exist
        //Read
        given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(Formats.JSONV2)
                .queryParam(Controllers.OFFICE, OFFICE)
                .queryParam(Controllers.NAME, SPEC_ID)
                .queryParam(Controllers.LOCATION_ID, locationId)
                .queryParam(Controllers.FORECAST_DATE, forecastDate)
                .queryParam(Controllers.ISSUE_DATE, issueDate)
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get(PATH)
            .then()
                .log().ifValidationFails(LogDetail.ALL,true)
            .assertThat()
                .statusCode(is(HttpServletResponse.SC_NOT_FOUND))
        ;

        // Step 2)
        // Create the ForecastInstance
        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/api/spk/forecast_inst_create.json");
        assertNotNull(resource);
        String tsData = IOUtils.toString(resource, StandardCharsets.UTF_8);
        assertNotNull(tsData);

        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

        given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(Formats.JSONV2)
                .contentType(Formats.JSONV2)
                .body(tsData)
                .header(AUTH_HEADER, user.toHeaderValue())
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .post(PATH)
            .then()
                .log().ifValidationFails(LogDetail.ALL,true)
            .assertThat()
                .statusCode(is(HttpServletResponse.SC_CREATED));

        // Step 3)
        // Retrieve the inst and assert that it exists
        given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(Formats.JSONV2)
                .queryParam(Controllers.OFFICE, OFFICE)
                .queryParam(Controllers.NAME, SPEC_ID)
                .queryParam(Controllers.LOCATION_ID, locationId)
                .queryParam(Controllers.FORECAST_DATE, forecastDate)
                .queryParam(Controllers.ISSUE_DATE, issueDate)
                .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get(PATH)
                .then()
                .log().ifValidationFails(LogDetail.ALL,true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
                .body("spec-id", equalTo("test-spec"))
                .body("date-time", equalTo(1624284010000L))
                .body("issue-date-time", equalTo(1653221020000L))
                .body("first-date-time", equalTo(1692702150000L))
                .body("last-date-time", equalTo(1727017260000L))
                .body("max-age", equalTo(5))
                .body("time-series-count", equalTo(3))
                .body("notes", equalTo("test notes"))
                .body("metadata.key1", equalTo("value1"))
                .body("metadata.key2", equalTo("value2"))
                .body("metadata.key3", equalTo("value3"))
                .body("filename", equalTo("testFilename.txt"))
                .body("file-description", equalTo( "test file description"))
                .body("file-data", equalTo("dGVzdCBmaWxlIGNvbnRlbnQ="))
        ;

        // Step 4)
        // Update the inst series
        resource = this.getClass().getResourceAsStream("/cwms/cda/api/spk/forecast_inst_update.json");
        assertNotNull(resource);
        tsData = IOUtils.toString(resource, StandardCharsets.UTF_8);
        assertNotNull(tsData);

        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .body(tsData)
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .patch(PATH + SPEC_ID)
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK));

        // Step 5)
        // Retrieve thespec and assert it changed
        given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(Formats.JSONV2)
                .queryParam(Controllers.OFFICE, OFFICE)
                .queryParam(Controllers.NAME, SPEC_ID)
                .queryParam(Controllers.LOCATION_ID, locationId)
                .queryParam(Controllers.FORECAST_DATE, forecastDate)
                .queryParam(Controllers.ISSUE_DATE, issueDate)
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get(PATH)
            .then()
                .log().ifValidationFails(LogDetail.ALL,true)
            .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
                // this part the same
                .body("spec-id", equalTo("test-spec"))
                .body("date-time", equalTo(1624284010000L))
                .body("issue-date-time", equalTo(1653221020000L))
                .body("first-date-time", equalTo(1692702150000L))
                .body("last-date-time", equalTo(1727017260000L))
                .body("max-age", equalTo(5))
                .body("time-series-count", equalTo(3))
                // this part updated
                .body("notes", equalTo("updated notes"))
                .body("metadata.key1", equalTo("value4"))
                .body("metadata.key2", equalTo("value5"))
                .body("metadata.key3", equalTo("value6"))
                .body("filename", equalTo("testFilename2.txt"))
                .body("file-description", equalTo( "new description"))
        ;
    }

}