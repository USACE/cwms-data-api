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
public class ForecastSpecControllerTestIT extends DataApiTestIT {
    private static final String OFFICE = "SPK";
    private static final String SPEC_ID = "test-spec";
    private static final String locationId = "TsBinTestLoc";
    private static final String designator = "designator";

    public static final String PATH = "/forecast/spec/";

    @BeforeAll
    public static void create() throws Exception {
        createLocation(locationId, true, OFFICE);
    }


    @Test
    void test_get_create_get() throws IOException {

        // Structure of test:
        // 1)Retrieve a ForecastSpec and assert that it does not exist
        // 2)Create the ForecastSpec
        // 3)Retrieve the ForecastSpec and assert that it exists

        // Step 1)
        // Retrieve a ForecastSpec and assert that it does not exist
        //Read
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV2)
            .queryParam(Controllers.OFFICE, OFFICE)
            .queryParam(Controllers.NAME, SPEC_ID)
            .queryParam(Controllers.DESIGNATOR, designator)
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
        // Create the ForecastSpec

        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/api/spk/forecast_spec_create.json");
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
        // Retrieve the spec and assert that it exists

        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV2)
                .queryParam(Controllers.OFFICE, OFFICE)
                .queryParam(Controllers.NAME, SPEC_ID)
                .queryParam(Controllers.DESIGNATOR, designator)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get(PATH)
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
                .body("time-series-ids.size()", equalTo(3))
        ;


    }

    @Test
    void test_create_get_delete_get() throws IOException {

        // Structure of test:
        //
        // 1)Create the spec
        // 2)Retrieve the spec and assert that it exists
        // 3)Delete the spec
        // 4)Retrieve the spec and assert that it does not exist


        // Step 1)
        // Create the spec
        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/api/spk/forecast_spec_create.json");
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
        // Retrieve the spec and assert that it exists
        given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(Formats.JSONV2)
                .queryParam(Controllers.OFFICE, OFFICE)
                .queryParam(Controllers.NAME, SPEC_ID)
                .queryParam(Controllers.DESIGNATOR, designator)
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get(PATH)
            .then()
                .log().ifValidationFails(LogDetail.ALL,true)
            .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
                .body("time-series-ids.size()", equalTo(3))
        ;

        // Step 3)
        // Delete the spec
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV2)
            .header(AUTH_HEADER, user.toHeaderValue())
                .queryParam(Controllers.OFFICE, OFFICE)
                .queryParam(Controllers.NAME, SPEC_ID)
                .queryParam(Controllers.DESIGNATOR, designator)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete(PATH + SPEC_ID)
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NO_CONTENT));

        // Step 4)
        // Retrieve the spec and assert that it does not exist
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV2)
            .queryParam(Controllers.OFFICE, OFFICE)
            .queryParam(Controllers.NAME, SPEC_ID)
            .queryParam(Controllers.DESIGNATOR, designator)
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
        // 1)Retrieve spec
        // 2)Create the spec
        // 3)Retrieve the spec and assert that it exists
        // 4)Update the spec
        // 5)Retrieve the spec and assert that its changed


        // Step 1)
        // Retrieve a spec and assert that it does not exist
        //Read
        given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(Formats.JSONV2)
                .queryParam(Controllers.OFFICE, OFFICE)
                .queryParam(Controllers.NAME, SPEC_ID)
                .queryParam(Controllers.DESIGNATOR, designator)
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
        // Create the ForecastSpec

        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/api/spk/forecast_spec_create.json");
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
        // Retrieve the spec and assert that it exists
        given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(Formats.JSONV2)
                .queryParam(Controllers.OFFICE, OFFICE)
                .queryParam(Controllers.NAME, SPEC_ID)
                .queryParam(Controllers.DESIGNATOR, designator)
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get(PATH)
            .then()
                .log().ifValidationFails(LogDetail.ALL,true)
            .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
                .body("source-entity-id", equalTo("sourceEntity"))
        ;

        // Step 4)
        // Update the spec
        resource = this.getClass().getResourceAsStream("/cwms/cda/api/spk/forecast_spec_update.json");
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
                .queryParam(Controllers.DESIGNATOR, designator)
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get(PATH)
            .then()
                .log().ifValidationFails(LogDetail.ALL,true)
            .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
                .body("source-entity-id", equalTo("anotherEntity"))
        ;
    }

}