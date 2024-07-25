package cwms.cda.api;

import com.google.common.flogger.FluentLogger;
import cwms.cda.data.dto.forecast.ForecastInstance;
import cwms.cda.data.dto.forecast.ForecastSpec;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.json.JsonV2;
import fixtures.TestAccounts;
import io.restassured.filter.log.LogDetail;
import io.restassured.response.ResponseBody;
import org.apache.commons.io.IOUtils;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URIBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.servlet.http.HttpServletResponse;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;
import java.util.Random;

import static cwms.cda.api.ForecastSpecControllerTestIT.*;
import static cwms.cda.security.KeyAccessManager.AUTH_HEADER;
import static io.restassured.RestAssured.given;
import static java.util.stream.Collectors.toMap;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@Tag("integration")
@Disabled("Full implementation not in available database schemas.")
public class ForecastInstanceControllerTestIT extends DataApiTestIT {
    private static final FluentLogger LOGGER = FluentLogger.forEnclosingClass();
    private static final String OFFICE = "SPK";
    private static final String SPEC_ID = "TEST-SPEC";
    private static final String designator = "designator";
    private static final String locationId = "FcstInstTestLoc";
    private static final String forecastDate = "2021-06-21T14:00:00+00:00";
    private static final String issueDate = "2022-05-22T12:03:00+00:00";
    public static final String PATH = "/forecast-instance/";
    private static byte[] LARGE_BYTES;


    @BeforeAll
    public static void create() throws Exception {
        createLocation(locationId, true, OFFICE);
        createTimeSeries(locationId);
        LARGE_BYTES = new byte[1024 * 100];
        Random random = new Random();
        random.nextBytes(LARGE_BYTES);
    }

    @AfterEach
    public void tearDown() throws Exception {
        truncateFcstTimeSeries();
        deleteSpec();
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
            .queryParam(Controllers.DESIGNATOR, designator)
            .queryParam(Controllers.FORECAST_DATE, forecastDate)
            .queryParam(Controllers.ISSUE_DATE, issueDate)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get(PATH + SPEC_ID)
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

        ForecastSpec spec = JsonV2.buildObjectMapper().readValue(tsData, ForecastInstance.class).getSpec();
        String specJson = JsonV2.buildObjectMapper().writeValueAsString(spec);
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .body(specJson)
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/forecast-spec/")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED))
        ;

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
            .queryParam(Controllers.DESIGNATOR, designator)
            .queryParam(Controllers.FORECAST_DATE, forecastDate)
            .queryParam(Controllers.ISSUE_DATE, issueDate)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get(PATH + SPEC_ID)
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("spec.spec-id", equalTo(SPEC_ID))
            .body("date-time", equalTo(1624284000000L))
            .body("issue-date-time", equalTo(1653220980000L))
            .body("max-age", equalTo(5))
            .body("notes", equalTo("test notes"))
//                .body("metadata.key1", equalTo("value1"))
//                .body("metadata.key2", equalTo("value2"))
//                .body("metadata.key3", equalTo("value3"))
            .body("filename", equalTo("testFilename.txt"))
//                .body("file-description", equalTo( "test file description"))
            .body("file-data", equalTo("dGVzdCBmaWxlIGNvbnRlbnQ="))
        ;


    }

    @Test
    void test_large_file_download() throws IOException, URISyntaxException {

        // Structure of test:
        // 1)Create the ForecastInstance with large file
        // 3)Retrieve the ForecastInstance and assert that it has a download url
        // 3)Use that download url to download the blob


        // Step 1)
        // Create the ForecastInstance with large file
        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/api/spk/forecast_inst_create.json");
        assertNotNull(resource);
        String tsData = IOUtils.toString(resource, StandardCharsets.UTF_8);
        assertNotNull(tsData);

        ForecastInstance instance = JsonV2.buildObjectMapper().readValue(tsData, ForecastInstance.class);
        instance = new ForecastInstance.Builder().from(instance)
                .withFileData(LARGE_BYTES)
                .build();
        ForecastSpec spec = instance.getSpec();
        String specJson = JsonV2.buildObjectMapper().writeValueAsString(spec);
        String largeInstanceJson = JsonV2.buildObjectMapper().writeValueAsString(instance);
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .body(specJson)
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/forecast-spec/")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED));

        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .body(largeInstanceJson)
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
        String fileDataUrl = given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(Formats.JSONV2)
                .queryParam(Controllers.OFFICE, OFFICE)
                .queryParam(Controllers.DESIGNATOR, designator)
                .queryParam(Controllers.FORECAST_DATE, forecastDate)
                .queryParam(Controllers.ISSUE_DATE, issueDate)
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get(PATH + SPEC_ID)
            .then()
                .log().ifValidationFails(LogDetail.ALL, true)
            .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
                .body("file-data", is(nullValue()))
                .body("file-data-url", is(notNullValue()))
                .extract()
                .response()
                .path("file-data-url");

        // Step 4)
        // Use the URL returned in the JSON to download the large byte[]
        URIBuilder builder = new URIBuilder(fileDataUrl);
        Map<String, String> params = builder.getQueryParams()
                .stream()
                .filter(Objects::nonNull)
                .filter(s -> s.getName() != null)
                .filter(s -> s.getValue() != null)
                .collect(toMap(NameValuePair::getName, NameValuePair::getValue));
        ResponseBody body = given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(Formats.JSONV2)
                .queryParams(params)
                .basePath("")
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get(builder.getPath())
            .then()
                .log().ifValidationFails(LogDetail.ALL, true)
            .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
                .header("Transfer-Encoding", equalTo("chunked"))
                .contentType(equalTo("text/plain"))
                .extract()
                .response()
                .body();

        byte[] data = new byte[LARGE_BYTES.length];
        try (ByteArrayOutputStream buffer = new ByteArrayOutputStream();
             InputStream is = body.asInputStream()) {
            int nRead;
            while ((nRead = is.read(data, 0, data.length)) != -1) {
                buffer.write(data, 0, nRead);
            }
            assertArrayEquals(LARGE_BYTES, buffer.toByteArray());
        }
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


        ForecastSpec spec = JsonV2.buildObjectMapper().readValue(tsData, ForecastInstance.class).getSpec();
        String specJson = JsonV2.buildObjectMapper().writeValueAsString(spec);
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .body(specJson)
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/forecast-spec/")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED));

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
            .queryParam(Controllers.DESIGNATOR, designator)
            .queryParam(Controllers.FORECAST_DATE, forecastDate)
            .queryParam(Controllers.ISSUE_DATE, issueDate)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get(PATH + SPEC_ID)
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("spec.spec-id", equalTo(SPEC_ID))
            .body("date-time", equalTo(1624284000000L))
            .body("issue-date-time", equalTo(1653220980000L))
            .body("max-age", equalTo(5))
            .body("notes", equalTo("test notes"))
//                .body("metadata.key1", equalTo("value1"))
//                .body("metadata.key2", equalTo("value2"))
//                .body("metadata.key3", equalTo("value3"))
            .body("filename", equalTo("testFilename.txt"))
//                .body("file-description", equalTo( "test file description"))
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
                .queryParam(Controllers.DESIGNATOR, designator)
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
                 .queryParam(Controllers.DESIGNATOR, designator)
                .queryParam(Controllers.FORECAST_DATE, forecastDate)
                .queryParam(Controllers.ISSUE_DATE, issueDate)
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get(PATH + SPEC_ID)
            .then()
                .log().ifValidationFails(LogDetail.ALL,true)
            .assertThat()
                .statusCode(is(HttpServletResponse.SC_NOT_FOUND))
        ;
    }

    @Disabled("Update currently fails with an error trying to store a null spec id")
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
                 .queryParam(Controllers.DESIGNATOR, designator)
                .queryParam(Controllers.FORECAST_DATE, forecastDate)
                .queryParam(Controllers.ISSUE_DATE, issueDate)
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get(PATH + SPEC_ID)
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


        ForecastSpec spec = JsonV2.buildObjectMapper().readValue(tsData, ForecastInstance.class).getSpec();
        String specJson = JsonV2.buildObjectMapper().writeValueAsString(spec);
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .body(specJson)
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/forecast-spec/")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED));

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
             .queryParam(Controllers.DESIGNATOR, designator)
            .queryParam(Controllers.FORECAST_DATE, forecastDate)
            .queryParam(Controllers.ISSUE_DATE, issueDate)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get(PATH + SPEC_ID)
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("spec.spec-id", equalTo(SPEC_ID))
            .body("date-time", equalTo(1624284000000L))
            .body("issue-date-time", equalTo(1653220980000L))
            .body("max-age", equalTo(5))
            .body("notes", equalTo("test notes"))
//                .body("metadata.key1", equalTo("value1"))
//                .body("metadata.key2", equalTo("value2"))
//                .body("metadata.key3", equalTo("value3"))
            .body("filename", equalTo("testFilename.txt"))
//                .body("file-description", equalTo( "test file description"))
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
                 .queryParam(Controllers.DESIGNATOR, designator)
                .queryParam(Controllers.FORECAST_DATE, forecastDate)
                .queryParam(Controllers.ISSUE_DATE, issueDate)
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get(PATH + SPEC_ID)
            .then()
                .log().ifValidationFails(LogDetail.ALL,true)
            .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
                // this part the same
                .body("spec.spec-id", equalTo(SPEC_ID))
                .body("date-time", equalTo(1624284000000L))
                .body("issue-date-time", equalTo(1653220980000L))
                .body("max-age", equalTo(5))
                // this part updated
                .body("notes", equalTo("updated notes"))
//                .body("metadata.key1", equalTo("value4"))
//                .body("metadata.key2", equalTo("value5"))
//                .body("metadata.key3", equalTo("value6"))
                .body("filename", equalTo("testFilename2.txt"))
//                .body("file-description", equalTo( "new description"))
        ;
    }

}