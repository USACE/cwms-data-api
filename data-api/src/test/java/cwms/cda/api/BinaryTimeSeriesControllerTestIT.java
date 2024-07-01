package cwms.cda.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import cwms.cda.data.dto.binarytimeseries.BinaryTimeSeries;
import cwms.cda.data.dto.binarytimeseries.BinaryTimeSeriesRow;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.json.JsonV2;
import fixtures.TestAccounts;
import io.restassured.filter.log.LogDetail;
import io.restassured.response.ResponseBody;
import org.apache.commons.io.IOUtils;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URIBuilder;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.servlet.http.HttpServletResponse;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Date;
import java.util.Map;
import java.util.Objects;
import java.util.Random;

import static cwms.cda.api.Controllers.BLOB_ID;
import static cwms.cda.api.Controllers.VERSION_DATE;
import static io.restassured.RestAssured.given;
import static java.util.stream.Collectors.toMap;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

@Tag("integration")
public class BinaryTimeSeriesControllerTestIT extends DataApiTestIT {
    private static final String OFFICE = "SPK";
    private static final String locationId = "TsBinTestLoc";
    private static byte[] LARGE_BYTES;
    public static final String BEGIN_STR = "2008-05-01T15:00:00Z";
    public static final String END_STR = "2008-05-01T23:00:00Z";
    private String tsId;

    @BeforeAll
    public static void create() throws Exception {
        createLocation(locationId, true, OFFICE);
        LARGE_BYTES = new byte[1024 * 100];
        Random random = new Random();
        random.nextBytes(LARGE_BYTES);
    }

    @BeforeEach
    public void setup() throws Exception {
        tsId = locationId + ".Flow.Inst.1Hour.0." + Instant.now().getEpochSecond() + (int)(Math.random() * 100);
        createTimeseries(OFFICE, tsId, 0);
    }


    @Test
    void test_get_create_get() throws IOException {

        // Structure of test:
        // 1)Retrieve a binary time series and assert that it does not exist
        // 2)Create the binary time series
        // 3)Retrieve the binary time series and assert that it exists

        // Step 1)
        // Retrieve a binary time series and assert that it does not exist
        //Read
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV2)
            .queryParam("office", OFFICE)
            .queryParam("name", tsId)
            .queryParam("begin", BEGIN_STR)
            .queryParam("end", END_STR)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/binary/")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
                .body("binary-values.size()", equalTo(0))
        ;

        // Step 2)
        // Create the binary time series

        String tsData = getTsBody();

        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .body(tsData)
            .header("Authorization", user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/timeseries/binary/")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED));

        // Step 3)
        // Retrieve the binary time series and assert that it exists

        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV2)
            .queryParam("office", OFFICE)
            .queryParam("name", tsId)
            .queryParam("begin", BEGIN_STR)
            .queryParam("end", END_STR)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/binary/")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("binary-values.size()", equalTo(3))
        ;


    }

    @NotNull
    private String getTsBody() throws IOException {
        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/api/spk/bin_ts_create.json");
        assertNotNull(resource);
        String tsData = IOUtils.toString(resource, StandardCharsets.UTF_8);
        assertNotNull(tsData);

        ObjectMapper om = JsonV2.buildObjectMapper();
        BinaryTimeSeries bts = om.readValue(tsData, BinaryTimeSeries.class);
        bts = new BinaryTimeSeries.Builder()
                .withBinaryValues(bts.getBinaryValues())
                .withName(tsId)
                .withOfficeId(OFFICE)
                .withTimeZone(bts.getTimeZone())
                .withDateVersionType(bts.getDateVersionType())
                .withVersionDate(bts.getVersionDate())
                .withIntervalOffset(bts.getIntervalOffset())
                .build();
        return om.writeValueAsString(bts);
    }

    @NotNull
    private String getTsBodyLarge() throws IOException {
        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/api/spk/bin_ts_large_value.json");
        assertNotNull(resource);
        String tsData = IOUtils.toString(resource, StandardCharsets.UTF_8);
        assertNotNull(tsData);

        ObjectMapper om = JsonV2.buildObjectMapper();
        BinaryTimeSeries bts = om.readValue(tsData, BinaryTimeSeries.class);
        BinaryTimeSeriesRow row1 = bts.getBinaryValues().iterator().next();

        bts = new BinaryTimeSeries.Builder()
                .withBinaryValue(new BinaryTimeSeriesRow.Builder()
                        .withDateTime(row1.getDateTime())
                        .withBinaryValue(LARGE_BYTES)
                        .withMediaType(row1.getMediaType())
                        .build())
                .withName(tsId)
                .withOfficeId(OFFICE)
                .withTimeZone(bts.getTimeZone())
                .withDateVersionType(bts.getDateVersionType())
                .withVersionDate(bts.getVersionDate())
                .withIntervalOffset(bts.getIntervalOffset())
                .build();
        return om.writeValueAsString(bts);
    }

    @Test
    void test_create_get_delete_get() throws IOException {

        // Structure of test:
        //
        // 1)Create the binary time series
        // 2)Retrieve the binary time series and assert that it exists
        // 3)Delete the binary time series
        // 4)Retrieve the binary time series and assert that it does not exist


        // Step 1)
        // Create the binary time series

        String tsData = getTsBody();

        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .body(tsData)
            .header("Authorization", user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/timeseries/binary/")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED))
        ;

        // Step 2)
        // Retrieve the binary time series and assert that it exists

        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV2)
            .queryParam("office", OFFICE)
            .queryParam("name", tsId)
            .queryParam("begin", BEGIN_STR)
            .queryParam("end", END_STR)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/binary/")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("binary-values.size()", equalTo(3))
        ;

        // Step 3)
        // Delete the binary time series
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV2)
            .header("Authorization", user.toHeaderValue())
            .queryParam("office", OFFICE)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .queryParam("office", OFFICE)
            .queryParam("begin", BEGIN_STR)
            .queryParam("end", END_STR)
            .queryParam("office", OFFICE)
            .delete("/timeseries/binary/" + tsId)
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NO_CONTENT));

        Date versionDate = new Date(1139911000L);
        Instant instant = versionDate.toInstant();
        String versionDateStr = instant.toString();

        // Step 4)
        // Retrieve the binary time series and assert that it does not exist
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV2)
            .queryParam("office", OFFICE)
            .queryParam("name", tsId)
            .queryParam("begin", BEGIN_STR)
            .queryParam("end", END_STR)
            .queryParam("version-date", versionDateStr)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/binary/")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("binary-values.size()", equalTo(0))
        ;
    }

    @Test
    void test_create_get_update_get() throws IOException {

        // Structure of test:
        // 1)Retrieve bts and make sure its empty
        // 2)Create the binary time series
        // 3)Retrieve the binary time series and assert that it exists
        // 4)Update the binary time series
        // 5)Retrieve the binary time series and assert that it does not exist


        // Step 1)
        // Retrieve a binary time series and assert that it does not exist
        //Read
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV2)
            .queryParam("office", OFFICE)
            .queryParam("name", tsId)
            .queryParam("begin", BEGIN_STR)
            .queryParam("end", END_STR)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/binary/")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("binary-values.size()", equalTo(0))
        ;


        // Step 2)
        // Create the binary time series

        String tsData = getTsBody();
        InputStream resource;

        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .body(tsData)
            .header("Authorization", user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/timeseries/binary/")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED));

        // Step 3)
        // Retrieve the binary time series and assert that it exists

        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV2)
            .queryParam(Controllers.OFFICE, OFFICE)
            .queryParam(Controllers.NAME, tsId)
            .queryParam(Controllers.BEGIN, BEGIN_STR)
            .queryParam(Controllers.END, END_STR)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/binary/")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("binary-values.size()", equalTo(3))
        ;

        // Step 4)
        // Update the binary time series

        tsData = getTsUpdateBody();

        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV2)
            .queryParam("replace-all", "true")
            .contentType(Formats.JSONV2)
            .body(tsData)
            .header("Authorization", user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .patch("/timeseries/binary/" + tsId)
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK));


        String newValue = "bmV3VmFsdWU=";
        // Step 5)
        // Retrieve the binary time series and assert that it does not exist
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV2)
            .queryParam("office", OFFICE)
            .queryParam("name", tsId)
            .queryParam("begin", BEGIN_STR)
            .queryParam("end", END_STR)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/binary/")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("binary-values.size()", equalTo(3))
            .body("binary-values[1].binary-value", equalTo(newValue))
        ;
    }

    @NotNull
    private String getTsUpdateBody() throws IOException {
        InputStream resource;
        String tsData;
        resource = this.getClass().getResourceAsStream("/cwms/cda/api/spk/bin_ts_update.json");
        assertNotNull(resource);
        tsData = IOUtils.toString(resource, StandardCharsets.UTF_8);
        assertNotNull(tsData);
        ObjectMapper om = JsonV2.buildObjectMapper();
        BinaryTimeSeries bts = om.readValue(tsData, BinaryTimeSeries.class);
        bts = new BinaryTimeSeries.Builder()
                .withBinaryValues(bts.getBinaryValues())
                .withName(tsId)
                .withOfficeId(OFFICE)
                .withTimeZone(bts.getTimeZone())
                .withDateVersionType(bts.getDateVersionType())
                .withVersionDate(bts.getVersionDate())
                .withIntervalOffset(bts.getIntervalOffset())
                .build();
        return om.writeValueAsString(bts);
    }

    @Test
    void test_large_data_url() throws Exception {

        // Structure of test:
        // 1)Retrieve a binary time series and assert that it does not exist
        // 2)Create the binary time series with a large binary value
        // 3)Retrieve the binary time series and assert that it gives me back a new url to retrieve with
        // 4)Retrieve the single value from the new url

        // Step 1)
        // Retrieve a binary time series and assert that it does not exist
        //Read
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV2)
            .queryParam("office", OFFICE)
            .queryParam("name", tsId)
            .queryParam("begin", BEGIN_STR)
            .queryParam("end", END_STR)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/binary/")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("binary-values.size()", equalTo(0))
        ;

        // Step 2)
        // Create the binary time series

        String tsData = getTsBodyLarge();

        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .body(tsData)
            .header("Authorization", user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/timeseries/binary/")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED));

        // Step 3)
        // Retrieve the binary time series and assert that it exists

        String valueUrl = given()
                            .log().ifValidationFails(LogDetail.ALL, true)
                            .accept(Formats.JSONV2)
                            .queryParam("office", OFFICE)
                            .queryParam("name", tsId)
                            .queryParam("begin", BEGIN_STR)
                            .queryParam("end", END_STR)
                        .when()
                            .redirects().follow(true)
                            .redirects().max(3)
                            .get("/timeseries/binary/")
                        .then()
                            .log().ifValidationFails(LogDetail.ALL, true)
                        .assertThat()
                            .statusCode(is(HttpServletResponse.SC_OK))
                            .body("binary-values.size()", equalTo(1))
                            .body("binary-values[0].binary-value", is(nullValue()))
                            .body("binary-values[0].value-url", is(notNullValue()))
                            .extract()
                            .response()
                            .path("binary-values[0].value-url");
        // Step 4)
        // Use the URL returned in the JSON to download the large byte[]
        URIBuilder builder = new URIBuilder(valueUrl);
        assertTrue(builder.getPath().contains("timeseries/binary/" + tsId + "/value"));
        assertTrue(builder.getQueryParams().stream()
                .anyMatch(v -> v.getName().equals(Controllers.OFFICE) && v.getValue().equals(OFFICE)));
        assertTrue(builder.getQueryParams().stream()
                .anyMatch(v -> v.getName().equals(VERSION_DATE)));
        assertTrue(builder.getQueryParams().stream()
                .anyMatch(v -> v.getName().equals(BLOB_ID)));
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
                                .contentType(equalTo("application/octet-stream"))
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

}