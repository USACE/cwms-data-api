package cwms.cda.api;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import javax.servlet.http.HttpServletResponse;

import cwms.cda.formatters.Formats;
import fixtures.TestAccounts;
import io.jsonwebtoken.io.IOException;
import io.restassured.filter.log.LogDetail;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@Tag("integration")
public class TimeseriesControllerTestIT extends DataApiTestIT {

    private static final String OFFICE = "SWT";
    private static final String locationId = "TsVersionedTestLoc";
    private static final String tsId = locationId + ".Flow.Inst.1Hour.0.raw";
    public static final String BEGIN_STR = "2008-05-01T15:00:00Z";
    public static final String END_STR = "2008-05-01T17:00:00Z";

    @BeforeAll
    public static void create() throws Exception {
        createLocation(locationId, true, OFFICE);
        createTimeseries(OFFICE, tsId, 0);
    }

    @Test
    void test_create_get() throws IOException, java.io.IOException {
        // Post versioned time series
        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/api/swt/num_ts_create.json");
        assertNotNull(resource);
        String tsData = IOUtils.toString(resource, StandardCharsets.UTF_8);
        assertNotNull(tsData);

        TestAccounts.KeyUser user = TestAccounts.KeyUser.SWT_NORMAL;

        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .body(tsData)
            .header("Authorization", user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/timeseries/")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK));


        // Get versioned time series
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .header("Authorization", user.toHeaderValue())
            .queryParam("office", OFFICE)
            .queryParam("name", tsId)
            .queryParam("begin", BEGIN_STR)
            .queryParam("end", END_STR)
            .queryParam("date-version-type", "SINGLE_VERSION")
            .queryParam("version-date", "2021-06-21T08:00:00-0000[UTC]")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("values.size()", equalTo(3))
            .body("values[0][1]", equalTo(1.0F))
            .body("values[1][1]", equalTo(1.0F))
            .body("values[2][1]", equalTo(1.0F));
    }

    @Test
    void test_create_get_delete_get() throws IOException, java.io.IOException {
        // Post versioned time series
        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/api/swt/num_ts_create.json");
        assertNotNull(resource);
        String tsData = IOUtils.toString(resource, StandardCharsets.UTF_8);
        assertNotNull(tsData);

        TestAccounts.KeyUser user = TestAccounts.KeyUser.SWT_NORMAL;

        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .body(tsData)
            .header("Authorization", user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/timeseries/")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK));


        // Get versioned time series
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .header("Authorization", user.toHeaderValue())
            .queryParam("office", OFFICE)
            .queryParam("name", tsId)
            .queryParam("begin", BEGIN_STR)
            .queryParam("end", END_STR)
            .queryParam("date-version-type", "SINGLE_VERSION")
            .queryParam("version-date", "2021-06-21T08:00:00-0000[UTC]")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/")
            .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("values.size()", equalTo(3))
            .body("values[0][1]", equalTo(1.0F))
            .body("values[1][1]", equalTo(1.0F))
            .body("values[2][1]", equalTo(1.0F));

        // Delete all values from timeseries
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .header("Authorization", user.toHeaderValue())
            .queryParam("office", OFFICE)
            .queryParam("timeseries", tsId)
            .queryParam("begin", BEGIN_STR)
            .queryParam("end", "2008-05-01T18:00:00Z")
            .queryParam("version-date", "2021-06-21T08:00:00-0000[UTC]")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("/timeseries/" + tsId)
            .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK));


        // Get versioned time series
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .header("Authorization", user.toHeaderValue())
            .queryParam("office", OFFICE)
            .queryParam("name", tsId)
            .queryParam("begin", BEGIN_STR)
            .queryParam("end", END_STR)
            .queryParam("date-version-type", "SINGLE_VERSION")
            .queryParam("version-date", "2021-06-21T08:00:00-0000[UTC]")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/")
            .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("values.size()", equalTo(3))
            .body("values[0][1]", equalTo(null))
            .body("values[1][1]", equalTo(null))
            .body("values[2][1]", equalTo(null));
    }

    @Test
    void test_create_get_param_conflict() throws IOException, java.io.IOException {
        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/api/swt/num_ts_create.json");
        assertNotNull(resource);
        String tsData = IOUtils.toString(resource, StandardCharsets.UTF_8);
        assertNotNull(tsData);

        TestAccounts.KeyUser user = TestAccounts.KeyUser.SWT_NORMAL;

        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .body(tsData)
            .header("Authorization", user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/timeseries/")
            .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK));

        // Get versioned time series with bad request parameters
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .header("Authorization", user.toHeaderValue())
            .queryParam("office", OFFICE)
            .queryParam("name", tsId)
            .queryParam("begin", BEGIN_STR)
            .queryParam("end", END_STR)
            .queryParam("date-version-type", "MAX_AGGREGATE")
            .queryParam("version-date", "2021-06-21T08:00:00-0000[UTC]")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/")
            .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_BAD_REQUEST));

        // Get versioned time series with bad request parameters
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .header("Authorization", user.toHeaderValue())
            .queryParam("office", OFFICE)
            .queryParam("name", tsId)
            .queryParam("begin", BEGIN_STR)
            .queryParam("end", END_STR)
            .queryParam("date-version-type", "SINGLE_VERSION")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/")
            .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_BAD_REQUEST));
    }
    /*@Test
    public void test_lrl_timeseries_psuedo_reg1hour() throws Exception {
        ObjectMapper mapper = new ObjectMapper();

        String tsData = IOUtils.toString(
            this.getClass()
                .getResourceAsStream("/cwms/cda/api/lrl/pseudo_reg_1hour.json"),"UTF-8"
            );

        JsonNode ts = mapper.readTree(tsData);
        String location = ts.get("name").asText().split("\\.")[0];
        String officeId = ts.get("office-id").asText();

        try {
            createLocation(location,true,officeId);

            KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

            // inserting the time series
            given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(Formats.JSONV2)
                .contentType(Formats.JSONV2)
                .body(tsData)
                .header("Authorization",user.toHeaderValue())
                .queryParam("office",officeId)
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .post("/timeseries/")
            .then()
                .log().ifValidationFails(LogDetail.ALL,true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK));
    
            // get it back
            given()
                .config(RestAssured.config().jsonConfig(jsonConfig().numberReturnType(JsonPathConfig.NumberReturnType.DOUBLE)))
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(Formats.JSONV2)
                .body(tsData)
                .header("Authorization",user.toHeaderValue())
                .queryParam("office",officeId)
                .queryParam("units","cfs")
                .queryParam("name",ts.get("name").asText())
                .queryParam("begin","2023-01-11T12:00:00-00:00")
                .queryParam("end","2023-01-11T13:00:00-00:00")
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/timeseries/")
            .then()
                .log().ifValidationFails(LogDetail.ALL,true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
                .body("values[1][1]",closeTo(600.0,0.0001))
                .body("values[0][1]",closeTo(500.0,0.0001))
    
                ;
        } catch( SQLException ex) {
            throw new RuntimeException("Unable to create location for TS",ex);
        }
    }

    @Test
    public void test_lrl_1day() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
    
        String tsData = IOUtils.toString(this.getClass().getResourceAsStream("/cwms/cda/api/lrl/1day_offset.json"),"UTF-8");

        JsonNode ts = mapper.readTree(tsData);
        String location = ts.get("name").asText().split("\\.")[0];
        String officeId = ts.get("office-id").asText();
    
        try {
            createLocation(location,true,officeId);

            KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

            // inserting the time series
            given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(Formats.JSONV2)
                .contentType(Formats.JSONV2)
                .body(tsData)
                .header("Authorization",user.toHeaderValue())
                .queryParam("office",officeId)
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .post("/timeseries/")
            .then()
                .log().ifValidationFails(LogDetail.ALL,true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK));
    
            // get it back
            given()
                .config(RestAssured.config().jsonConfig(jsonConfig().numberReturnType(JsonPathConfig.NumberReturnType.DOUBLE)))
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(Formats.JSONV2)
                .body(tsData)
                .header("Authorization",user.toHeaderValue())
                .queryParam("office",officeId)
                .queryParam("units","F")
                .queryParam("name",ts.get("name").asText())
                .queryParam("begin","2023-02-02T06:00:00-05:00")
                .queryParam("end","2023-02-02T06:00:00-05:00")
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/timeseries/")
            .then()
                .log().ifValidationFails(LogDetail.ALL,true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
                .body("values[0][1]",closeTo(35,0.0001));
        } catch( SQLException ex) {
            throw new RuntimeException("Unable to create location for TS",ex);
        }
    }

    @Test
    public void test_delete_ts() throws Exception {
        ObjectMapper mapper = new ObjectMapper();

        String tsData = IOUtils.toString(this.getClass().getResourceAsStream("/cwms/cda/api/lrl/1day_offset.json"),"UTF-8");

        JsonNode ts = mapper.readTree(tsData);
        String location = ts.get("name").asText().split("\\.")[0];
        String officeId = ts.get("office-id").asText();
        createLocation(location,true,officeId);

        KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

        // inserting the time series
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .body(tsData)
            .header("Authorization",user.toHeaderValue())
            .queryParam("office",officeId)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/timeseries/")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK));

        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV2)
            .header("Authorization",user.toHeaderValue())
            .queryParam("office",officeId)
            .queryParam("begin","2023-02-02T11:00:00+00:00")
            .queryParam("end","2023-02-02T11:00:00+00:00")
            .queryParam("start-time-inclusive","true")
            .queryParam("end-time-inclusive","true")
            .queryParam("override-protection","true")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("/timeseries/" + ts.get("name").asText())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK));

        // get it back
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV2)
            .body(tsData)
            .header("Authorization",user.toHeaderValue())
            .queryParam("office",officeId)
            .queryParam("units","F")
            .queryParam("name",ts.get("name").asText())
            .queryParam("begin","2023-02-02T11:00:00+00:00")
            .queryParam("end","2023-02-03T11:00:00+00:00")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("values[0][1]",nullValue());
    }

    @Test
    public void test_no_office_permissions() throws Exception
    {
        ObjectMapper mapper = new ObjectMapper();

        String tsData = IOUtils.toString(this.getClass().getResourceAsStream("/cwms/cda/api/timeseries/no_office_perms.json"),"UTF-8");

        JsonNode ts = mapper.readTree(tsData);
        String location = ts.get("name").asText().split("\\.")[0];
        String officeId = ts.get("office-id").asText();

        createLocation(location,true,officeId);

        KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

        // inserting the time series
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .body(tsData)
            .header("Authorization",user.toHeaderValue())
            .queryParam("office",officeId)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/timeseries/")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_UNAUTHORIZED))
            .body("message", is("User not authorized for this office."));
    }*/
}
