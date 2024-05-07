package cwms.cda.api;

import static io.restassured.RestAssured.given;
import static io.restassured.config.JsonConfig.jsonConfig;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import cwms.cda.formatters.Formats;
import fixtures.TestAccounts;
import io.restassured.RestAssured;
import io.restassured.filter.log.LogDetail;
import io.restassured.path.json.config.JsonPathConfig;
import java.io.InputStream;
import java.sql.SQLException;
import java.time.ZonedDateTime;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("integration")
class TimeseriesControllerTestIT extends DataApiTestIT {
    @Test
    void test_lrl_timeseries_psuedo_reg1hour() throws Exception {
        ObjectMapper mapper = new ObjectMapper();

        InputStream resource = this.getClass().getResourceAsStream(
                "/cwms/cda/api/lrl/pseudo_reg_1hour.json");
        assertNotNull(resource);
        String tsData = IOUtils.toString(resource, "UTF-8");

        JsonNode ts = mapper.readTree(tsData);
        String location = ts.get("name").asText().split("\\.")[0];
        String officeId = ts.get("office-id").asText();

        try {
            createLocation(location, true, officeId);

            TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

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
        } catch (SQLException ex) {
            throw new RuntimeException("Unable to create location for TS", ex);
        }
    }

    @Test
    void test_lrl_1day() throws Exception {
        ObjectMapper mapper = new ObjectMapper();

        InputStream resource = this.getClass().getResourceAsStream(
                "/cwms/cda/api/lrl/1day_offset.json");
        assertNotNull(resource);
        String tsData = IOUtils.toString(resource, "UTF-8");

        JsonNode ts = mapper.readTree(tsData);
        String location = ts.get("name").asText().split("\\.")[0];
        String officeId = ts.get("office-id").asText();

        createLocation(location, true, officeId);

        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

        // inserting the time series
        given()
                .log().ifValidationFails(LogDetail.ALL, true)
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
        String firstPoint = "2023-02-02T06:00:00-05:00"; //aka 2023-02-02T11:00:00.000Z or
        // 1675335600000
        given()
                .config(RestAssured.config().jsonConfig(jsonConfig().numberReturnType(JsonPathConfig.NumberReturnType.DOUBLE)))
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(Formats.JSONV2)
                .body(tsData)
                .header("Authorization", user.toHeaderValue())
                .queryParam("office", officeId)
                .queryParam("units", "F")
                .queryParam("name", ts.get("name").asText())
                .queryParam("begin", firstPoint)
                .queryParam("end", firstPoint)
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/timeseries/")
            .then()
                .log().ifValidationFails(LogDetail.ALL,true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
                .body("values.size()", equalTo(1))  // one point
                .body("values[0].size()", equalTo(3))  // time, value, quality
                .body("values[0][0]", equalTo(1675335600000L)) // time
                .body("values[0][1]", closeTo(35, 0.0001))
        ;

    }

    @Test
    void test_lrl_1day_bad_units() throws Exception {
        ObjectMapper mapper = new ObjectMapper();

        InputStream resource = this.getClass().getResourceAsStream(
                "/cwms/cda/api/lrl/1day_offset_bad_units.json");
        assertNotNull(resource);
        String tsData = IOUtils.toString(resource, "UTF-8");

        JsonNode ts = mapper.readTree(tsData);
        String location = ts.get("name").asText().split("\\.")[0];
        String officeId = ts.get("office-id").asText();

        createLocation(location, true, officeId);

        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

        // inserting the time series
        given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(Formats.JSONV2)
                .contentType(Formats.JSONV2)
                .body(tsData)
                .header("Authorization", user.toHeaderValue())
                .queryParam("office", officeId)
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .post("/timeseries/")
            .then()
                .log().ifValidationFails(LogDetail.ALL, true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_BAD_REQUEST))
                .body("details.message", containsString("The unit: m is not a recognized CWMS "
                        + "Database unit for the Temp Parameter"));

    }

    @Test
    void test_lrl_1day_malicious_units() throws Exception {
        // We only get 16 chars in the units field so this input isn't a
        // valid malicious input but it looks close enough to freak owasp
        InputStream resource = this.getClass().getResourceAsStream(
                "/cwms/cda/api/lrl/1day_offset_malicious_units.json");
        assertNotNull(resource);
        String tsData = IOUtils.toString(resource, "UTF-8");

        ObjectMapper mapper = new ObjectMapper();
        JsonNode ts = mapper.readTree(tsData);
        String location = ts.get("name").asText().split("\\.")[0];
        String officeId = ts.get("office-id").asText();

        createLocation(location, true, officeId);

        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

        // inserting the time series
        given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(Formats.JSONV2)
                .contentType(Formats.JSONV2)
                .body(tsData)
                .header("Authorization", user.toHeaderValue())
                .queryParam("office", officeId)
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .post("/timeseries/")
            .then()
                .log().ifValidationFails(LogDetail.ALL, true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_BAD_REQUEST))
                .body("details.message", equalTo("Invalid Units."));

    }


    @Test
    void test_delete_ts() throws Exception {
        ObjectMapper mapper = new ObjectMapper();

        InputStream resource = this.getClass().getResourceAsStream(
                "/cwms/cda/api/lrl/1day_offset.json");
        assertNotNull(resource);

        String tsData = IOUtils.toString(resource, "UTF-8");

        JsonNode ts = mapper.readTree(tsData);
        String location = ts.get("name").asText().split("\\.")[0];
        String officeId = ts.get("office-id").asText();
        createLocation(location, true, officeId);

        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

        // inserting the time series
        given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(Formats.JSONV2)
                .contentType(Formats.JSONV2)
                .body(tsData)
                .header("Authorization", user.toHeaderValue())
                .queryParam("office", officeId)
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .post("/timeseries/")
            .then()
                .log().ifValidationFails(LogDetail.ALL, true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK));

        given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(Formats.JSONV2)
                .header("Authorization", user.toHeaderValue())
                .queryParam("office", officeId)
                .queryParam("begin", "2023-02-02T11:00:00+00:00")
                .queryParam("end", "2023-02-02T11:00:00+00:00")
                .queryParam("start-time-inclusive", "true")
                .queryParam("end-time-inclusive", "true")
                .queryParam("override-protection", "true")
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .delete("/timeseries/" + ts.get("name").asText())
            .then()
                .log().ifValidationFails(LogDetail.ALL, true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK));

        // get it back
        given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(Formats.JSONV2)
                .body(tsData)
                .header("Authorization", user.toHeaderValue())
                .queryParam("office", officeId)
                .queryParam("units", "F")
                .queryParam("name", ts.get("name").asText())
                .queryParam("begin", "2023-02-02T11:00:00+00:00")
                .queryParam("end", "2023-02-03T11:00:00+00:00")
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/timeseries/")
            .then()
                .log().ifValidationFails(LogDetail.ALL, true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
                .body("values[0][1]", nullValue());
    }

    @Test
    void test_no_office_permissions() throws Exception {
        ObjectMapper mapper = new ObjectMapper();

        InputStream resource = this.getClass().getResourceAsStream(
                "/cwms/cda/api/timeseries/no_office_perms.json");
        assertNotNull(resource);
        String tsData = IOUtils.toString(resource, "UTF-8");

        JsonNode ts = mapper.readTree(tsData);
        String location = ts.get("name").asText().split("\\.")[0];
        String officeId = ts.get("office-id").asText();

        createLocation(location, true, officeId);

        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

        // inserting the time series
        given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(Formats.JSONV2)
                .contentType(Formats.JSONV2)
                .body(tsData)
                .header("Authorization", user.toHeaderValue())
                .queryParam("office", officeId)
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .post("/timeseries/")
            .then()
                .log().ifValidationFails(LogDetail.ALL, true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_UNAUTHORIZED))
                .body("message", is("User not authorized for this office."));
    }

    @Test
    void test_v1_cant_trim() throws Exception {
        InputStream resource = this.getClass().getResourceAsStream(
                "/cwms/cda/api/lrl/1day_offset.json");
        assertNotNull(resource);

        String tsData = IOUtils.toString(resource, "UTF-8");

        ObjectMapper mapper = new ObjectMapper();
        JsonNode ts = mapper.readTree(tsData);
        String location = ts.get("name").asText().split("\\.")[0];
        String officeId = ts.get("office-id").asText();
        createLocation(location, true, officeId);

        String firstPoint = "2023-02-02T06:00:00-05:00"; //aka 2023-02-02T11:00:00.000Z or
        // 1675335600000
        given()
                .config(RestAssured.config().jsonConfig(jsonConfig().numberReturnType(JsonPathConfig.NumberReturnType.DOUBLE)))
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(Formats.JSON)
                .queryParam("office", officeId)
                .queryParam("units", "F")
                .queryParam("name", ts.get("name").asText())
                .queryParam("begin", firstPoint)
                .queryParam("end", firstPoint)
                .queryParam("trim", "true")
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/timeseries/")
            .then()
                .log().ifValidationFails(LogDetail.ALL, true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_BAD_REQUEST))
        ;
    }

    @Test
    void test_v1_cant_version() throws Exception {
        InputStream resource = this.getClass().getResourceAsStream(
                "/cwms/cda/api/lrl/1day_offset.json");
        assertNotNull(resource);

        String tsData = IOUtils.toString(resource, "UTF-8");

        ObjectMapper mapper = new ObjectMapper();
        JsonNode ts = mapper.readTree(tsData);
        String location = ts.get("name").asText().split("\\.")[0];
        String officeId = ts.get("office-id").asText();
        createLocation(location, true, officeId);

        Object version = null;

        String firstPoint = "2023-02-02T06:00:00-05:00"; //aka 2023-02-02T11:00:00.000Z or
        // 1675335600000
        given()
                .config(RestAssured.config().jsonConfig(jsonConfig().numberReturnType(JsonPathConfig.NumberReturnType.DOUBLE)))
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(Formats.JSON)
                .queryParam("office", officeId)
                .queryParam("units", "F")
                .queryParam("name", ts.get("name").asText())
                .queryParam("begin", firstPoint)
                .queryParam("end", firstPoint)
                .queryParam("version-date", version)
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/timeseries/")
            .then()
                .log().ifValidationFails(LogDetail.ALL, true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_BAD_REQUEST))
        ;
    }

    @Test
    void test_v2_cant_datum() throws Exception {
        InputStream resource = this.getClass().getResourceAsStream(
                "/cwms/cda/api/lrl/1day_offset.json");
        assertNotNull(resource);

        String tsData = IOUtils.toString(resource, "UTF-8");

        ObjectMapper mapper = new ObjectMapper();
        JsonNode ts = mapper.readTree(tsData);
        String location = ts.get("name").asText().split("\\.")[0];
        String officeId = ts.get("office-id").asText();
        createLocation(location, true, officeId);

        String firstPoint = "2023-02-02T06:00:00-05:00"; //aka 2023-02-02T11:00:00.000Z or
        // 1675335600000
        given()
                .config(RestAssured.config().jsonConfig(jsonConfig().numberReturnType(JsonPathConfig.NumberReturnType.DOUBLE)))
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(Formats.JSONV2)
                .queryParam("office", officeId)
                .queryParam("units", "F")
                .queryParam("name", ts.get("name").asText())
                .queryParam("begin", firstPoint)
                .queryParam("end", firstPoint)
                .queryParam("datum", "NAVD88")
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/timeseries/")
            .then()
                .log().ifValidationFails(LogDetail.ALL, true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_BAD_REQUEST))
        ;
    }

    @Test
    void test_lrl_trim() throws Exception {
        ObjectMapper mapper = new ObjectMapper();

        InputStream resource = this.getClass().getResourceAsStream(
                "/cwms/cda/api/lrl/1day_offset.json");
        assertNotNull(resource);
        String tsData = IOUtils.toString(resource, "UTF-8");

        JsonNode ts = mapper.readTree(tsData);
        String location = ts.get("name").asText().split("\\.")[0];
        String officeId = ts.get("office-id").asText();

        try {
            createLocation(location, true, officeId);

            TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

            // inserting the time series
            given()
                    .log().ifValidationFails(LogDetail.ALL, true)
                    .accept(Formats.JSONV2)
                    .contentType(Formats.JSONV2)
                    .body(tsData)
                    .header("Authorization", user.toHeaderValue())
                    .queryParam("office", officeId)
                .when()
                    .redirects().follow(true)
                    .redirects().max(3)
                    .post("/timeseries/")
                .then()
                    .log().ifValidationFails(LogDetail.ALL, true)
                    .assertThat()
                    .statusCode(is(HttpServletResponse.SC_OK));


            // The ts we created has   two values 1675335600000, 1675422000000,

            // get it back
            String firstPoint = "2023-02-02T06:00:00-05:00"; //aka 2023-02-02T11:00:00.000Z or
            // 1675335600000

            ZonedDateTime beginZdt = ZonedDateTime.parse(firstPoint);
            ZonedDateTime dayBeforeFirst = beginZdt.minusDays(1);

            // without trim we should get extra null point
            given()
                    .config(RestAssured.config().jsonConfig(jsonConfig().numberReturnType(JsonPathConfig.NumberReturnType.DOUBLE)))
                    .log().ifValidationFails(LogDetail.ALL, true)
                    .accept(Formats.JSONV2)
                    .body(tsData)
                    .header("Authorization", user.toHeaderValue())
                    .queryParam("office", officeId)
                    .queryParam("units", "F")
                    .queryParam("name", ts.get("name").asText())
                    .queryParam("begin", dayBeforeFirst.toInstant().toString())
                    .queryParam("end", firstPoint)
                    .queryParam("trim", false)
                .when()
                    .redirects().follow(true)
                    .redirects().max(3)
                    .get("/timeseries/")
                .then()
                    .log().ifValidationFails(LogDetail.ALL, true)
                    .assertThat()
                    .statusCode(is(HttpServletResponse.SC_OK))
                    .body("values.size()", equalTo(2))
                    .body("values[0].size()", equalTo(3))  // time, value, quality
                    .body("values[1][0]", equalTo(1675335600000L)) // time
                    .body("values[0][1]", nullValue())
                    .body("values[1][1]", closeTo(35, 0.0001));

            // with trim the null should get trimmed.
            given()
                    .config(RestAssured.config().jsonConfig(jsonConfig().numberReturnType(JsonPathConfig.NumberReturnType.DOUBLE)))
                    .log().ifValidationFails(LogDetail.ALL, true)
                    .accept(Formats.JSONV2)
                    .body(tsData)
                    .header("Authorization", user.toHeaderValue())
                    .queryParam("office", officeId)
                    .queryParam("units", "F")
                    .queryParam("name", ts.get("name").asText())
                    .queryParam("begin", dayBeforeFirst.toInstant().toString())
                    .queryParam("end", firstPoint)
                    .queryParam("trim", true)
                .when()
                    .redirects().follow(true)
                    .redirects().max(3)
                    .get("/timeseries/")
                .then()
                    .log().ifValidationFails(LogDetail.ALL, true)
                    .assertThat()
                    .statusCode(is(HttpServletResponse.SC_OK))
                    .body("values.size()", equalTo(1))
                    .body("values[0].size()", equalTo(3))  // time, value, quality
                    .body("values[0][0]", equalTo(1675335600000L)) // time
                    .body("values[0][1]", closeTo(35, 0.0001))
            ;
        } catch (SQLException ex) {
            throw new RuntimeException("Unable to create location for TS", ex);
        }
    }

}
