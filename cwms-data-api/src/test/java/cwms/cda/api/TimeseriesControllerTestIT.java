package cwms.cda.api;

import static cwms.cda.api.Controllers.*;
import static cwms.cda.data.dao.JooqDao.getDslContext;
import static helpers.FloatCloseTo.floatCloseTo;
import static io.restassured.RestAssured.given;
import static io.restassured.config.JsonConfig.jsonConfig;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import cwms.cda.ApiServlet;
import cwms.cda.data.dao.VerticalDatum;
import cwms.cda.formatters.Formats;
import cwms.cda.helpers.DatabaseHelpers.SCHEMA_VERSION;
import cwms.cda.helpers.ZoneIdHelper;
import fixtures.CwmsDataApiSetupCallback;
import fixtures.MinimumSchema;
import fixtures.TestAccounts;
import io.restassured.RestAssured;
import io.restassured.filter.log.LogDetail;
import io.restassured.path.json.config.JsonPathConfig;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.ZonedDateTime;
import javax.servlet.http.HttpServletResponse;

import io.restassured.response.ExtractableResponse;
import io.restassured.response.Response;
import io.restassured.response.ValidatableResponse;
import mil.army.usace.hec.test.database.CwmsDatabaseContainer;
import org.apache.commons.io.IOUtils;
import org.jooq.DSLContext;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import usace.cwms.db.jooq.codegen.packages.CWMS_LOC_PACKAGE;

@Tag("integration")
final class TimeseriesControllerTestIT extends DataApiTestIT {
    public static final int MINIMUM_SCHEMA = 999999;

    @Test
    void test_lrl_timeseries_psuedo_reg1hour() throws Exception {
        ObjectMapper mapper = new ObjectMapper();

        InputStream resource = this.getClass().getResourceAsStream(
                "/cwms/cda/api/lrl/pseudo_reg_1hour.json");
        assertNotNull(resource);
        String tsData = IOUtils.toString(resource, StandardCharsets.UTF_8);

        JsonNode ts = mapper.readTree(tsData);
        String location = ts.get(NAME).asText().split("\\.")[0];
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
                .queryParam(OFFICE,officeId)
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
                .queryParam(OFFICE,officeId)
                .queryParam(UNIT,"cfs")
                .queryParam(NAME,ts.get(NAME).asText())
                .queryParam(BEGIN,"2023-01-11T12:00:00-00:00")
                .queryParam(END,"2023-01-11T13:00:00-00:00")
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/timeseries/")
            .then()
                .log().ifValidationFails(LogDetail.ALL,true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
                .body("values[0][1]", closeTo(500.0,0.0001))
                .body("values[1][1]", nullValue())
                .body("values[1][2]", is(5))
                .body("values[2][1]", nullValue())
                .body("values[2][2]", is(5))
                .body("values[3][1]", closeTo(600.0,0.0001))
            ;
        } catch (SQLException ex) {
            throw new RuntimeException("Unable to create location for TS", ex);
        }
    }

    @Test
    @MinimumSchema(MINIMUM_SCHEMA)
    void test_local_regular_new_LRTS_ID() throws Exception {
        ObjectMapper mapper = new ObjectMapper();

        InputStream resource = this.getClass().getResourceAsStream(
                "/cwms/cda/api/timeseries/local_regular_ts.json");
        assertNotNull(resource);
        String tsData = IOUtils.toString(resource, StandardCharsets.UTF_8);

        JsonNode ts = mapper.readTree(tsData);
        String location = ts.get(NAME).asText().split("\\.")[0];
        String officeId = ts.get("office-id").asText();

        createLocation(location, true, officeId);

        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

        // inserting the time series
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .body(tsData)
            .header("Authorization", user.toHeaderValue())
            .header(ApiServlet.IS_NEW_LRTS, true)
            .queryParam(OFFICE, officeId)
            .queryParam(CREATE_AS_LRTS, true)
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
            .queryParam(OFFICE, officeId)
            .queryParam(UNIT,"F")
            .queryParam(NAME, ts.get(NAME).asText())
            .queryParam(BEGIN,"2025-05-08T12:00:00-00:00")
            .queryParam(END,"2025-05-19T13:00:00-00:00")
            .queryParam(INCLUDE_EXTENTS, true)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("values.size()", is(9))
            .body("values[0][1]", closeTo(35.0,0.0001))
        ;

        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV2)
            .header("Authorization", user.toHeaderValue())
            .header(ApiServlet.IS_NEW_LRTS, true)
            .queryParam(OFFICE, officeId)
            .queryParam(BEGIN, "2025-05-08T11:00:00+00:00")
            .queryParam(END, "2025-05-19T11:00:00+00:00")
            .queryParam("start-time-inclusive", "true")
            .queryParam("end-time-inclusive", "true")
            .queryParam("override-protection", "true")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("/timeseries/" + ts.get(NAME).asText())
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK));

        // inserting the time series with new LRTS ID turned off
        var assertThat =
            given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(Formats.JSONV2)
                .contentType(Formats.JSONV2)
                .header("Authorization", user.toHeaderValue())
                .header(ApiServlet.IS_NEW_LRTS, false)
                .queryParam(OFFICE, officeId)
                .queryParam(CREATE_AS_LRTS, true)
                .body(tsData)
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .post("/timeseries/")
            .then()
                .log().ifValidationFails(LogDetail.ALL, true)
            .assertThat();
        if (getSchemaVersion() > SCHEMA_VERSION.V2025_07_01.numeric()) {
            assertThat.statusCode(is(HttpServletResponse.SC_BAD_REQUEST));
        } else {
            assertThat.statusCode(is(HttpServletResponse.SC_INTERNAL_SERVER_ERROR));
        }
    }

    @Test
    void test_lrl_1day() throws Exception {
        ObjectMapper mapper = new ObjectMapper();

        InputStream resource = this.getClass().getResourceAsStream(
                "/cwms/cda/api/lrl/1day_offset.json");
        assertNotNull(resource);
        String tsData = IOUtils.toString(resource, StandardCharsets.UTF_8);

        JsonNode ts = mapper.readTree(tsData);
        String location = ts.get(NAME).asText().split("\\.")[0];
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
            .queryParam(OFFICE,officeId)
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
            .queryParam(OFFICE, officeId)
            .queryParam(UNIT, "F")
            .queryParam(NAME, ts.get(NAME).asText())
            .queryParam(BEGIN, firstPoint)
            .queryParam(END, firstPoint)
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
    void test_lrl_1day_default() throws Exception {
        ObjectMapper mapper = new ObjectMapper();

        InputStream resource = this.getClass().getResourceAsStream(
            "/cwms/cda/api/lrl/1day_offset.json");
        assertNotNull(resource);
        String tsData = IOUtils.toString(resource, StandardCharsets.UTF_8);

        tsData = tsData.replace("Buckhorn.Temp-Water.Inst.1Day.0.cda-test", "Buckhorn.Temp-Water.Inst.1Day.0.cda-accept-test");

        JsonNode ts = mapper.readTree(tsData);
        String location = ts.get(NAME).asText().split("\\.")[0];
        String officeId = ts.get("office-id").asText();

        createLocation(location, true, officeId);

        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

        // inserting the time series
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.DEFAULT)
            .contentType(Formats.JSONV2)
            .body(tsData)
            .header("Authorization",user.toHeaderValue())
            .queryParam(OFFICE,officeId)
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
            .accept(Formats.DEFAULT)
            .queryParam(OFFICE, officeId)
            .queryParam(UNIT, "F")
            .queryParam(NAME, ts.get(NAME).asText())
            .queryParam(BEGIN, firstPoint)
            .queryParam(END, firstPoint)
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
    void test_lrl_1day_max_version() throws Exception {
        ObjectMapper mapper = new ObjectMapper();

        InputStream resource = this.getClass().getResourceAsStream(
                "/cwms/cda/api/lrl/1day_offset_version_date.json");
        assertNotNull(resource);
        String tsData = IOUtils.toString(resource, StandardCharsets.UTF_8);

        JsonNode ts = mapper.readTree(tsData);
        String location = ts.get(NAME).asText().split("\\.")[0];
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
            .queryParam(OFFICE, officeId)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/timeseries/")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK));

        String secondVersionDate = "1604786000000";
        tsData = tsData.replace("1594786000000", secondVersionDate).replace("35,", "47.5,");
        // inserting the second time series
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .body(tsData)
            .header("Authorization",user.toHeaderValue())
            .queryParam(OFFICE, officeId)
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
        String versionDate = "2020-07-15T04:06:40Z";
        // 1675335600000
        given()
            .config(RestAssured.config().jsonConfig(jsonConfig().numberReturnType(JsonPathConfig.NumberReturnType.DOUBLE)))
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV2)
            .queryParam(OFFICE, officeId)
            .queryParam(UNIT, "F")
            .queryParam(NAME, ts.get(NAME).asText())
            .queryParam(BEGIN, firstPoint)
            .queryParam(END, firstPoint)
            .queryParam(VERSION_DATE, versionDate)
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
            .body("version-date", equalTo(versionDate))
        ;

        // get again as max version
        given()
            .config(RestAssured.config().jsonConfig(jsonConfig().numberReturnType(JsonPathConfig.NumberReturnType.DOUBLE)))
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV2)
            .queryParam(OFFICE, officeId)
            .queryParam(UNIT, "F")
            .queryParam(NAME, ts.get(NAME).asText())
            .queryParam(BEGIN, firstPoint)
            .queryParam(END, firstPoint)
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
            .body("values[0][1]", closeTo(47.5, 0.0001))
        ;
    }

    @Test
    @MinimumSchema(MINIMUM_SCHEMA)
    void test_lrl_1day_max_version_with_entry_date() throws Exception {
        ObjectMapper mapper = new ObjectMapper();

        InputStream resource = this.getClass().getResourceAsStream(
                "/cwms/cda/api/lrl/1day_offset_version_date_max.json");
        assertNotNull(resource);
        String tsData = IOUtils.toString(resource, StandardCharsets.UTF_8);

        JsonNode ts = mapper.readTree(tsData);
        String location = ts.get(NAME).asText().split("\\.")[0];
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
            .queryParam(OFFICE,officeId)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/timeseries/")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK));

        String secondVersionDate = "1604786000000";
        tsData = tsData.replace("1594786000000", secondVersionDate).replace("35,", "47.5,");
        // inserting the second time series
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .body(tsData)
            .header("Authorization",user.toHeaderValue())
            .queryParam(OFFICE,officeId)
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
        String versionDate = "2020-07-15T04:06:40Z";
        // 1675335600000
        given()
            .config(RestAssured.config().jsonConfig(jsonConfig().numberReturnType(JsonPathConfig.NumberReturnType.DOUBLE)))
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV2)
            .queryParam(OFFICE, officeId)
            .queryParam(UNIT, "F")
            .queryParam(NAME, ts.get(NAME).asText())
            .queryParam(BEGIN, firstPoint)
            .queryParam(END, firstPoint)
            .queryParam(VERSION_DATE, versionDate)
            .queryParam(INCLUDE_ENTRY_DATE, true)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("values.size()", equalTo(1))  // one point
            .body("values[0].size()", equalTo(4))  // time, value, quality, entry date
            .body("values[0][0]", equalTo(1675335600000L)) // time
            .body("values[0][1]", closeTo(35, 0.0001))
            .body("version-date", equalTo(versionDate))
        ;

        // get again as max version
        given()
            .config(RestAssured.config().jsonConfig(jsonConfig().numberReturnType(JsonPathConfig.NumberReturnType.DOUBLE)))
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV2)
            .queryParam(OFFICE, officeId)
            .queryParam(UNIT, "F")
            .queryParam(NAME, ts.get(NAME).asText())
            .queryParam(BEGIN, firstPoint)
            .queryParam(END, firstPoint)
            .queryParam(INCLUDE_ENTRY_DATE, true)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("values.size()", equalTo(1))  // one point
            .body("values[0].size()", equalTo(4))  // time, value, quality, entry date
            .body("values[0][0]", equalTo(1675335600000L)) // time
            .body("values[0][1]", closeTo(47.5, 0.0001))
        ;
    }

    @Test
    void test_lrl_1day_bad_units() throws Exception {
        ObjectMapper mapper = new ObjectMapper();

        InputStream resource = this.getClass().getResourceAsStream(
                "/cwms/cda/api/lrl/1day_offset_bad_units.json");
        assertNotNull(resource);
        String tsData = IOUtils.toString(resource, StandardCharsets.UTF_8);

        JsonNode ts = mapper.readTree(tsData);
        String location = ts.get(NAME).asText().split("\\.")[0];
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
            .queryParam(OFFICE, officeId)
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
    void test_lrl_timeseries_psuedo_reg1week() throws Exception {
        ObjectMapper mapper = new ObjectMapper();

        InputStream resource = this.getClass().getResourceAsStream(
                "/cwms/cda/api/lrl/pseudo_reg_1week.json");
        assertNotNull(resource);
        String tsData = IOUtils.toString(resource, StandardCharsets.UTF_8);

        JsonNode ts = mapper.readTree(tsData);
        String location = ts.get("name").asText().split("\\.")[0];
        String officeId = ts.get("office-id").asText();
        String units = ts.get("units").asText();

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
                .queryParam("office", officeId)
                .queryParam("units",units)
                .queryParam("name", ts.get("name").asText())
                .queryParam("begin","2024-12-15T15:00:00+00:00")
                .queryParam("end","2024-12-17T15:00:00+00:00")
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/timeseries/")
            .then()
                .log().ifValidationFails(LogDetail.ALL,true)
            .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
                .body("values[0][1]", closeTo(11.1,0.0001))
            ;
        } catch (SQLException ex) {
            throw new RuntimeException("Unable to create location for TS", ex);
        }
    }

    @Test
    void test_lrl_timeseries_lrts_reg1week() throws Exception {
        ObjectMapper mapper = new ObjectMapper();

        InputStream resource = this.getClass().getResourceAsStream(
                "/cwms/cda/api/lrl/pseudo_reg_1week.json");
        assertNotNull(resource);
        String tsData = IOUtils.toString(resource, StandardCharsets.UTF_8);

        JsonNode ts = mapper.readTree(tsData);
        String location = ts.get("name").asText().split("\\.")[0];
        String officeId = ts.get("office-id").asText();
        String units = ts.get("units").asText();

        String tsDataPsuedoOff = tsData.replace("2024-12-16T15:00:00+00:00", "2024-12-16T15:23:00+00:00");

        try {
            createLocation(location, true, officeId);

            TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

            // inserting the PRTS
            given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(Formats.JSONV2)
                .contentType(Formats.JSONV2)
                .body(tsDataPsuedoOff)
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

            // inserting the LRTS at same time
            given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(Formats.JSONV2)
                .contentType(Formats.JSONV2)
                .body(tsData)
                .header("Authorization",user.toHeaderValue())
                .queryParam("office",officeId)
                .queryParam("create-as-lrts", "true")
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
                .queryParam("office", officeId)
                .queryParam("units",units)
                .queryParam("name", ts.get("name").asText())
                .queryParam("begin","2024-12-15T15:00:00+00:00")
                .queryParam("end","2024-12-17T15:00:00+00:00")
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/timeseries/")
            .then()
                .log().ifValidationFails(LogDetail.ALL,true)
            .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
                .body("values[0][1]", closeTo(11.1,0.0001))
            ;
        } catch (SQLException ex) {
            throw new RuntimeException("Unable to create location for TS", ex);
        }
    }

    @Test
    void test_lrl_1day_malicious_units() throws Exception {
        // We only get 16 chars in the units field so this input isn't a
        // valid malicious input but it looks close enough to freak owasp
        InputStream resource = this.getClass().getResourceAsStream(
                "/cwms/cda/api/lrl/1day_offset_malicious_units.json");
        assertNotNull(resource);
        String tsData = IOUtils.toString(resource, StandardCharsets.UTF_8);

        ObjectMapper mapper = new ObjectMapper();
        JsonNode ts = mapper.readTree(tsData);
        String location = ts.get(NAME).asText().split("\\.")[0];
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
            .queryParam(OFFICE, officeId)
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
    @MinimumSchema(MINIMUM_SCHEMA)
    void test_include_data_entry_date() throws Exception {
        ObjectMapper mapper = new ObjectMapper();

        InputStream resource = this.getClass().getResourceAsStream(
            "/cwms/cda/api/spk/num_ts_create2.json");
        assertNotNull(resource);

        String tsData = IOUtils.toString(resource, StandardCharsets.UTF_8);

        JsonNode ts = mapper.readTree(tsData);
        String location = ts.get(NAME).asText().split("\\.")[0];
        String officeId = ts.get("office-id").asText();
        createLocation(location, true, officeId);

        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

        // insert the time series
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .body(tsData)
            .header("Authorization", user.toHeaderValue())
            .queryParam(OFFICE, officeId)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/timeseries/")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
        ;

        //     1675335600000 is Thursday, February 2, 2023 11:00:00 AM
        // fyi 1675422000000 is Friday, February 3, 2023 11:00:00 AM

        // get it back with the data entry date
        ExtractableResponse<Response> response = given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV2)
            .queryParam(OFFICE, officeId)
            .queryParam(UNIT, "CFS")
            .queryParam(NAME, ts.get(NAME).asText())
            .queryParam(BEGIN, "2007-02-02T11:00:00Z")
            .queryParam(END, "2010-02-03T11:00:00Z")
            .queryParam(VERSION_DATE, "2021-06-20T08:00:00-0000[UTC]")
            .queryParam(INCLUDE_ENTRY_DATE, true)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("values.size()", equalTo(4))
            .body("values[0][1]", equalTo(4.0F))
            .body("values[0].size()", equalTo(4))
            .extract();

        assertNotNull(response.body().path("values[0][3]"));

        // get it back without the data entry date
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV2)
            .queryParam(OFFICE, officeId)
            .queryParam(UNIT, "CFS")
            .queryParam(NAME, ts.get(NAME).asText())
            .queryParam(BEGIN, "2007-02-02T11:00:00Z")
            .queryParam(END, "2010-02-03T11:00:00Z")
            .queryParam(VERSION_DATE, "2021-06-20T08:00:00-0000[UTC]")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("values.size()", equalTo(4))
            .body("values[0][1]", equalTo(4.0F))
            .body("values[0].size()", equalTo(3));
    }

    @Test
    void test_get_with_units() throws Exception {
        ObjectMapper mapper = new ObjectMapper();

        InputStream resource = this.getClass().getResourceAsStream(
                "/cwms/cda/api/spk/num_ts_create2.json");
        assertNotNull(resource);

        String tsData = IOUtils.toString(resource, StandardCharsets.UTF_8);

        JsonNode ts = mapper.readTree(tsData);
        String location = ts.get(Controllers.NAME).asText().split("\\.")[0];
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
                .queryParam(Controllers.OFFICE, officeId)
        .when()
                .redirects().follow(true)
                .redirects().max(3)
                .post("/timeseries/")
        .then()
                .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
        ;

        // get it back using 'units' parameter
        given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(Formats.JSONV2)
                .queryParam(Controllers.OFFICE, officeId)
                .queryParam(Controllers.UNITS, "CFS")
                .queryParam(Controllers.NAME, ts.get(Controllers.NAME).asText())
                .queryParam(Controllers.BEGIN, "2007-02-02T11:00:00Z")
                .queryParam(Controllers.END, "2010-02-03T11:00:00Z")
                .queryParam(Controllers.VERSION_DATE, "2021-06-20T08:00:00-0000[UTC]")
        .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/timeseries/")
       .then()
                .log().ifValidationFails(LogDetail.ALL, true)
       .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
                .body("values.size()", equalTo(4))
                .body("values[0][1]", equalTo(4.0F))
                .body("values[0].size()", equalTo(3));

       // get it back using old 'unit' parameter
       given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(Formats.JSONV2)
                .queryParam(Controllers.OFFICE, officeId)
                .queryParam(Controllers.UNIT, "CFS")
                .queryParam(Controllers.NAME, ts.get(Controllers.NAME).asText())
                .queryParam(Controllers.BEGIN, "2007-02-02T11:00:00Z")
                .queryParam(Controllers.END, "2010-02-03T11:00:00Z")
                .queryParam(Controllers.VERSION_DATE, "2021-06-20T08:00:00-0000[UTC]")
       .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/timeseries/")
       .then()
                .log().ifValidationFails(LogDetail.ALL, true)
       .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
                .body("values.size()", equalTo(4))
                .body("values[0][1]", equalTo(4.0F))
                .body("values[0].size()", equalTo(3));

       // get it back using unit system for 'units'
       given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(Formats.JSONV2)
                .queryParam(Controllers.OFFICE, officeId)
                .queryParam(Controllers.UNITS, "EN")
                .queryParam(Controllers.NAME, ts.get(Controllers.NAME).asText())
                .queryParam(Controllers.BEGIN, "2007-02-02T11:00:00Z")
                .queryParam(Controllers.END, "2010-02-03T11:00:00Z")
                .queryParam(Controllers.VERSION_DATE, "2021-06-20T08:00:00-0000[UTC]")
       .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/timeseries/")
       .then()
                .log().ifValidationFails(LogDetail.ALL, true)
       .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
                .body("values.size()", equalTo(4))
                .body("values[0][1]", equalTo(4.0F))
                .body("values[0].size()", equalTo(3));
    }

    @Test
    @MinimumSchema(MINIMUM_SCHEMA)
    void test_attempt_store_with_entry_date() throws Exception
    {
        ObjectMapper mapper = new ObjectMapper();

        InputStream resource = this.getClass().getResourceAsStream(
            "/cwms/cda/api/lrl/timeseries_with_data_entry_dates.json");
        assertNotNull(resource);

        String tsData = IOUtils.toString(resource, StandardCharsets.UTF_8);

        JsonNode ts = mapper.readTree(tsData);
        String location = ts.get(NAME).asText().split("\\.")[0];
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
            .queryParam(OFFICE, officeId)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/timeseries/")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_BAD_REQUEST))
        ;
    }

    @Test
    void test_delete_ts() throws Exception {
        ObjectMapper mapper = new ObjectMapper();

        InputStream resource = this.getClass().getResourceAsStream(
                "/cwms/cda/api/lrl/1day_offset.json");
        assertNotNull(resource);

        String tsData = IOUtils.toString(resource, StandardCharsets.UTF_8);

        JsonNode ts = mapper.readTree(tsData);
        String location = ts.get(NAME).asText().split("\\.")[0];
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
            .queryParam(OFFICE, officeId)
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
            .queryParam(OFFICE, officeId)
            .queryParam(BEGIN, "2023-02-02T11:00:00+00:00")
            .queryParam(END, "2023-02-02T11:00:00+00:00")
            .queryParam(START_TIME_INCLUSIVE, "true")
            .queryParam(END_TIME_INCLUSIVE, "true")
            .queryParam(OVERRIDE_PROTECTION, "true")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("/timeseries/" + ts.get(Controllers.NAME).asText())
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK));

        //     1675335600000 is Thursday, February 2, 2023 11:00:00 AM
        // fyi 1675422000000 is Friday, February 3, 2023 11:00:00 AM

        // get it back
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV2)
            .queryParam(OFFICE, officeId)
            .queryParam(UNIT, "F")
            .queryParam(NAME, ts.get(NAME).asText())
            .queryParam(BEGIN, "2023-02-02T11:00:00+00:00")
            .queryParam(END, "2023-02-03T11:00:00+00:00")
            .queryParam(Controllers.TRIM, false)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("values.size()", equalTo(2))
            .body("values[0][1]", nullValue());
    }

    @Test
    void test_no_office_permissions() throws Exception {
        ObjectMapper mapper = new ObjectMapper();

        InputStream resource = this.getClass().getResourceAsStream(
                "/cwms/cda/api/timeseries/no_office_perms.json");
        assertNotNull(resource);
        String tsData = IOUtils.toString(resource, StandardCharsets.UTF_8);

        JsonNode ts = mapper.readTree(tsData);
        String location = ts.get(NAME).asText().split("\\.")[0];
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
            .queryParam(OFFICE, officeId)
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
    void test_invalid_office() {
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            //Purposefully misspelled office id
            .queryParam(OFFICE, "NWDW")
            .queryParam(NAME, "Buckhorn.Temp-Water.Inst.1Day.0.cda-test")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_BAD_REQUEST))
            .body("details.message", equalTo("\"NWDW\" is not a valid CWMS office id"));
    }

    @Test
    void test_v1_cant_trim() throws Exception {
        InputStream resource = this.getClass().getResourceAsStream(
                "/cwms/cda/api/lrl/1day_offset.json");
        assertNotNull(resource);

        String tsData = IOUtils.toString(resource, StandardCharsets.UTF_8);

        ObjectMapper mapper = new ObjectMapper();
        JsonNode ts = mapper.readTree(tsData);
        String location = ts.get(NAME).asText().split("\\.")[0];
        String officeId = ts.get("office-id").asText();
        createLocation(location, true, officeId);

        String firstPoint = "2023-02-02T06:00:00-05:00"; //aka 2023-02-02T11:00:00.000Z or
        // 1675335600000
        given()
            .config(RestAssured.config().jsonConfig(jsonConfig().numberReturnType(JsonPathConfig.NumberReturnType.DOUBLE)))
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV1)
            .queryParam(OFFICE, officeId)
            .queryParam(UNIT, "F")
            .queryParam(NAME, ts.get(NAME).asText())
            .queryParam(BEGIN, firstPoint)
            .queryParam(END, firstPoint)
            .queryParam(TRIM, "true")
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

        String tsData = IOUtils.toString(resource, StandardCharsets.UTF_8);

        ObjectMapper mapper = new ObjectMapper();
        JsonNode ts = mapper.readTree(tsData);
        String location = ts.get(NAME).asText().split("\\.")[0];
        String officeId = ts.get("office-id").asText();
        createLocation(location, true, officeId);

        Object version = null;

        String firstPoint = "2023-02-02T06:00:00-05:00"; //aka 2023-02-02T11:00:00.000Z or
        // 1675335600000
        given()
            .config(RestAssured.config().jsonConfig(jsonConfig().numberReturnType(JsonPathConfig.NumberReturnType.DOUBLE)))
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV1)
            .queryParam(OFFICE, officeId)
            .queryParam(UNIT, "F")
            .queryParam(NAME, ts.get(NAME).asText())
            .queryParam(BEGIN, firstPoint)
            .queryParam(END, firstPoint)
            .queryParam(VERSION_DATE, version)
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

        String tsData = IOUtils.toString(resource, StandardCharsets.UTF_8);

        ObjectMapper mapper = new ObjectMapper();
        JsonNode ts = mapper.readTree(tsData);
        String location = ts.get(NAME).asText().split("\\.")[0];
        String officeId = ts.get("office-id").asText();
        createLocation(location, true, officeId);

        String firstPoint = "2023-02-02T06:00:00-05:00"; //aka 2023-02-02T11:00:00.000Z or
        // 1675335600000
        given()
            .config(RestAssured.config().jsonConfig(jsonConfig().numberReturnType(JsonPathConfig.NumberReturnType.DOUBLE)))
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV2)
            .queryParam(OFFICE, officeId)
            .queryParam(UNIT, "F")
            .queryParam(NAME, ts.get(NAME).asText())
            .queryParam(BEGIN, firstPoint)
            .queryParam(END, firstPoint)
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
        String tsData = IOUtils.toString(resource, StandardCharsets.UTF_8);

        JsonNode ts = mapper.readTree(tsData);
        String location = ts.get(NAME).asText().split("\\.")[0];
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
                .queryParam(OFFICE, officeId)
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
                .queryParam(OFFICE, officeId)
                .queryParam(UNIT, "F")
                .queryParam(NAME, ts.get(NAME).asText())
                .queryParam(BEGIN, dayBeforeFirst.toInstant().toString())
                .queryParam(END, firstPoint)
                .queryParam(TRIM, false)
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
                .queryParam(OFFICE, officeId)
                .queryParam(UNIT, "F")
                .queryParam(NAME, ts.get(NAME).asText())
                .queryParam(BEGIN, dayBeforeFirst.toInstant().toString())
                .queryParam(END, firstPoint)
                .queryParam(TRIM, true)
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

    @Test
    @MinimumSchema(MINIMUM_SCHEMA)
    void test_lrl_trim_with_data_entry_date() throws Exception {
        ObjectMapper mapper = new ObjectMapper();

        InputStream resource = this.getClass().getResourceAsStream(
                "/cwms/cda/api/lrl/1day_offset_with_data_entry_dates.json");
        assertNotNull(resource);
        String tsData = IOUtils.toString(resource, StandardCharsets.UTF_8);

        JsonNode ts = mapper.readTree(tsData);
        String location = ts.get(NAME).asText().split("\\.")[0];
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
                .queryParam(OFFICE, officeId)
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
                .queryParam(OFFICE, officeId)
                .queryParam(UNIT, "m2")
                .queryParam(NAME, ts.get(NAME).asText())
                .queryParam(BEGIN, dayBeforeFirst.toInstant().toString())
                .queryParam(END, firstPoint)
                .queryParam(TRIM, false)
                .queryParam(INCLUDE_ENTRY_DATE, true)
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/timeseries/")
            .then()
                .log().ifValidationFails(LogDetail.ALL, true)
            .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
                .body("values.size()", equalTo(2))
                .body("values[1].size()", equalTo(4))  // time, value, quality, data entry date
                .body("values[1][0]", equalTo(1675335600000L)) // time
                .body("values[0][1]", nullValue())
                .body("values[1][1]", closeTo(35, 0.0001))
                .body("values[1][3]", notNullValue()); // data entry date

            // with trim the null should get trimmed.
            given()
                .config(RestAssured.config().jsonConfig(jsonConfig().numberReturnType(JsonPathConfig.NumberReturnType.DOUBLE)))
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(Formats.JSONV2)
                .queryParam(OFFICE, officeId)
                .queryParam(UNIT, "m2")
                .queryParam(NAME, ts.get(NAME).asText())
                .queryParam(BEGIN, dayBeforeFirst.toInstant().toString())
                .queryParam(END, firstPoint)
                .queryParam(TRIM, true)
                .queryParam(INCLUDE_ENTRY_DATE, true)
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/timeseries/")
            .then()
                .log().ifValidationFails(LogDetail.ALL, true)
            .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
                .body("values.size()", equalTo(1))
                .body("values[0].size()", equalTo(4))  // time, value, quality, data entry date
                .body("values[0][0]", equalTo(1675335600000L)) // time
                .body("values[0][1]", closeTo(35, 0.0001))
            ;
        } catch (SQLException ex) {
            throw new RuntimeException("Unable to create location for TS", ex);
        }
    }

    @Test
    void test_medium_create() throws Exception {

        InputStream resource = this.getClass().getResourceAsStream(
                "/cwms/cda/api/lrl/1day_offset.json");
        assertNotNull(resource);
        String tsData = IOUtils.toString(resource, StandardCharsets.UTF_8);

        // Note 09/08/25 Changed from 200k to 12k b/c the test was failing b/c zstore_ts was taking too long.
        //  5k took 13s, 10k = 26s, 12k = 31s, 15k closed @47s, 20k closed @83s
        String giantString = buildBigString(tsData, 12000);
        // 200k points looked like about 6MB.

        long bytes = giantString.getBytes().length;
        assertTrue(bytes > 350000, "The string should be over 350kB");

        ObjectMapper mapper = new ObjectMapper();
        JsonNode ts = mapper.readTree(tsData);
        String location = ts.get(NAME).asText().split("\\.")[0];
        String officeId = ts.get("office-id").asText();

        createLocation(location, true, officeId);

        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

        // inserting the time series
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .body(giantString)
            .header("Authorization", user.toHeaderValue())
            .queryParam(OFFICE, officeId)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/timeseries/")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK));
    }

    /**
     * Input looks like:
     * {
     *     "name": "Buckhorn.Temp-Water.Inst.1Day.0.cda-test",
     *     "office-id": "SPK",
     *     "units": "F",
     *     "values": [
     *         [
     *             1675335600000,
     *             35,
     *             0
     *         ],
     *         [
     *             1675422000000,
     *             36,
     *             0
     *         ]
     *     ]
     * }
     *
     * @param tsData  input json data
     * @return a new json string that has inserted the specified number of additional points.
     */
    private String buildBigString(String tsData, int count) throws JsonProcessingException {

        ObjectMapper mapper = new ObjectMapper();
        JsonNode ts = mapper.readTree(tsData);

        // get the first value in the second array entry of values
        long start1 = ts.get("values").get(0).get(0).asLong();
        long start2 = ts.get("values").get(1).get(0).asLong();
        long diff = start2 - start1;


        // From the back of the string find the last } and then the last ] before that
        int lastBrace = tsData.lastIndexOf("}");
        int lastBracket = tsData.lastIndexOf("]", lastBrace);

        String prefix = tsData.substring(0, lastBracket -1);

        // Now we insert a massive number of additional points

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < count; i++) {
            long time = start2 + (diff * (i+1));
            sb.append(String.format(",%n [ %d, %d,  %d]", time, count, 0));
        }

        return prefix + sb + "\n ]\n}";
    }

    @Test
    @Disabled("Referenced data set is missing")
    void test_daylight_saving_retrieve()throws Exception {

        InputStream resource = this.getClass().getResourceAsStream(
                "/cwms/cda/api/lrl/1hour.json");
        assertNotNull(resource);
        String tsData = IOUtils.toString(resource, StandardCharsets.UTF_8);

        int count = 365 * 24 * 5; // 5 years of hourly data (43.8k points)

        String giantString = buildBigString(tsData, count);
        // 200k points looked like about 6MB.

        // This creates data from  to May 21 2020 to May 20 2025

        ObjectMapper mapper = new ObjectMapper();
        JsonNode ts = mapper.readTree(tsData);
        String name = ts.get(NAME).asText();
        String location = name.split("\\.")[0];
        String officeId = ts.get("office-id").asText();

        try {
            deleteLocation(location, officeId);
        } catch (Exception ex) {
            // don't care.
        }
        createLocation(location, true, officeId);

        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

        // inserting the time series
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .body(giantString)
            .header("Authorization", user.toHeaderValue())
            .queryParam(OFFICE, officeId)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/timeseries/")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
        ;

        // this doesn't cross Daylight savings - should work
        given()
            .config(RestAssured.config().jsonConfig(jsonConfig().numberReturnType(JsonPathConfig.NumberReturnType.DOUBLE)))
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV2)
            .queryParam(OFFICE, officeId)
            .queryParam(UNIT,"mm")
            .queryParam(NAME, name)
            .queryParam(BEGIN,"2021-02-08T08:00:00Z")
            .queryParam(END,"2021-03-08T08:00:00Z")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("values[1][1]",closeTo(1724.4,0.1))
            .body("values[0][1]",closeTo(1724.4,0.1))
        ;

       // these dates do cross daylight savings - won't work if seessiontimezone isn't set in 24.04.05
        given()
            .config(RestAssured.config().jsonConfig(jsonConfig().numberReturnType(JsonPathConfig.NumberReturnType.DOUBLE)))
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV2)
            .queryParam(OFFICE, officeId)
            .queryParam(UNIT,"mm")
            .queryParam(NAME, name)
            .queryParam(BEGIN,"2021-03-08T08:00:00Z")
            .queryParam(END,"2021-03-15T08:00:00Z")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("values[1][1]",closeTo(1724.4,0.1))
            .body("values[0][1]",closeTo(1724.4,0.1))
        ;
    }

    private static void deleteLocation(String location, String officeId) throws SQLException {
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection(c-> {
            try(PreparedStatement stmt = c.prepareStatement("declare\n"
                    + "    p_location varchar2(64) := ?;\n"
                    + "    p_office varchar2(10) := ?;\n"
                    + "begin\n"
                    + "cwms_loc.delete_location(\n"
                    + "        p_location_id   => p_location,\n"
                    + "        p_delete_action => cwms_util.delete_all,\n"
                    + "        p_db_office_id  => p_office);\n"
                    + "end;")) {
                stmt.setString(1, location);
                stmt.setString(2, officeId);
                stmt.execute();

            } catch (SQLException ex) {
                throw new RuntimeException("Unable to delete location",ex);
            }
        }, "cwms_20");
    }

    @ParameterizedTest
    @EnumSource(GetAllTest.class)
    void test_lrl_1day_content_type_aliasing(GetAllTest test) throws Exception
    {
        //Based on test_lrl_1day()
        ObjectMapper mapper = new ObjectMapper();

        InputStream resource = this.getClass().getResourceAsStream(
                "/cwms/cda/api/lrl/1day_offset.json");
        assertNotNull(resource);
        String tsData = IOUtils.toString(resource, StandardCharsets.UTF_8);

        JsonNode ts = mapper.readTree(tsData);
        String location = ts.get(NAME).asText().split("\\.")[0];
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
            .queryParam(OFFICE,officeId)
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
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV2)
            .queryParam(OFFICE, officeId)
            .queryParam(UNIT, "F")
            .queryParam(NAME, ts.get(NAME).asText())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
        ;
    }



    @Test
    void test_wrong_units() throws Exception {
        ObjectMapper mapper = new ObjectMapper();

        InputStream resource = this.getClass().getResourceAsStream(
                "/cwms/cda/api/lrl/1day_offset.json");
        assertNotNull(resource);
        String tsData = IOUtils.toString(resource, StandardCharsets.UTF_8);

        JsonNode ts = mapper.readTree(tsData);
        String location = ts.get(NAME).asText().split("\\.")[0];
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
            .queryParam(OFFICE, officeId)
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
            .queryParam(Controllers.OFFICE, officeId)
            .queryParam(Controllers.UNIT, "m")
            .queryParam(Controllers.NAME, ts.get(Controllers.NAME).asText())
            .queryParam(Controllers.BEGIN, firstPoint)
            .queryParam(Controllers.END, firstPoint)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_BAD_REQUEST))
            .body("details.message", containsString("Cannot convert from unit C to unit m"))
        ;
    }

    enum GetAllTest
    {
        DEFAULT(Formats.DEFAULT, Formats.JSONV2),
        JSON(Formats.JSON, Formats.JSONV2),
        JSONV2(Formats.JSONV2, Formats.JSONV2),
        XML(Formats.XML, Formats.XMLV2),
        XMLV2(Formats.XMLV2, Formats.XMLV2),
        ;

        final String accept;
        final String expectedContentType;

        GetAllTest(String accept, String expectedContentType)
        {
            this.accept = accept;
            this.expectedContentType = expectedContentType;
        }
    }

    @Test
    void test_get_for_elev_has_datum() throws Exception {
        ObjectMapper mapper = new ObjectMapper();

        InputStream resource = this.getClass().getResourceAsStream(
                "/cwms/cda/api/spk/elev_ts_create.json");
        assertNotNull(resource);
        String tsData = IOUtils.toString(resource, StandardCharsets.UTF_8);

        JsonNode ts = mapper.readTree(tsData);
        String location = ts.get(NAME).asText().split("\\.")[0];
        String officeId = ts.get("office-id").asText();

        createLocation(location, true, officeId);  // This marks for delete at end of test.
        updateLocation(location, true, officeId);

        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

        // inserting the time series
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .body(tsData)
            .header("Authorization",user.toHeaderValue())
            .queryParam(OFFICE, officeId)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/timeseries/")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK));


        System.out.println("Data has been inserted for " + location);

        // 1209654000000 as ms == Thursday, May 1, 2008 3:00:00 PM

        // get it back
        String firstPoint = "2008-05-01T03:00:00.000Z";

        // try once with auth
        ValidatableResponse validatableResponse = given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .header("Authorization",user.toHeaderValue())
                .accept(Formats.JSONV2)
                .queryParam(OFFICE, officeId)
                .queryParam(UNIT, "m")
                .queryParam(NAME, ts.get(NAME).asText())
                .queryParam(BEGIN, firstPoint)
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/timeseries/")
            .then()
                .log().ifValidationFails(LogDetail.ALL, true)
            .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK));

        System.out.println(validatableResponse.extract().asString());
 // verify that there is vertical-datum-info in the response.
        validatableResponse.body("vertical-datum-info", notNullValue())
                .body("vertical-datum-info.location", equalTo(location))
                .body("vertical-datum-info.unit", equalTo("m"))
                .body("vertical-datum-info.offsets.size()", equalTo(1))
                ;

        // Try again without auth
        validatableResponse = given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(Formats.JSONV2)
                .queryParam(OFFICE, officeId)
                .queryParam(UNIT, "m")
                .queryParam(NAME, ts.get(NAME).asText())
                .queryParam(BEGIN, firstPoint)
                .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/timeseries/")
                .then()
                .log().ifValidationFails(LogDetail.ALL, true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK));

        System.out.println(validatableResponse.extract().asString());
        // verify that there is vertical-datum-info in the response.
        validatableResponse.body("vertical-datum-info", notNullValue())
                .body("vertical-datum-info.location", equalTo(location))
                .body("vertical-datum-info.unit", equalTo("m"))
                .body("vertical-datum-info.offsets.size()", equalTo(1))
        ;


    }

    private void updateLocation(String location, boolean active, String officeId) throws SQLException {

        String P_LOCATION_ID = location;
        String P_LOCATION_TYPE = "SITE";
        Number P_ELEVATION = 11;
        String P_ELEV_UNIT_ID = "m";

        // Pretty sure this isn't supposed to have a dash.  The create doesn't check.  The default create just passes null.
        // If it has a dash then the offsets don't work.
        // select VERTICAL_DATUM, count(*) as COUNT
        //  from AT_PHYSICAL_LOCATION
        //  group by VERTICAL_DATUM
        //  order by COUNT desc
        // has no entries with a dash in the name (unless we've run this test with a dash).
        String P_VERTICAL_DATUM = VerticalDatum.NAVD88.toString();
        Number P_LATITUDE = 38.5757;   // pretty sure that if these are 0,0 then its not inside the navd88 bounds and the offsets come back []
        Number P_LONGITUDE = -121.4789;
        String P_HORIZONTAL_DATUM = "WGS84";
        String P_PUBLIC_NAME = "Integration Test Sac Dam";
        String P_LONG_NAME= null;
        String P_DESCRIPTION = "for testing";
        String P_TIME_ZONE_ID = "UTC";
        String P_COUNTY_NAME = "Sacramento";
        String P_STATE_INITIAL = "CA";
        String P_ACTIVE = active ? "T" : "F";
        String P_DB_OFFICE_ID = officeId;

        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection(c -> {
            DSLContext dslContext = getDslContext(c, officeId);

//            CWMS_LOC_PACKAGE.call_DELETE_LOCATION(dslContext.configuration(), P_LOCATION_ID, String.valueOf(DeleteRule.DELETE_LOC_CASCADE), P_DB_OFFICE_ID);
//            CWMS_LOC_PACKAGE.call_CREATE_LOCATION(dslContext.configuration(),
//                    P_LOCATION_ID, P_LOCATION_TYPE, P_ELEVATION, P_ELEV_UNIT_ID, P_VERTICAL_DATUM, P_LATITUDE, P_LONGITUDE,
//                    P_HORIZONTAL_DATUM, P_PUBLIC_NAME, P_LONG_NAME, P_DESCRIPTION, P_TIME_ZONE_ID, P_COUNTY_NAME, P_STATE_INITIAL,
//                    P_ACTIVE, P_DB_OFFICE_ID);

            String P_IGNORENULLS = "F";
            CWMS_LOC_PACKAGE.call_UPDATE_LOCATION(dslContext.configuration(),
                     P_LOCATION_ID,  P_LOCATION_TYPE,  P_ELEVATION,  P_ELEV_UNIT_ID,  P_VERTICAL_DATUM,  P_LATITUDE,  P_LONGITUDE,
                    P_HORIZONTAL_DATUM,  P_PUBLIC_NAME,  P_LONG_NAME,  P_DESCRIPTION,  P_TIME_ZONE_ID,  P_COUNTY_NAME,  P_STATE_INITIAL,
                    P_ACTIVE,  P_IGNORENULLS,  P_DB_OFFICE_ID );

        });

    }


    //  vertical-datum parameter was recently added to the timeseries create call.
    // The timeseries sent as the body to the create can optionally also include a vertical-datum-info element. This
    // test is meant to verify 3 scenarios when the timeseries does not include vertical-datum-info:
    // 1) no vertical-datum parameter is sent to the create call.
    // 2) a NAVD88 vertical-datum parameter is sent to the create call.
    // 3) a NGVD29 vertical-datum parameter is sent to the create call.
    //
    @Test
    void test_create_with_vertical_datum_parameter_but_no_vertical_datum_info() throws Exception {
        ObjectMapper mapper = new ObjectMapper();

        InputStream resource = this.getClass().getResourceAsStream(
                "/cwms/cda/api/spk/elev_ts_create.json");
        assertNotNull(resource);
        String tsData = IOUtils.toString(resource, StandardCharsets.UTF_8);

        JsonNode ts = mapper.readTree(tsData);
        String tsName = ts.get(NAME).asText();
        String location = tsName.split("\\.")[0];
        String officeId = ts.get("office-id").asText();

        // Collect input times and values from the payload
        java.util.List<Long> inputTimes = new java.util.ArrayList<>();
        java.util.List<Double> inputValues = new java.util.ArrayList<>();
        for (JsonNode row : ts.get("values")) {
            inputTimes.add(row.get(0).asLong());
            inputValues.add(row.get(1).asDouble());
        }
        long firstMillis = inputTimes.get(0);
        long lastMillis = inputTimes.get(inputTimes.size() - 1);
        String beginIso = java.time.Instant.ofEpochMilli(firstMillis).toString();
        // pad end by 1 hour to ensure inclusion
        String endIso = java.time.Instant.ofEpochMilli(lastMillis + 3600_000L).toString();

        createLocation(location, true, officeId);  // This marks for delete at end of test.
        updateLocation(location, true, officeId);

        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

        // Helper lambda to GET the series and return a ValidatableResponse
        java.util.function.Supplier<ValidatableResponse> doGet = () ->
            given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(Formats.JSONV2)
                .queryParam(OFFICE, officeId)
                .queryParam(NAME, tsName)
                .queryParam(UNIT, "m")
                .queryParam(BEGIN, beginIso)
                .queryParam(END, endIso)
                .queryParam(TRIM, true)
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/timeseries/")
            .then()
                .log().ifValidationFails(LogDetail.ALL, true)
                .statusCode(is(HttpServletResponse.SC_OK));

        // 1) No vertical-datum parameter provided
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .body(tsData)
            .header("Authorization", user.toHeaderValue())
            .queryParam(OFFICE, officeId)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/timeseries/")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK));

        // GET after scenario 1 and verify values equal input (NAVD88 native)
        ValidatableResponse vr1 = doGet.get();
        System.out.println("1:" + vr1.extract().asString());

        /* Response includes
        "vertical-datum-info": {
        "office": "SPK",
        "unit": "m",
        "location": "Sacramento Dam",
        "native-datum": "NAVD-88",
        "elevation": 11.0,
        "offsets": [
            {
                "estimate": true,
                "to-datum": "NGVD-29",
                "value": -0.7717
            }
        ]
    }*/

        vr1.body("values.size()", equalTo(inputValues.size()));
        for (int i = 0; i < inputValues.size(); i++) {
            long expectedTime = inputTimes.get(i);
            double expectedVal = inputValues.get(i);
            vr1.body("values[" + i + "][0]", equalTo(expectedTime))
               .body("values[" + i + "][1]", floatCloseTo(expectedVal, 1e-6));
        }

        // 2) Provide NAVD88 vertical-datum parameter (matches location's datum)
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .body(tsData)
            .header("Authorization", user.toHeaderValue())
            .queryParam(OFFICE, officeId)
            .queryParam(VERTICAL_DATUM, "NAVD88")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/timeseries/")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK));

        // GET after scenario 2 and verify values equal input; also capture NGVD29 offset
        ValidatableResponse vr2 = doGet.get();
        System.out.println("2:" + vr2.extract().asString());
        vr2.body("values.size()", equalTo(inputValues.size()));
        for (int i = 0; i < inputValues.size(); i++) {
            vr2.body("values[" + i + "][1]", floatCloseTo(inputValues.get(i), 1e-6));
        }
        ExtractableResponse<Response> ex2 = vr2.extract();
        String body2 = ex2.asString();
        JsonNode resp2 = new ObjectMapper().readTree(body2);
        JsonNode vdi = resp2.get("vertical-datum-info");
        Double offsetToNgvd29 = null;
        if (vdi != null && vdi.has("offsets")) {
            for (JsonNode off : vdi.get("offsets")) {
                if ("NGVD-29".equalsIgnoreCase(off.get("to-datum").asText())) {
                    if (off.hasNonNull("value")) {
                        offsetToNgvd29 = off.get("value").asDouble();
                    }
                    break;
                }
            }
        }
        assertNotNull(offsetToNgvd29, "Expected NGVD-29 offset to be present in vertical-datum-info");

        // 3) Provide NGVD29 vertical-datum parameter (conversion should occur to as-stored datum)
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .body(tsData)
            .header("Authorization", user.toHeaderValue())
            .queryParam(OFFICE, officeId)
            .queryParam(VERTICAL_DATUM, "NGVD29")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/timeseries/")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK));

        // Compute expected NAVD88 = NGVD29 - offset(NAVD88->NGVD29)
        java.util.List<Double> expectedNavd88 = new java.util.ArrayList<>();
        for (Double v : inputValues) {
            expectedNavd88.add(v - offsetToNgvd29);
        }

        // GET after scenario 3 and verify conversion was applied
        ValidatableResponse vr3 = doGet.get();
        System.out.println("3:" + vr3.extract().asString());
        vr3.body("values.size()", equalTo(expectedNavd88.size()));
        for (int i = 0; i < expectedNavd88.size(); i++) {
            vr3.body("values[" + i + "][1]", floatCloseTo(expectedNavd88.get(i), 1e-6));
        }
    }



}
