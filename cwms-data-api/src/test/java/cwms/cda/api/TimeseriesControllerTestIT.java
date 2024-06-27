package cwms.cda.api;

import static io.restassured.RestAssured.given;
import static io.restassured.config.JsonConfig.jsonConfig;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import cwms.cda.formatters.Formats;
import fixtures.CwmsDataApiSetupCallback;
import fixtures.TestAccounts;
import io.restassured.RestAssured;
import io.restassured.filter.log.LogDetail;
import io.restassured.path.json.config.JsonPathConfig;
import java.io.InputStream;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.ZonedDateTime;
import javax.servlet.http.HttpServletResponse;
import mil.army.usace.hec.test.database.CwmsDatabaseContainer;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

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
//                .body(tsData)
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
//                .body(tsData)
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
//                .body(tsData)
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
//                    .body(tsData)
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
//                    .body(tsData)
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

    @Test
    void test_big_create() throws Exception {

        InputStream resource = this.getClass().getResourceAsStream(
                "/cwms/cda/api/lrl/1day_offset.json");
        assertNotNull(resource);
        String tsData = IOUtils.toString(resource, "UTF-8");

        String giantString = buildBigString(tsData, 200000);
        // 200k points looked like about 6MB.

        long bytes = giantString.getBytes().length;
        assertTrue(bytes > 2000000, "The string should be over 2MB");

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
                .body(giantString)
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
            sb.append(String.format(",\n [ %d, %d,  %d]", time, count, 0));
        }

        return prefix + sb + "\n ]\n}";
    }

    @Test
    void test_daylight_saving_retrieve()throws Exception {

        InputStream resource = this.getClass().getResourceAsStream(
                "/cwms/cda/api/lrl/1hour.json");
        assertNotNull(resource);
        String tsData = IOUtils.toString(resource, "UTF-8");

        int count = 365 * 24 * 5; // 5 years of hourly data (43.8k points)

        String giantString = buildBigString(tsData, count);
        // 200k points looked like about 6MB.

        // This creates data from  to May 21 2020 to May 20 2025

        ObjectMapper mapper = new ObjectMapper();
        JsonNode ts = mapper.readTree(tsData);
        String name = ts.get("name").asText();
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
                .queryParam("office", officeId)
                .when()
                .redirects().follow(true)
                .redirects().max(3)
                .post("/timeseries/")
                .then()
                .log().ifValidationFails(LogDetail.ALL, true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK));

        // this doesn't cross Daylight savings - should work
        given()
                .config(RestAssured.config().jsonConfig(jsonConfig().numberReturnType(JsonPathConfig.NumberReturnType.DOUBLE)))
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(Formats.JSONV2)
                .header("Authorization", user.toHeaderValue())
                .queryParam("office", officeId)
                .queryParam("units","mm")
                .queryParam("name", name)
                .queryParam("begin","2021-02-08T08:00:00Z")
                .queryParam("end","2021-03-08T08:00:00Z")
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
                .header("Authorization", user.toHeaderValue())
                .queryParam("office", officeId)
                .queryParam("units","mm")
                .queryParam("name", name)
                .queryParam("begin","2021-03-08T08:00:00Z")
                .queryParam("end","2021-03-15T08:00:00Z")
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
        given()
                .config(RestAssured.config().jsonConfig(jsonConfig().numberReturnType(JsonPathConfig.NumberReturnType.DOUBLE)))
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(Formats.JSONV2)
                .header("Authorization", user.toHeaderValue())
                .queryParam("office", officeId)
                .queryParam("units", "F")
                .queryParam("name", ts.get("name").asText())
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

    enum GetAllTest
    {
        DEFAULT(Formats.DEFAULT, Formats.JSONV2),
        JSON(Formats.JSON, Formats.JSONV2),
        JSONV2(Formats.JSONV2, Formats.JSONV2),
        XML(Formats.XML, Formats.XMLV2),
        XMLV2(Formats.XMLV2, Formats.XMLV2),
        ;

        final String _accept;
        final String _expectedContentType;

        GetAllTest(String accept, String expectedContentType)
        {
            _accept = accept;
            _expectedContentType = expectedContentType;
        }
    }
}
