package cwms.cda.api;

import static io.restassured.RestAssured.given;
import static io.restassured.config.JsonConfig.jsonConfig;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.flogger.FluentLogger;
import cwms.cda.features.CdaFeatures;
import cwms.cda.formatters.Formats;
import fixtures.TestAccounts;
import io.restassured.RestAssured;
import io.restassured.filter.log.LogDetail;
import io.restassured.path.json.config.JsonPathConfig;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;

import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.Test;
import org.togglz.core.context.FeatureContext;
import org.togglz.core.manager.FeatureManager;

@Tag("integration")
class TimeSeriesFilteredControllerTestIT extends DataApiTestIT {
    static FluentLogger logger = FluentLogger.forEnclosingClass();
    public static final String JSON_FILE = "/cwms/cda/api/lrl/1hour.json";

    static class StackTraceFeatureExtension implements BeforeEachCallback, AfterEachCallback {
        private boolean wasActive;

        @Override
        public void beforeEach(ExtensionContext context) {
            FeatureManager featureManager = FeatureContext.getFeatureManager();
            wasActive = featureManager.isActive(CdaFeatures.INCLUDE_ERROR_STACK_TRACES);
            featureManager.enable(CdaFeatures.INCLUDE_ERROR_STACK_TRACES);
        }

        @Override
        public void afterEach(ExtensionContext context) {
            FeatureManager featureManager = FeatureContext.getFeatureManager();
            if (wasActive) {
                featureManager.enable(CdaFeatures.INCLUDE_ERROR_STACK_TRACES);
            } else {
                featureManager.disable(CdaFeatures.INCLUDE_ERROR_STACK_TRACES);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {Formats.JSONV2, Formats.DEFAULT})
    void test_page_size_special_cases(String format) throws Exception {
        ObjectMapper mapper = new ObjectMapper();

        InputStream resource = this.getClass().getResourceAsStream(JSON_FILE);
        assertNotNull(resource);
        String tsData = IOUtils.toString(resource, StandardCharsets.UTF_8);

        JsonNode ts = mapper.readTree(tsData);
        String location = ts.get(Controllers.NAME).asText().split("\\.")[0];
        String officeId = ts.get("office-id").asText();

        try {
            createLocation(location, true, officeId);

            TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

            given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(format)
                .contentType(Formats.JSONV2)
                .body(tsData)
                .header("Authorization",user.toHeaderValue())
                .queryParam(Controllers.OFFICE, officeId)
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .post("/timeseries/")
            .then()
                .log().ifValidationFails(LogDetail.ALL,true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK));

            given()
                .config(RestAssured.config().jsonConfig(jsonConfig().numberReturnType(JsonPathConfig.NumberReturnType.DOUBLE)))
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(format)
                .queryParam(Controllers.OFFICE, officeId)
                .queryParam(Controllers.UNIT,"cfs")
                .queryParam(Controllers.NAME, ts.get(Controllers.NAME).asText())
                .queryParam(Controllers.BEGIN,"2023-01-11T12:00:00-00:00")
                .queryParam(Controllers.END,"2023-01-11T15:00:00-00:00")
                .queryParam(Controllers.PAGE_SIZE, 0)
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/timeseries/filtered/")
            .then()
                .log().ifValidationFails(LogDetail.ALL,true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
                .body("page-size", equalTo(0))
                .body("time-series.values.size()", equalTo(0));

            given()
                .config(RestAssured.config().jsonConfig(jsonConfig().numberReturnType(JsonPathConfig.NumberReturnType.DOUBLE)))
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(format)
                .queryParam(Controllers.OFFICE, officeId)
                .queryParam(Controllers.UNIT,"cfs")
                .queryParam(Controllers.NAME, ts.get(Controllers.NAME).asText())
                .queryParam(Controllers.BEGIN,"2023-01-11T12:00:00-00:00")
                .queryParam(Controllers.END,"2023-01-11T15:00:00-00:00")
                .queryParam(Controllers.PAGE_SIZE, -1)
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/timeseries/filtered/")
            .then()
                .log().ifValidationFails(LogDetail.ALL,true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
                .body("page-size", equalTo(-1))
                .body("time-series.values.size()", equalTo(4));
        } catch (SQLException ex) {
            throw new RuntimeException("Unable to create location for TS", ex);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {Formats.JSONV2, Formats.DEFAULT})
    void test_filter_nulls(String format) throws Exception {
        ObjectMapper mapper = new ObjectMapper();

        InputStream resource = this.getClass().getResourceAsStream(JSON_FILE);
        assertNotNull(resource);
        String tsData = IOUtils.toString(resource, StandardCharsets.UTF_8);

        JsonNode ts = mapper.readTree(tsData);
        String location = ts.get(Controllers.NAME).asText().split("\\.")[0];
        String officeId = ts.get("office-id").asText();

        try {
            createLocation(location, true, officeId);

            TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

            // inserting the time series
            given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(format)
                .contentType(Formats.JSONV2)
                .body(tsData)
                .header("Authorization",user.toHeaderValue())
                .queryParam(Controllers.OFFICE, officeId)
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .post("/timeseries/")
            .then()
                .log().ifValidationFails(LogDetail.ALL,true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK));

            // get it back without filtering
            given()
                .config(RestAssured.config().jsonConfig(jsonConfig().numberReturnType(JsonPathConfig.NumberReturnType.DOUBLE)))
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(format)
                .queryParam(Controllers.OFFICE, officeId)
                .queryParam(Controllers.UNIT,"cfs")
                .queryParam(Controllers.NAME, ts.get(Controllers.NAME).asText())
                .queryParam(Controllers.BEGIN,"2023-01-11T12:00:00-00:00")
                .queryParam(Controllers.END,"2023-01-11T15:00:00-00:00")
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/timeseries/filtered/")
            .then()
                .log().ifValidationFails(LogDetail.ALL,true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
                .body("time-series.values[0][1]", closeTo(500.0,0.0001))
                .body("time-series.values[1][1]", nullValue())
                .body("time-series.values[2][1]", nullValue())
                .body("time-series.values[3][1]", closeTo(600.0,0.0001))
            ;

            // get it back with filter-nulls
            given()
                .config(RestAssured.config().jsonConfig(jsonConfig().numberReturnType(JsonPathConfig.NumberReturnType.DOUBLE)))
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(format)
                .queryParam(Controllers.OFFICE, officeId)
                .queryParam(Controllers.UNIT,"cfs")
                .queryParam(Controllers.NAME, ts.get(Controllers.NAME).asText())
                .queryParam(Controllers.BEGIN,"2023-01-11T12:00:00-00:00")
                .queryParam(Controllers.END,"2023-01-11T15:00:00-00:00")
                .queryParam(Controllers.QUERY,"value!=null")
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/timeseries/filtered/")
            .then()
                .log().ifValidationFails(LogDetail.ALL,true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
                .body("time-series.values[0][0]",  equalTo(1673438400000L))
                .body("time-series.values[0][1]", closeTo(500.0,0.0001))
                .body("time-series.values[1][0]", equalTo(1673449200000L))
                .body("time-series.values[1][1]", closeTo(600.0,0.0001))
                .body("time-series.values.size()", equalTo(2))
            ;
        } catch (SQLException ex) {
            throw new RuntimeException("Unable to create location for TS", ex);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {Formats.JSONV2, Formats.DEFAULT})
    void test_min_value(String format) throws Exception {
        ObjectMapper mapper = new ObjectMapper();

        InputStream resource = this.getClass().getResourceAsStream(JSON_FILE);
        assertNotNull(resource);
        String tsData = IOUtils.toString(resource, StandardCharsets.UTF_8);

        JsonNode ts = mapper.readTree(tsData);
        String location = ts.get(Controllers.NAME).asText().split("\\.")[0];
        String officeId = ts.get("office-id").asText();

        try {
            createLocation(location, true, officeId);

            TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

            // inserting the time series
            given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(format)
                .contentType(Formats.JSONV2)
                .body(tsData)
                .header("Authorization",user.toHeaderValue())
                .queryParam(Controllers.OFFICE, officeId)
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .post("/timeseries/")
            .then()
                .log().ifValidationFails(LogDetail.ALL,true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK));

            // get it back with min-value
            given()
                .config(RestAssured.config().jsonConfig(jsonConfig().numberReturnType(JsonPathConfig.NumberReturnType.DOUBLE)))
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(format)
                .queryParam(Controllers.OFFICE, officeId)
                .queryParam(Controllers.UNIT,"cfs")
                .queryParam(Controllers.NAME, ts.get(Controllers.NAME).asText())
                .queryParam(Controllers.BEGIN,"2023-01-11T12:00:00-00:00")
                .queryParam(Controllers.END,"2023-01-11T15:00:00-00:00")
                .queryParam(Controllers.QUERY, "value>550.0")
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/timeseries/filtered/")
            .then()
                .log().ifValidationFails(LogDetail.ALL,true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
                .body("time-series.values[0][0]", equalTo(1673449200000L))
                .body("time-series.values[0][1]", closeTo(600.0,0.0001))
                .body("time-series.values.size()", equalTo(1))
            ;
        } catch (SQLException ex) {
            throw new RuntimeException("Unable to create location for TS", ex);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {Formats.JSONV2, Formats.DEFAULT})
    void test_max_value(String format) throws Exception {
        ObjectMapper mapper = new ObjectMapper();

        InputStream resource = this.getClass().getResourceAsStream(JSON_FILE);
        assertNotNull(resource);
        String tsData = IOUtils.toString(resource, StandardCharsets.UTF_8);

        JsonNode ts = mapper.readTree(tsData);
        String location = ts.get(Controllers.NAME).asText().split("\\.")[0];
        String officeId = ts.get("office-id").asText();

        try {
            createLocation(location, true, officeId);

            TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

            // inserting the time series
            given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(format)
                .contentType(Formats.JSONV2)
                .body(tsData)
                .header("Authorization",user.toHeaderValue())
                .queryParam(Controllers.OFFICE, officeId)
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .post("/timeseries/")
            .then()
                .log().ifValidationFails(LogDetail.ALL,true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK));

            // get it back with max-value
            given()
                .config(RestAssured.config().jsonConfig(jsonConfig().numberReturnType(JsonPathConfig.NumberReturnType.DOUBLE)))
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(format)
                .queryParam(Controllers.OFFICE, officeId)
                .queryParam(Controllers.UNIT,"cfs")
                .queryParam(Controllers.NAME, ts.get(Controllers.NAME).asText())
                .queryParam(Controllers.BEGIN,"2023-01-11T12:00:00-00:00")
                .queryParam(Controllers.END,"2023-01-11T15:00:00-00:00")
                .queryParam(Controllers.QUERY, "value<=550.0")
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/timeseries/filtered")
            .then()
                .log().ifValidationFails(LogDetail.ALL,true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
                .body("time-series.values[0][0]",  equalTo(1673438400000L))
                .body("time-series.values[0][1]", closeTo(500.0,0.0001))
                .body("time-series.values.size()", equalTo(1))
            ;
        } catch (SQLException ex) {
            throw new RuntimeException("Unable to create location for TS", ex);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {Formats.JSONV2, Formats.DEFAULT})
    void test_min_max_value_combined(String format) throws Exception {
        ObjectMapper mapper = new ObjectMapper();

        InputStream resource = this.getClass().getResourceAsStream(JSON_FILE);
        assertNotNull(resource);
        String tsData = IOUtils.toString(resource, StandardCharsets.UTF_8);

        JsonNode ts = mapper.readTree(tsData);
        String location = ts.get(Controllers.NAME).asText().split("\\.")[0];
        String officeId = ts.get("office-id").asText();

        try {
            createLocation(location, true, officeId);

            TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

            // inserting the time series
            given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(format)
                .contentType(Formats.JSONV2)
                .body(tsData)
                .header("Authorization",user.toHeaderValue())
                .queryParam(Controllers.OFFICE, officeId)
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .post("/timeseries/")
            .then()
                .log().ifValidationFails(LogDetail.ALL,true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK));

            // get it back with min-value and max-value
            given()
                .config(RestAssured.config().jsonConfig(jsonConfig().numberReturnType(JsonPathConfig.NumberReturnType.DOUBLE)))
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(format)
                .queryParam(Controllers.OFFICE, officeId)
                .queryParam(Controllers.UNIT,"cfs")
                .queryParam(Controllers.NAME, ts.get(Controllers.NAME).asText())
                .queryParam(Controllers.BEGIN,"2023-01-11T12:00:00-00:00")
                .queryParam(Controllers.END,"2023-01-11T15:00:00-00:00")
                .queryParam(Controllers.QUERY, "value>450.0 and value <=550.0")
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/timeseries/filtered/")
            .then()
                .log().ifValidationFails(LogDetail.ALL,true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
                .body("time-series.values[0][0]",  equalTo(1673438400000L))
                .body("time-series.values[0][1]", closeTo(500.0,0.0001))
                .body("time-series.values.size()", equalTo(1))
            ;
        } catch (SQLException ex) {
            throw new RuntimeException("Unable to create location for TS", ex);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {Formats.JSONV2, Formats.DEFAULT})
    void test_all_filters_combined(String format) throws Exception {
        ObjectMapper mapper = new ObjectMapper();

        InputStream resource = this.getClass().getResourceAsStream(JSON_FILE);
        assertNotNull(resource);
        String tsData = IOUtils.toString(resource, StandardCharsets.UTF_8);

        JsonNode ts = mapper.readTree(tsData);
        String location = ts.get(Controllers.NAME).asText().split("\\.")[0];
        String officeId = ts.get("office-id").asText();

        try {
            createLocation(location, true, officeId);

            TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

            // inserting the time series
            given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(format)
                .contentType(Formats.JSONV2)
                .body(tsData)
                .header("Authorization",user.toHeaderValue())
                .queryParam(Controllers.OFFICE, officeId)
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .post("/timeseries/")
            .then()
                .log().ifValidationFails(LogDetail.ALL,true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK));

            // get it back with filter-nulls, min-value, and max-value
            given()
                .config(RestAssured.config().jsonConfig(jsonConfig().numberReturnType(JsonPathConfig.NumberReturnType.DOUBLE)))
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(format)
                .queryParam(Controllers.OFFICE, officeId)
                .queryParam(Controllers.UNIT,"cfs")
                .queryParam(Controllers.NAME, ts.get(Controllers.NAME).asText())
                .queryParam(Controllers.BEGIN,"2023-01-11T12:00:00-00:00")
                .queryParam(Controllers.END,"2023-01-11T15:00:00-00:00")
                .queryParam(Controllers.QUERY, "value!=null and value>450.0 and value <=550.0")
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/timeseries/filtered/")
            .then()
                .log().ifValidationFails(LogDetail.ALL,true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
                .body("time-series.values[0][0]", equalTo(1673438400000L))
                .body("time-series.values[0][1]", closeTo(500.0,0.0001))
                .body("time-series.values.size()", equalTo(1))
            ;
        } catch (SQLException ex) {
            throw new RuntimeException("Unable to create location for TS", ex);
        }
    }

    @Test
    @ExtendWith(StackTraceFeatureExtension.class)
    void returnsStackTraceLinesForTraceRoleWhenRequestFails() throws Exception {
        ObjectMapper mapper = new ObjectMapper();

        InputStream resource = this.getClass().getResourceAsStream(JSON_FILE);
        assertNotNull(resource);
        String tsData = IOUtils.toString(resource, StandardCharsets.UTF_8);

        JsonNode ts = mapper.readTree(tsData);
        String location = ts.get(Controllers.NAME).asText().split("\\.")[0];
        String officeId = ts.get("office-id").asText();

        createLocation(location, true, officeId);

        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL2;

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
            .statusCode(is(HttpServletResponse.SC_OK));

        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV2)
            .cookie("JSESSIONIDSSO", user.getJSessionId())
            .queryParam(Controllers.OFFICE, officeId)
            .queryParam(Controllers.NAME, ts.get(Controllers.NAME).asText())
            .queryParam(Controllers.BEGIN, "not-a-date")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/filtered/")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_BAD_REQUEST))
            .body("incidentIdentifier", notNullValue())
            .body("details.stackTraceLines.size()", greaterThan(0))
            .body("details.stackTraceLines[0]", containsString("Exception"));
    }

}
