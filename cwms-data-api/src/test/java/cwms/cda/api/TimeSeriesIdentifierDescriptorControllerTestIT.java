package cwms.cda.api;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import cwms.cda.ApiServlet;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dto.TimeSeriesIdentifierDescriptor;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.json.JsonV2;
import cwms.cda.helpers.DatabaseHelpers.SCHEMA_VERSION;
import fixtures.TestAccounts;
import io.restassured.filter.log.LogDetail;
import io.restassured.path.json.JsonPath;
import io.restassured.response.Response;

import java.sql.SQLException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import javax.servlet.http.HttpServletResponse;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;


@Tag("integration")
final class TimeSeriesIdentifierDescriptorControllerTestIT extends DataApiTestIT {

    public static final String OFFICE = "SPK";

    @ParameterizedTest
    @ValueSource(strings = {Formats.JSONV2, Formats.DEFAULT})
    void test_create_delete(String format) throws JsonProcessingException, SQLException {

        createLocation("Alder Springs",true,"SPK");
        String likePattern = "Alder Springs\\.Precip-Cumulative\\.Inst\\.15Minutes\\.0\\.DescriptorTEST_ID.*";

        // Check that we don't have any ts like this in the catalog.
        List<String> names = getIdsLike(OFFICE, likePattern);
        Assertions.assertTrue(names.isEmpty());

        ObjectMapper om = JsonV2.buildObjectMapper();
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

        // Create a bunch of ts and store them.
        int count = 8;
        for (int i = 0; i < count; i++) {
            String tsId = String.format("Alder Springs.Precip-Cumulative.Inst.15Minutes.0.DescriptorTEST_ID%d", i);
            TimeSeriesIdentifierDescriptor ts = buildTimeSeriesIdentifierDescriptor(OFFICE, tsId);
            String serializedTs = om.writeValueAsString(ts);

            given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(format)
                .contentType(Formats.JSONV2)
                .body(serializedTs)
                .header("Authorization", user.toHeaderValue())
                .queryParam("office",OFFICE)
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .post("/timeseries/identifier-descriptor/")
            .then()
                .log().ifValidationFails(LogDetail.ALL,true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_CREATED));
        }

        // Check that we have the right number of ts like this in the catalog.
        names = getIdsLike(OFFICE, likePattern);
        Assertions.assertFalse(names.isEmpty());
        assertEquals(count, names.size());

        // Now lets delete them
        for (int i = 0; i < count; i++) {
            String tsId = String.format("Alder Springs.Precip-Cumulative.Inst.15Minutes.0.DescriptorTEST_ID%d", i);

            // String urlencoded = java.net.URLEncoder.encode(tsId); // This isn't the right thing
            // to call here b/c it encodes a space into +
            // but the tsId is in the url part - not the url parameters part.
            // In the url part a + is a valid character - we must do the %20 type encoding for
            // the url part. For the params part you can do either + or %20

            // RestAssured does the right thing with the url encoding - we don't need to escape

            given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(format)
                .contentType(Formats.JSONV2)
                .queryParam("office", OFFICE)
                .queryParam(Controllers.METHOD,JooqDao.DeleteMethod.DELETE_ALL)
                .header("Authorization", user.toHeaderValue())
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .delete("/timeseries/identifier-descriptor/" + tsId)
            .then()
                .log().ifValidationFails(LogDetail.ALL,true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK));
        }


        // Check that we don't have any ts like this in the catalog.
        names = getIdsLike(OFFICE, likePattern);
        Assertions.assertTrue(names.isEmpty());
    }

    @ParameterizedTest
    @ValueSource(strings = {Formats.JSONV2, Formats.DEFAULT})
    void test_create_delete_new_LRTS_identifier(String format) throws JsonProcessingException, SQLException {

        createLocation("Alder Springs",true,"SPK");
        String likePattern = "Alder Springs\\.Precip-Cumulative\\.Inst\\.12HoursLocal\\.0\\.DescriptorTEST_LRTS*";

        // Check that we don't have any ts like this in the catalog.
        List<String> names = getIdsLike(OFFICE, likePattern);
        Assertions.assertTrue(names.isEmpty());

        ObjectMapper om = JsonV2.buildObjectMapper();
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

        String tsId = "Alder Springs.Precip-Cumulative.Inst.12HoursLocal.0.DescriptorTEST_LRTS1";
        TimeSeriesIdentifierDescriptor ts = buildTimeSeriesIdentifierDescriptor(OFFICE, tsId);
        String serializedTs = om.writeValueAsString(ts);

        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(format)
            .contentType(Formats.JSONV2)
            .body(serializedTs)
            .header("Authorization", user.toHeaderValue())
            .header(ApiServlet.IS_NEW_LRTS, true)
            .queryParam("office",OFFICE)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/timeseries/identifier-descriptor/")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED));

        // Check that we have the right number of ts like this in the catalog.
        names = getIdsLike(OFFICE, likePattern);
        Assertions.assertFalse(names.isEmpty());
        assertEquals(1, names.size());
        String name = names.get(0);

        assertEquals("12HoursLocal", name.split("\\.")[3]);

        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(format)
            .contentType(Formats.JSONV2)
            .queryParam("office", OFFICE)
            .queryParam(Controllers.METHOD,JooqDao.DeleteMethod.DELETE_ALL)
            .header("Authorization", user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("/timeseries/identifier-descriptor/{tsId}", tsId)
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK));

        // Check that we don't have any ts like this in the catalog.
        names = getIdsLike(OFFICE, likePattern);
        Assertions.assertTrue(names.isEmpty());

        // Try to store it again, but this time with the new LRTS flag set to false.
        var assertThat =
            given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(format)
                .contentType(Formats.JSONV2)
                .body(serializedTs)
                .header("Authorization", user.toHeaderValue())
                .header(ApiServlet.IS_NEW_LRTS, false)
                .queryParam("office",OFFICE)
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .post("/timeseries/identifier-descriptor/")
            .then()
                .log().ifValidationFails(LogDetail.ALL,true)
            .assertThat();
        if (getSchemaVersion() > SCHEMA_VERSION.V2025_07_01.numeric()) {
            assertThat
                .statusCode(is(HttpServletResponse.SC_BAD_REQUEST))
                .body("details.message",
                    is(String.format("Invalid time series description: ORA-20998: ERROR: " +
                                     "INVALID Time Series Identifier \"%s\": No such interval", tsId)));
        } else {
            assertThat
                .statusCode(is(HttpServletResponse.SC_BAD_REQUEST))
                .body("details.message",
                    is("Invalid time series description: "
                    + "12HoursLocal is not a valid interval"));
        }
    }

    @Test
    void test_invalid_ts_id() throws Exception {
        createLocation("BadLocationTSTest",true,"SPK");
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;
        String tsId = "BadLocationTSTest" +
            ".Precip-Cumulative.Ave.~12Hours.12HoursLocal.DescriptorTEST_LRTS25";
        TimeSeriesIdentifierDescriptor ts = buildTimeSeriesIdentifierDescriptor(OFFICE, tsId);

        ObjectMapper om = JsonV2.buildObjectMapper();
        String serializedTs = om.writeValueAsString(ts);
        var assertThat =
            given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(Formats.DEFAULT)
                .contentType(Formats.JSONV2)
                .body(serializedTs)
                .header("Authorization", user.toHeaderValue())
                .header(ApiServlet.IS_NEW_LRTS, false)
                .queryParam("office",OFFICE)
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .post("/timeseries/identifier-descriptor/")
            .then()
                .log().ifValidationFails(LogDetail.ALL,true)
            .assertThat()
                .statusCode(is(HttpServletResponse.SC_BAD_REQUEST))
                .body("message", equalTo("Bad Request."))
                .body("source", equalTo("User Input"))
            ;
        if (getSchemaVersion() > SCHEMA_VERSION.V2025_07_01.numeric()) {
            assertThat.body("details.message",
                is(String.format("Invalid time series description: ORA-20998: ERROR: INVALID Time Series Identifier \"%s\": No such duration", tsId)));
        } else {
            assertThat.body("details.message",
                is("Invalid time series description: 12HoursLocal is not a valid duration"));
        }
    }


    @NotNull
    private TimeSeriesIdentifierDescriptor buildTimeSeriesIdentifierDescriptor(String officeId, String tsId) {
        TimeSeriesIdentifierDescriptor.Builder builder =
                new TimeSeriesIdentifierDescriptor.Builder();
        builder = builder.withOfficeId(officeId);
        builder = builder.withTimeSeriesId(tsId);
        builder = builder.withZoneId(ZoneId.of("America/Los_Angeles"));
        builder = builder.withIntervalOffsetMinutes(0L);
        builder = builder.withActive(true);
        return builder.build();
    }




    private static List<String> getIdsLike( String officeId, String likePattern) {
        List<String> retval = new ArrayList<>();

        int pageSize = 8000;

        Response response = 
            given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(Formats.JSONV2)
                .queryParam(Controllers.PAGE_SIZE, pageSize)
                .queryParam(Controllers.OFFICE, officeId)
                .queryParam(Controllers.LIKE, likePattern)
                .queryParam(Controllers.EXCLUDE_EMPTY, false)
            .when()
                .get("/catalog/TIMESERIES")
            .then()
                .assertThat()
                .log().ifValidationFails(LogDetail.ALL,true)
                .statusCode(is(200))
                .extract().response();
        JsonPath jsonPath = response.jsonPath();

        List<String> names = jsonPath.getList("entries.name", String.class);
        if(names != null && !names.isEmpty()){
            retval.addAll(names);
        }
        return retval;
    }
}
