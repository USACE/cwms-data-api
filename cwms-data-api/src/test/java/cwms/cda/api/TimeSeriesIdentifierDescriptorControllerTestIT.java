/*
 * MIT License
 *
 * Copyright (c) 2025 Hydrologic Engineering Center
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package cwms.cda.api;

import static cwms.cda.api.Controllers.CATEGORY_ID;
import static cwms.cda.api.Controllers.FAIL_IF_EXISTS;
import static cwms.cda.api.Controllers.REPLACE_ASSIGNED_TS;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import cwms.cda.ApiServlet;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dto.AssignedLocation;
import cwms.cda.data.dto.AssignedTimeSeries;
import cwms.cda.data.dto.LocationCategory;
import cwms.cda.data.dto.LocationGroup;
import cwms.cda.data.dto.TimeSeriesCategory;
import cwms.cda.data.dto.TimeSeriesGroup;
import cwms.cda.data.dto.TimeSeriesIdentifierDescriptor;
import cwms.cda.formatters.ContentType;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;


@Tag("integration")
final class TimeSeriesIdentifierDescriptorControllerTestIT extends DataApiTestIT {
    private final List<TimeSeriesIdentifierDescriptor> tsDescriptors = new ArrayList<>();
    private final List<TimeSeriesGroup> tsGroups = new ArrayList<>();
    private final List<TimeSeriesCategory> tsCategories = new ArrayList<>();

    @AfterEach
    void tearDown() {
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;


        for (TimeSeriesIdentifierDescriptor ts : tsDescriptors) {
            given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(Formats.JSONV2)
                .contentType(Formats.JSONV2)
                .queryParam("office", OFFICE)
                .queryParam(Controllers.METHOD,JooqDao.DeleteMethod.DELETE_ALL)
                .header("Authorization", user.toHeaderValue())
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .delete("/timeseries/identifier-descriptor/" + ts.getTimeSeriesId())
            .then()
                .log().ifValidationFails(LogDetail.ALL,true)
            .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK));
        }
        for (TimeSeriesGroup group : tsGroups) {
            ContentType contentType = Formats.parseHeader(Formats.JSON, TimeSeriesCategory.class);

            // Clear assigned TS from group to allow for deletion
            group.getAssignedTimeSeries().clear();
            String groupXml = Formats.format(contentType, group);
            given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(Formats.JSON)
                .contentType(Formats.JSON)
                .body(groupXml)
                .header("Authorization", user.toHeaderValue())
                .queryParam(CATEGORY_ID, group.getTimeSeriesCategory().getId())
                .queryParam(REPLACE_ASSIGNED_TS, true)
                .queryParam(Controllers.OFFICE, OFFICE)
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .patch("/timeseries/group/"+ group.getId())
            .then()
                .log().ifValidationFails(LogDetail.ALL,true)
            .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK));

            // Delete group
            given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(Formats.JSON)
                .contentType(Formats.JSON)
                .header("Authorization", user.toHeaderValue())
                .queryParam(Controllers.OFFICE, OFFICE)
                .queryParam(CATEGORY_ID, group.getTimeSeriesCategory().getId())
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .delete("/timeseries/group/" + group.getId())
            .then()
                .log().ifValidationFails(LogDetail.ALL,true)
            .assertThat()
                .statusCode(is(HttpServletResponse.SC_NO_CONTENT));
        }
        for (TimeSeriesCategory cat : tsCategories) {
            //Delete Category
            given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(Formats.JSON)
                .contentType(Formats.JSON)
                .header("Authorization", user.toHeaderValue())
                .queryParam(Controllers.OFFICE, OFFICE)
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .delete("/timeseries/category/" + cat.getId())
            .then()
                .log().ifValidationFails(LogDetail.ALL,true)
            .assertThat()
                .statusCode(is(HttpServletResponse.SC_NO_CONTENT));
        }
    }


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
        assertFalse(names.isEmpty());
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
        assertFalse(names.isEmpty());
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

    @Test
    void pagingTest() throws Exception {
        createLocation("Alder Springs",true,"SPK");
        String likePattern = "Alder Springs\\.Precip-Cumulative\\.Inst\\.15Minutes\\.0\\.DescriptorTEST_PAGING.*";

        // Check that we don't have any ts like this in the catalog.
        List<String> names = getIdsLike(OFFICE, likePattern);
        Assertions.assertTrue(names.isEmpty());

        ObjectMapper om = JsonV2.buildObjectMapper();
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

        // Create a bunch of ts and store them.
        int count = 8;
        for (int i = 0; i < count; i++) {
            String tsId = String.format("Alder Springs.Precip-Cumulative.Inst.15Minutes.0.DescriptorTEST_PAGING%d", i);
            TimeSeriesIdentifierDescriptor ts = buildTimeSeriesIdentifierDescriptor(OFFICE, tsId);
            String serializedTs = om.writeValueAsString(ts);

            given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(Formats.JSONV2)
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
            tsDescriptors.add(ts);
        }

        // Add TS with differing ID to verify we don't get it back
        String tsId = "Alder Springs.Precip-Cumulative.Inst.15Minutes.0.DescriptorDO_NOT_INCLUDE";
        TimeSeriesIdentifierDescriptor ts = buildTimeSeriesIdentifierDescriptor(OFFICE, tsId);
        String serializedTs = om.writeValueAsString(ts);

        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV2)
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
        tsDescriptors.add(ts);

        // Check that we have the right number of ts like this in the catalog.
        names = getIdsLike(OFFICE, likePattern);
        assertFalse(names.isEmpty());
        assertEquals(count, names.size());


        // testing paging, make sure totals are present
        int pageSize = 5;
        Response response = given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV2)
            .queryParam(Controllers.PAGE_SIZE, pageSize)
            .queryParam(Controllers.OFFICE, OFFICE)
            .queryParam(Controllers.TIMESERIES_ID_REGEX, likePattern)
            .queryParam(Controllers.EXCLUDE_EMPTY, false)
        .when()
            .get("/timeseries/identifier-descriptor/")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("descriptors.size()", is(pageSize))
            .body("total", is(count))
            .body(not(contains("Alder Springs.Precip-Cumulative.Inst.15Minutes.0.DescriptorDO_NOT_INCLUDE")))
            .extract()
            .response();

       String nextPage =  response.path("next-page").toString();

       // verify correct total count on next page
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
        .accept(Formats.JSONV2)
            .queryParam(Controllers.PAGE, nextPage)
            .queryParam(Controllers.OFFICE, OFFICE)
            .queryParam(Controllers.TIMESERIES_ID_REGEX, likePattern)
            .queryParam(Controllers.EXCLUDE_EMPTY, false)
        .when()
            .get("/timeseries/identifier-descriptor/")
            .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("total", is(count))
            .body(not(contains("Alder Springs.Precip-Cumulative.Inst.15Minutes.0.DescriptorDO_NOT_INCLUDE")))
            .body("descriptors.size()", is(count - pageSize));

        // testing paging with aliases, make sure totals are present
        response = given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV2)
            .queryParam(Controllers.PAGE_SIZE, pageSize)
            .queryParam(Controllers.OFFICE, OFFICE)
            .queryParam(Controllers.TIMESERIES_ID_REGEX, likePattern)
            .queryParam(Controllers.EXCLUDE_EMPTY, false)
            .queryParam(Controllers.INCLUDE_ALIASES, true)
        .when()
            .get("/timeseries/identifier-descriptor/")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("descriptors.size()", is(pageSize))
            .body("total", is(count))
            .body(not(contains("Alder Springs.Precip-Cumulative.Inst.15Minutes.0.DescriptorDO_NOT_INCLUDE")))
            .extract()
            .response();

        nextPage =  response.path("next-page").toString();

        // verify correct total count on next page
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV2)
            .queryParam(Controllers.PAGE, nextPage)
            .queryParam(Controllers.OFFICE, OFFICE)
            .queryParam(Controllers.TIMESERIES_ID_REGEX, likePattern)
            .queryParam(Controllers.EXCLUDE_EMPTY, false)
            .queryParam(Controllers.INCLUDE_ALIASES, true)
        .when()
            .get("/timeseries/identifier-descriptor/")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("total", is(count))
            .body(not(contains("Alder Springs.Precip-Cumulative.Inst.15Minutes.0.DescriptorDO_NOT_INCLUDE")))
            .body("descriptors.size()", is(count - pageSize));
    }


    @Test
    void testAliasedTimeSeries() throws Exception {
        // Test Structure
        //
        // 1. Create many TimeSeries
        // 2. Store new category
        // 3. Add TS to group and store group
        // 4. Retrieve without aliases
        // 5. Retrieve with aliases

        createLocation("Alder Springs", true, "SPK");
        String likePattern = "Alder Springs\\.Precip-Cumulative\\.Inst\\.15Minutes\\.0\\.DescriptorTEST_ALIASES.*";

        // Check that we don't have any ts like this in the catalog.
        List<String> names = getIdsLike(OFFICE, likePattern);
        Assertions.assertTrue(names.isEmpty());

        ObjectMapper om = JsonV2.buildObjectMapper();
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

        // Create a bunch of ts and store them.
        int count = 8;
        for (int i = 0; i < count; i++) {
            String tsId = String.format("Alder Springs.Precip-Cumulative.Inst.15Minutes.0.DescriptorTEST_ALIASES%d", i);
            TimeSeriesIdentifierDescriptor ts = buildTimeSeriesIdentifierDescriptor(OFFICE, tsId);
            String serializedTs = om.writeValueAsString(ts);

            given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(Formats.JSONV2)
                .contentType(Formats.JSONV2)
                .body(serializedTs)
                .header("Authorization", user.toHeaderValue())
                .queryParam("office", OFFICE)
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .post("/timeseries/identifier-descriptor/")
            .then()
                .log().ifValidationFails(LogDetail.ALL, true)
            .assertThat()
                .statusCode(is(HttpServletResponse.SC_CREATED));
            tsDescriptors.add(ts);
        }

        // Check that we have the right number of ts like this in the catalog.
        names = getIdsLike(OFFICE, likePattern);
        assertFalse(names.isEmpty());
        assertEquals(count, names.size());

        // Add to group
        String catId = "Identifier Aliases";
        String groupId = "TSID Aliases";
        TimeSeriesCategory category = new TimeSeriesCategory(OFFICE, catId, "A test category");
        TimeSeriesGroup group = new TimeSeriesGroup(category, OFFICE, groupId, "A test group", "ALIASES", null);

        List<AssignedTimeSeries> assignedTimeSeries = group.getAssignedTimeSeries();

        for (String name : names) {
            assignedTimeSeries.add(new AssignedTimeSeries(OFFICE, name, "AliasId", null, 1));
        }
        ContentType contentType = Formats.parseHeader(Formats.JSON, TimeSeriesCategory.class);
        String categoryXml = Formats.format(contentType, category);
        group = new TimeSeriesGroup(group, assignedTimeSeries);
        String groupXml = Formats.format(contentType, group);

        //Create Category
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSON)
            .contentType(Formats.JSON)
            .body(categoryXml)
            .header("Authorization", user.toHeaderValue())
            .queryParam(Controllers.OFFICE, OFFICE)
            .queryParam(FAIL_IF_EXISTS, false)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/timeseries/category")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED));
        tsCategories.add(category);
        //Create Group
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSON)
            .contentType(Formats.JSON)
            .body(groupXml)
            .header("Authorization", user.toHeaderValue())
            .queryParam(FAIL_IF_EXISTS, false)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/timeseries/group")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED));
        tsGroups.add(group);

        // get without aliases
        Response response = given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV2)
            .queryParam(Controllers.OFFICE, OFFICE)
            .queryParam(Controllers.TIMESERIES_ID_REGEX, likePattern)
            .queryParam(Controllers.EXCLUDE_EMPTY, false)
        .when()
            .get("/timeseries/identifier-descriptor/")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("descriptors.size()", is(count))
            .body("total", is(count))
            .extract()
            .response();

        String content = response.getBody().prettyPrint();
        assertFalse(content.contains("aliases"));

        // get back with aliases
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV2)
            .queryParam(Controllers.OFFICE, OFFICE)
            .queryParam(Controllers.TIMESERIES_ID_REGEX, likePattern)
            .queryParam(Controllers.EXCLUDE_EMPTY, false)
            .queryParam(Controllers.INCLUDE_ALIASES, true)
        .when()
            .get("/timeseries/identifier-descriptor/")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("descriptors[0].aliases.size()", greaterThan(0));
    }

    @Test
    void testAliasedLocations() throws Exception {
        // Test Structure
        //
        // 1. Create many TimeSeries
        // 2. Store new location category
        // 3. Store a new location group
        // 4. Retrieve without aliases
        // 5. Retrieve with aliases

        createLocation("Alder Springs", true, "SPK");
        String likePattern = "Alder Springs\\.Precip-Cumulative\\.Inst\\.15Minutes\\.0\\.DescriptorLOC_ALIASES.*";

        // Check that we don't have any ts like this in the catalog.
        List<String> names = getIdsLike(OFFICE, likePattern);
        Assertions.assertTrue(names.isEmpty());

        ObjectMapper om = JsonV2.buildObjectMapper();
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

        // Create a bunch of ts and store them.
        int count = 8;
        for (int i = 0; i < count; i++) {
            String tsId = String.format("Alder Springs.Precip-Cumulative.Inst.15Minutes.0.DescriptorLOC_ALIASES%d", i);
            TimeSeriesIdentifierDescriptor ts = buildTimeSeriesIdentifierDescriptor(OFFICE, tsId);
            String serializedTs = om.writeValueAsString(ts);

            given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(Formats.JSONV2)
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
            tsDescriptors.add(ts);
        }

        // Check that we have the right number of ts like this in the catalog.
        names = getIdsLike(OFFICE, likePattern);
        assertFalse(names.isEmpty());
        assertEquals(count, names.size());

        // Add to group
        String catId = "Identifier Aliases";
        String groupId = "TSID Aliases";
        LocationCategory category = new LocationCategory(OFFICE, catId, "A test category");
        LocationGroup group = new LocationGroup(category, OFFICE, groupId, "A test group", "ALIASES", null, 1);

        AssignedLocation assignedLocation = new AssignedLocation("Alder Springs", OFFICE, "TS_ID_TEST", 1, null);
        ContentType contentType = Formats.parseHeader(Formats.JSON, LocationCategory.class);
        String categoryXml = Formats.format(contentType, category);
        group = new LocationGroup(group, List.of(assignedLocation));
        String groupXml = Formats.format(contentType, group);

        //Create Category
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSON)
            .contentType(Formats.JSON)
            .body(categoryXml)
            .header("Authorization", user.toHeaderValue())
            .queryParam(Controllers.OFFICE, OFFICE)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/location/category")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED));
        registerCategory(category);
        //Create Group
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSON)
            .contentType(Formats.JSON)
            .body(groupXml)
            .header("Authorization", user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/location/group")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED));
        registerGroup(group);

        // get without aliases
        Response response = given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV2)
            .queryParam(Controllers.OFFICE, OFFICE)
            .queryParam(Controllers.TIMESERIES_ID_REGEX, likePattern)
            .queryParam(Controllers.EXCLUDE_EMPTY, false)
        .when()
            .get("/timeseries/identifier-descriptor/")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("descriptors.size()", is(count))
            .body("total", is(count))
            .extract()
            .response();

        String content = response.getBody().prettyPrint();
        assertFalse(content.contains("aliases"));

        // get back with aliases
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
        .accept(Formats.JSONV2)
            .queryParam(Controllers.OFFICE, OFFICE)
            .queryParam(Controllers.TIMESERIES_ID_REGEX, likePattern)
            .queryParam(Controllers.EXCLUDE_EMPTY, false)
            .queryParam(Controllers.INCLUDE_ALIASES, true)
        .when()
            .get("/timeseries/identifier-descriptor/")
            .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("descriptors[0].aliases.size()", greaterThan(0));

        // Clear assigned locations from group to allow for deletion
        group.getAssignedLocations().clear();
        groupXml = Formats.format(contentType, group);
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSON)
            .contentType(Formats.JSON)
            .body(groupXml)
            .header("Authorization", user.toHeaderValue())
            .queryParam(CATEGORY_ID, group.getLocationCategory().getId())
            .queryParam(REPLACE_ASSIGNED_TS, true)
            .queryParam(Controllers.OFFICE, OFFICE)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .patch("/location/group/"+ group.getId())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK));
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
                .statusCode(is(HttpServletResponse.SC_OK))
                .extract().response();
        JsonPath jsonPath = response.jsonPath();

        List<String> names = jsonPath.getList("entries.name", String.class);
        if(names != null && !names.isEmpty()){
            retval.addAll(names);
        }
        return retval;
    }
}
