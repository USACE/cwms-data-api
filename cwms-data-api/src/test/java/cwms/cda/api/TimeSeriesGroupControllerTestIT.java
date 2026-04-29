/*
 * MIT License
 *
 * Copyright (c) 2023 Hydrologic Engineering Center
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

import static cwms.cda.api.Controllers.BEGIN;
import static cwms.cda.api.Controllers.CASCADE_DELETE;
import static cwms.cda.api.Controllers.CATEGORY_ID;
import static cwms.cda.api.Controllers.CATEGORY_OFFICE_ID;
import static cwms.cda.api.Controllers.CWMS_OFFICE;
import static cwms.cda.api.Controllers.END;
import static cwms.cda.api.Controllers.FAIL_IF_EXISTS;
import static cwms.cda.api.Controllers.GROUP_OFFICE_ID;
import static cwms.cda.api.Controllers.IGNORE_NULLS;
import static cwms.cda.api.Controllers.OFFICE;
import static cwms.cda.api.Controllers.REPLACE_ASSIGNED_LOCS;
import static cwms.cda.api.Controllers.REPLACE_ASSIGNED_TS;
import static cwms.cda.data.dao.Dao.formatBool;
import static io.restassured.RestAssured.given;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.flogger.FluentLogger;
import cwms.cda.ApiServlet;
import cwms.cda.api.errors.NotFoundException;
import cwms.cda.data.dao.TimeSeriesCategoryDao;
import cwms.cda.data.dao.TimeSeriesDaoImpl;
import cwms.cda.data.dao.TimeSeriesGroupDao;
import cwms.cda.data.dto.AssignedTimeSeries;
import cwms.cda.data.dto.LocationCategory;
import cwms.cda.data.dto.TimeSeries;
import cwms.cda.data.dto.TimeSeriesCategory;
import cwms.cda.data.dto.TimeSeriesGroup;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import cwms.cda.helpers.DatabaseHelpers.SCHEMA_VERSION;
import fixtures.CwmsDataApiSetupCallback;
import fixtures.FunctionalSchemas;
import fixtures.TestAccounts;
import io.restassured.filter.log.LogDetail;
import io.restassured.path.json.JsonPath;
import io.restassured.response.Response;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import javax.servlet.http.HttpServletResponse;
import mil.army.usace.hec.test.database.CwmsDatabaseContainer;
import org.apache.commons.io.IOUtils;
import org.hamcrest.Matchers;
import org.jooq.Configuration;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@Tag("integration")
final class TimeSeriesGroupControllerTestIT extends DataApiTestIT {

    private static final FluentLogger LOGGER = FluentLogger.forEnclosingClass();
    private final List<TimeSeriesCategory> categoriesToCleanup = new ArrayList<>();
    private final List<TimeSeriesGroup> groupsToCleanup = new ArrayList<>();

    private final List<TimeSeriesGroup> cwmsgroupsToSPKUnassign = new ArrayList<>();
    private final List<TimeSeries> timeSeriesToCleanup = new ArrayList<>();
    TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;
    TestAccounts.KeyUser user2 = TestAccounts.KeyUser.SWT_NORMAL;

    @BeforeAll
    static void load_data() throws Exception {
        createLocation("Alder Springs",true,"SPK");
        createLocation("Wet Meadows",true,"SPK");
        createLocation("Pine Flat-Outflow",true,"SPK");
        createTimeseries("SPK","Alder Springs.Precip-Cumulative.Inst.15Minutes.0.raw-cda");
        createTimeseries("SPK","Alder Springs.Precip-INC.Total.15Minutes.15Minutes.calc-cda");
        createTimeseries("SPK","Pine Flat-Outflow.Stage.Inst.15Minutes.0.raw-cda");
        createTimeseries("SPK","Wet Meadows.Depth-SWE.Inst.15Minutes.0.raw-cda");
        createLocation("Clear Creek",true,"LRL");
        createTimeseries("LRL","Clear Creek.Precip-Cumulative.Inst.15Minutes.0.raw-cda");
        loadSqlDataFromResource("cwms/cda/data/sql/mixed_ts_group.sql");
        loadSqlDataFromResource("cwms/cda/data/sql/spk_aliases_and_groups.sql");
    }

    @AfterAll
    static void tear_down() throws Exception {
        loadSqlDataFromResource("cwms/cda/data/sql/delete_mixed_ts_group.sql");
        loadSqlDataFromResource("cwms/cda/data/sql/delete_spk_aliases_and_groups.sql");
    }

    @AfterEach
    void clear_data() throws Exception {
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection(c -> {
            Configuration configuration = DSL.using(c).configuration();
            TimeSeriesGroupDao groupDao = new TimeSeriesGroupDao(configuration.dsl());
            TimeSeriesCategoryDao categoryDao = new TimeSeriesCategoryDao(configuration.dsl());
            TimeSeriesDaoImpl timeSeriesDao = new TimeSeriesDaoImpl(configuration.dsl(), new MetricRegistry());

            for (TimeSeriesGroup group : cwmsgroupsToSPKUnassign) {
                // We can't delete CWMS groups and we don't want to try to unassign "CWMS" assignments
                // We do want to unassign SPK assignments
                String assignOffice = user.getOperatingOffice();
                try {
                    groupDao.unassignForOffice(group.getTimeSeriesCategory().getId(), group.getId(), group.getOfficeId(), assignOffice );
                } catch (NotFoundException e) {
                    LOGGER.atConfig().withCause(e).log("Group not found");
                } catch (DataAccessException e) {
                    LOGGER.atInfo().withCause(e).log("Failed to unassign ts from %s that are in group owned by:%s", assignOffice, group.getOfficeId());
                }
            }
            cwmsgroupsToSPKUnassign.clear();


            for (TimeSeriesGroup group : groupsToCleanup) {
                String assignOffice = group.getOfficeId();
                try {
                    groupDao.unassignForOffice(group.getTimeSeriesCategory().getId(), group.getId(), group.getOfficeId(),  assignOffice);
                } catch (NotFoundException e) {
                    LOGGER.atConfig().withCause(e).log("Group not found");
                } catch (DataAccessException e) {
                    LOGGER.atInfo().withCause(e).log("Failed to unassign time series from office %s in group owned by %s", assignOffice, group.getOfficeId() );
                }

                try {
                    groupDao.delete(group.getTimeSeriesCategory().getId(), group.getId(), group.getOfficeId(), true);
                } catch (NotFoundException e) {
                    LOGGER.atConfig().withCause(e).log("Group not found");
                } catch (DataAccessException e) {
                    LOGGER.atInfo().withCause(e).log("Failed to delete time series from group in office %s", group.getOfficeId());
                }
            }
            for (TimeSeriesCategory category : categoriesToCleanup) {
                try {
                    categoryDao.delete(category.getId(), true, category.getOfficeId());
                } catch (NotFoundException e) {
                    LOGGER.atConfig().withCause(e).log("Category not found");
                }
            }
            for (TimeSeries ts : timeSeriesToCleanup) {
                try {
                    timeSeriesDao.delete(ts.getOfficeId(), ts.getName(), new TimeSeriesDaoImpl.DeleteOptions.Builder()
                            .withStartTimeInclusive(true).withEndTimeInclusive(true).withMaxVersion(false)
                            .withOverrideProtection(formatBool(true)).build());
                } catch (NotFoundException e) {
                    LOGGER.atConfig().withCause(e).log("Time Series not found");
                }
            }
            groupsToCleanup.clear();
            categoriesToCleanup.clear();
            timeSeriesToCleanup.clear();
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    @Test
    @Disabled("Unknown failure. Likely schema related.")
    void test_group_SPK() {

        Response response =
            given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept("application/json")
                .queryParam(OFFICE, user.getOperatingOffice())
            .when()
                .get("/timeseries/group")
            .then()
                .assertThat()
                .log().ifValidationFails(LogDetail.ALL,true)
                .statusCode(is(200))
                .body("$.size()", is(1),
                "[0].time-series-category.office-id", is(user.getOperatingOffice()),
                    "[0].office-id", is(user.getOperatingOffice()))
            .extract()
                .response();

        JsonPath jsonPathEval = response.jsonPath();
        List<String> ids = jsonPathEval.get("id");

        String testGroupId = "Test Group";
        assertThat("Response does not contain " + testGroupId, ids, Matchers.contains(testGroupId));
    }

    @Test
    @Disabled("Unknown Failure. Likely Schema related")
    void test_group_CWMS() {

        Response response = 
            given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept("application/json")
                .queryParam(OFFICE, CWMS_OFFICE)
                .queryParam(CATEGORY_OFFICE_ID, CWMS_OFFICE)
                .queryParam(GROUP_OFFICE_ID, CWMS_OFFICE)
            .when()
                .get("/timeseries/group")
            .then()
                .assertThat()
                .log().ifValidationFails(LogDetail.ALL,true)
                .statusCode(is(200))
                .body("$.size()",greaterThan(0))
            .extract()
                .response();
        JsonPath jsonPathEval = response.jsonPath();

        List<String> ids = jsonPathEval.get("id");

        String testGroupId = "Test Group2";
        assertThat("Response does not contain " + testGroupId, ids, Matchers.hasItem(testGroupId));

        int itemIndex = ids.indexOf(testGroupId);

        assertThat(jsonPathEval.get("[" + itemIndex + "].time-series-category.office-id"), Matchers.is(CWMS_OFFICE));

        List<String> tsIds = jsonPathEval.get("[" + itemIndex + "].assigned-time-series.timeseries-id");
        assertNotNull(tsIds);
        assertFalse(tsIds.isEmpty());

        String[] lookFor = {"Clear Creek.Precip-Cumulative.Inst.15Minutes.0.raw-cda",
                "Alder Springs.Precip-Cumulative.Inst.15Minutes.0.raw-cda"};

        for(final String tsId : lookFor)
        {
            assertThat("Response did not contain expected item", tsIds, Matchers.hasItem(tsId));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {Formats.JSON, Formats.DEFAULT})
    void test_create_read_delete(String format) throws Exception {
        String officeId = user.getOperatingOffice();
        String timeSeriesId = "Alder Springs.Precip-Cumulative.Inst.15Minutes.0.raw-cda";
        createLocation(timeSeriesId.split("\\.")[0],true,officeId);
        TimeSeriesCategory cat = new TimeSeriesCategory(officeId, "test_create_read_delete", "IntegrationTesting");
        TimeSeriesGroup group = new TimeSeriesGroup(cat, officeId, "test_create_read_delete", "IntegrationTesting",
                "sharedTsAliasId", timeSeriesId);
        List<AssignedTimeSeries> assignedTimeSeries = group.getAssignedTimeSeries();

        groupsToCleanup.add(group);
        categoriesToCleanup.add(cat);

        assignedTimeSeries.add(new AssignedTimeSeries(officeId,timeSeriesId, "AliasId", timeSeriesId, 1));
        ContentType contentType = Formats.parseHeader(Formats.JSON, TimeSeriesCategory.class);
        String categoryXml = Formats.format(contentType, cat);
        String groupXml = Formats.format(contentType, group);
        //Create Category
        given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(format)
                .contentType(Formats.JSON)
                .body(categoryXml)
                .header("Authorization", user.toHeaderValue())
                .queryParam(OFFICE, officeId)
                .queryParam(FAIL_IF_EXISTS, false)
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .post("/timeseries/category")
            .then()
                .assertThat()
                .log().ifValidationFails(LogDetail.ALL,true)
                .statusCode(is(HttpServletResponse.SC_CREATED));
        //Create Group
        given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(format)
                .contentType(Formats.JSON)
                .body(groupXml)
                .header("Authorization", user.toHeaderValue())
                .queryParam(FAIL_IF_EXISTS, false)
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .post("/timeseries/group")
            .then()
                .assertThat()
                .log().ifValidationFails(LogDetail.ALL,true)
                .statusCode(is(HttpServletResponse.SC_CREATED));
        //Read
        given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(format)
                .contentType(Formats.JSON)
                .queryParam(OFFICE, officeId)
                .queryParam(CATEGORY_OFFICE_ID, officeId)
                .queryParam(GROUP_OFFICE_ID, officeId)
                .queryParam(CATEGORY_ID, group.getTimeSeriesCategory().getId())
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/timeseries/group/" + group.getId())
            .then()
                .assertThat()
                .log().ifValidationFails(LogDetail.ALL,true)
                .statusCode(is(HttpServletResponse.SC_OK))
                .body("office-id", equalTo(group.getOfficeId()))
                .body("id", equalTo(group.getId()))
                .body("description", equalTo(group.getDescription()))
                .body("assigned-time-series[0].timeseries-id", equalTo(timeSeriesId))
                .body("assigned-time-series[0].alias-id", equalTo("AliasId"))
                .body("assigned-time-series[0].ref-ts-id", equalTo(timeSeriesId))
                .body("assigned-time-series[0].ts-code", nullValue());
        //Clear Assigned TS
        group.getAssignedTimeSeries().clear();
        groupXml = Formats.format(contentType, group);
        given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(format)
                .contentType(Formats.JSON)
                .body(groupXml)
                .header("Authorization", user.toHeaderValue())
                .queryParam(CATEGORY_ID, group.getTimeSeriesCategory().getId())
                .queryParam(REPLACE_ASSIGNED_TS, "true")
                .queryParam(OFFICE, group.getOfficeId())
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .patch("/timeseries/group/"+ group.getId())
            .then()
                .assertThat()
                .log().ifValidationFails(LogDetail.ALL,true)
                .statusCode(is(HttpServletResponse.SC_OK));
        //Delete Group
        given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(format)
                .contentType(Formats.JSON)
                .header("Authorization", user.toHeaderValue())
                .queryParam(OFFICE, officeId)
                .queryParam(CATEGORY_ID, cat.getId())
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .delete("/timeseries/group/" + group.getId())
            .then()
                .assertThat()
                .log().ifValidationFails(LogDetail.ALL,true)
                .statusCode(is(HttpServletResponse.SC_NO_CONTENT));

        //Read Empty
        given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(format)
                .contentType(Formats.JSON)
                .queryParam(OFFICE, officeId)
                .queryParam(GROUP_OFFICE_ID, officeId)
                .queryParam(CATEGORY_OFFICE_ID, officeId)
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/timeseries/group/" + group.getId())
            .then()
                .assertThat()
                .log().ifValidationFails(LogDetail.ALL,true)
                .statusCode(is(HttpServletResponse.SC_NOT_FOUND));
        //Delete Category
        given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(format)
                .contentType(Formats.JSON)
                .header("Authorization", user.toHeaderValue())
                .queryParam(OFFICE, officeId)
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .delete("/timeseries/category/" + group.getTimeSeriesCategory().getId())
            .then()
                .assertThat()
                .log().ifValidationFails(LogDetail.ALL,true)
                .statusCode(is(HttpServletResponse.SC_NO_CONTENT));
    }

    @ParameterizedTest
    @ValueSource(strings = {Formats.JSON, Formats.DEFAULT})
    void test_create_read_delete_LRTS(String format) throws Exception {
        String officeId = user.getOperatingOffice();
        String timeSeriesId = "Alder Springs.Precip-Cumulative.Inst.1DayLocal.0.cda-lrts";
        createLocation(timeSeriesId.split("\\.")[0],true,officeId);
        TimeSeriesCategory cat = new TimeSeriesCategory(officeId, "test_lrts", "IntegrationTesting");
        TimeSeriesGroup group = new TimeSeriesGroup(cat, officeId, "test_lrts", "IntegrationTesting",
                "sharedTsAliasId", timeSeriesId);

        groupsToCleanup.add(group);
        categoriesToCleanup.add(cat);

        List<AssignedTimeSeries> assignedTimeSeries = group.getAssignedTimeSeries();

        assignedTimeSeries.add(new AssignedTimeSeries(officeId,timeSeriesId, "AliasId", timeSeriesId, 1));
        ContentType contentType = Formats.parseHeader(Formats.JSON, TimeSeriesCategory.class);
        String categoryXml = Formats.format(contentType, cat);
        String groupXml = Formats.format(contentType, group);
        //Create Category
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(format)
            .contentType(Formats.JSON)
            .body(categoryXml)
            .header("Authorization", user.toHeaderValue())
            .header(ApiServlet.IS_NEW_LRTS, true)
            .queryParam(OFFICE, officeId)
            .queryParam(FAIL_IF_EXISTS, false)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/timeseries/category")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED));

        // inserting the time series
        createTimeseriesWithNewLRTSInterval(officeId, timeSeriesId, 0);

        // try to create a group without setting the LRTS header
        var assertThat =
            given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(format)
                .contentType(Formats.JSON)
                .body(groupXml)
                .header("Authorization", user.toHeaderValue())
                .header(ApiServlet.IS_NEW_LRTS, false)
                .queryParam(FAIL_IF_EXISTS, false)
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .post("/timeseries/group")
            .then()
                .log().ifValidationFails(LogDetail.ALL,true)
            .assertThat();
        if (getSchemaVersion() > SCHEMA_VERSION.V2025_07_01.numeric()) {
            assertThat.statusCode(is(HttpServletResponse.SC_BAD_REQUEST));
        } else {
            assertThat.statusCode(is(HttpServletResponse.SC_NOT_FOUND));
        }
        //Create Group
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(format)
            .contentType(Formats.JSON)
            .body(groupXml)
            .header("Authorization", user.toHeaderValue())
            .header(ApiServlet.IS_NEW_LRTS, true)
            .queryParam(FAIL_IF_EXISTS, false)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/timeseries/group")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED));

        //Read
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(format)
            .contentType(Formats.JSON)
            .header(ApiServlet.IS_NEW_LRTS, true)
            .queryParam(OFFICE, officeId)
            .queryParam(CATEGORY_OFFICE_ID, officeId)
            .queryParam(GROUP_OFFICE_ID, officeId)
            .queryParam(CATEGORY_ID, group.getTimeSeriesCategory().getId())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/group/" + group.getId())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("office-id", equalTo(group.getOfficeId()))
            .body("id", equalTo(group.getId()))
            .body("description", equalTo(group.getDescription()))
            .body("assigned-time-series[0].timeseries-id", equalTo(timeSeriesId))
            .body("assigned-time-series[0].alias-id", equalTo("AliasId"))
            .body("assigned-time-series[0].ref-ts-id", equalTo(timeSeriesId))
            .body("assigned-time-series[0].ts-code", nullValue());
        //Clear Assigned TS
        group.getAssignedTimeSeries().clear();
        groupXml = Formats.format(contentType, group);
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(format)
            .contentType(Formats.JSON)
            .body(groupXml)
            .header("Authorization", user.toHeaderValue())
            .header(ApiServlet.IS_NEW_LRTS, true)
            .queryParam(CATEGORY_ID, group.getTimeSeriesCategory().getId())
            .queryParam(REPLACE_ASSIGNED_TS, "true")
            .queryParam(OFFICE, group.getOfficeId())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .patch("/timeseries/group/"+ group.getId())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK));

        //Delete timeseries
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(format)
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
            .delete("/timeseries/" + timeSeriesId)
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK));

        //Delete Group
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(format)
            .contentType(Formats.JSON)
            .header("Authorization", user.toHeaderValue())
            .header(ApiServlet.IS_NEW_LRTS, true)
            .queryParam(OFFICE, officeId)
            .queryParam(CATEGORY_ID, cat.getId())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("/timeseries/group/" + group.getId())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NO_CONTENT));

        //Read Empty
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(format)
            .header(ApiServlet.IS_NEW_LRTS, true)
            .contentType(Formats.JSON)
            .queryParam(OFFICE, officeId)
            .queryParam(GROUP_OFFICE_ID, officeId)
            .queryParam(CATEGORY_OFFICE_ID, officeId)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/group/" + group.getId())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NOT_FOUND));
        //Delete Category
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(format)
            .contentType(Formats.JSON)
            .header(ApiServlet.IS_NEW_LRTS, true)
            .header("Authorization", user.toHeaderValue())
            .queryParam(OFFICE, officeId)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("/timeseries/category/" + group.getTimeSeriesCategory().getId())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NO_CONTENT));
    }

    @ParameterizedTest
    @ValueSource(strings = {Formats.JSON, Formats.DEFAULT})
    @FunctionalSchemas(values = {"99.99.99.9-CDA_STAGING"})
    void test_create_read_delete_agency_aliases_same_name(String format) throws Exception {
        // Create two location groups of the same name with an agency alias category
        String officeId = user.getOperatingOffice();
        String officeId2 = user2.getOperatingOffice();
        String timeSeriesId = "Alder Springs.Precip-Cumulative.Inst.15Minutes.0.raw-cda";
        String locationId = timeSeriesId.split("\\.")[0];
        createLocation(locationId, true, officeId);
        createLocation(locationId, true, officeId2);
        createTimeseries(officeId2, timeSeriesId);
        TimeSeriesCategory cat = new TimeSeriesCategory(CWMS_OFFICE, "Agency Aliases", "Time series aliases for various agencies");
        TimeSeriesGroup group = new TimeSeriesGroup(cat, officeId, "test_create_read_delete", "IntegrationTesting",
                "sharedTsAliasId", timeSeriesId);
        TimeSeriesGroup group3 = new TimeSeriesGroup(group, null);
        TimeSeriesGroup group2 = new TimeSeriesGroup(cat, officeId2, "test_create_read_delete", "IntegrationTesting",
                "sharedTsAliasId", timeSeriesId);
        TimeSeriesGroup group4 = new TimeSeriesGroup(group, null);
        List<AssignedTimeSeries> assignedTimeSeries = group.getAssignedTimeSeries();
        assignedTimeSeries.add(new AssignedTimeSeries(officeId,timeSeriesId, "AliasId", timeSeriesId, 1));
        ContentType contentType = Formats.parseHeader(Formats.JSON, TimeSeriesCategory.class);
        String groupXml = Formats.format(contentType, group);
        groupsToCleanup.add(group);
        groupsToCleanup.add(group2);
        //Create Group
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(format)
            .contentType(Formats.JSON)
            .body(groupXml)
            .header("Authorization", user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/timeseries/group")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED));
        //Create Group 2
        loadSqlDataFromResource("cwms/cda/data/sql/create_test_group2.sql");
        // Read
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(format)
            .contentType(Formats.JSON)
            .queryParam(OFFICE, officeId)
            .queryParam(CATEGORY_ID, group.getTimeSeriesCategory().getId())
            .queryParam(CATEGORY_OFFICE_ID, CWMS_OFFICE)
            .queryParam(GROUP_OFFICE_ID, officeId)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/group/" + group.getId())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("office-id", equalTo(group.getOfficeId()))
            .body("id", equalTo(group.getId()))
            .body("description", equalTo(group.getDescription()));
        //Read
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(format)
            .contentType(Formats.JSON)
            .queryParam(OFFICE, officeId2)
            .queryParam(CATEGORY_ID, group2.getTimeSeriesCategory().getId())
            .queryParam(CATEGORY_OFFICE_ID, CWMS_OFFICE)
            .queryParam(GROUP_OFFICE_ID, officeId2)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/group/" + group2.getId())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("office-id", equalTo(group2.getOfficeId()))
            .body("id", equalTo(group2.getId()))
            .body("description", equalTo(group2.getDescription()));
        // update group to unassign all time series
        groupXml = Formats.format(contentType, group3);
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(format)
            .contentType(Formats.JSON)
            .body(groupXml)
            .header("Authorization", user.toHeaderValue())
            .queryParam(OFFICE, group3.getOfficeId())
            .queryParam(REPLACE_ASSIGNED_TS, "true")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .patch("/timeseries/group/" + group3.getId())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK));
        // update group to unassign all time series
        groupXml = Formats.format(contentType, group4);
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(format)
            .contentType(Formats.JSON)
            .body(groupXml)
            .header("Authorization", user.toHeaderValue())
            .queryParam(OFFICE, group4.getOfficeId())
            .queryParam(REPLACE_ASSIGNED_TS, "true")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .patch("/timeseries/group/" + group4.getId())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK));
        //Delete Group
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(format)
            .contentType(Formats.JSON)
            .header("Authorization", user.toHeaderValue())
            .queryParam(OFFICE, officeId)
            .queryParam(CATEGORY_ID, cat.getId())
            .queryParam(CASCADE_DELETE, "true")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("/timeseries/group/" + group.getId())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NO_CONTENT));
        //Delete Group
        loadSqlDataFromResource("cwms/cda/data/sql/delete_test_group2.sql");

        //Read Empty
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(format)
            .contentType(Formats.JSON)
            .queryParam(OFFICE, officeId)
            .queryParam(CATEGORY_ID, group.getTimeSeriesCategory().getId())
            .queryParam(CATEGORY_OFFICE_ID, officeId)
            .queryParam(GROUP_OFFICE_ID, officeId)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/group/" + group.getId())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NOT_FOUND));
        //Read Empty
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(format)
            .contentType(Formats.JSON)
            .queryParam(OFFICE, officeId2)
            .queryParam(CATEGORY_ID, group2.getTimeSeriesCategory().getId())
            .queryParam(CATEGORY_OFFICE_ID,CWMS_OFFICE)
            .queryParam(GROUP_OFFICE_ID, officeId2)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/group/" + group2.getId())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NOT_FOUND));
    }

    @ParameterizedTest
    @ValueSource(strings = {Formats.JSON, Formats.DEFAULT})
    @FunctionalSchemas(values = {"99.99.99.9-CDA_STAGING"})
    void test_create_read_delete_same_names_different_offices(String format) throws Exception {
        // Create two location groups of the same name with an agency alias category
        String officeId = user.getOperatingOffice();
        String officeId2 = user2.getOperatingOffice();
        String timeSeriesId = "Alder Springs.Precip-Cumulative.Inst.15Minutes.0.raw-cda";
        String locationId = timeSeriesId.split("\\.")[0];
        createLocation(locationId,true, officeId);
        createLocation(locationId,true, officeId2);
        createTimeseries(officeId2, timeSeriesId);
        TimeSeriesCategory cat = new TimeSeriesCategory(officeId, "TestCategory2", "IntegrationTesting");
        TimeSeriesCategory cat2 = new TimeSeriesCategory(officeId2, "TestCategory2", "IntegrationTesting");
        TimeSeriesGroup group = new TimeSeriesGroup(cat, officeId, "test_create_read_delete", "IntegrationTesting",
                "sharedTsAliasId", timeSeriesId);
        TimeSeriesGroup group2 = new TimeSeriesGroup(cat2, officeId2, "test_create_read_delete", "IntegrationTesting",
                "sharedTsAliasId", timeSeriesId);
        ContentType contentType = Formats.parseHeader(Formats.JSON, LocationCategory.class);
        String groupXml = Formats.format(contentType, group);
        groupsToCleanup.add(group);
        groupsToCleanup.add(group2);
        String categoryXml = Formats.format(contentType, cat);
        categoriesToCleanup.add(cat);
        categoriesToCleanup.add(cat2);
        //Create Category
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(format)
            .contentType(Formats.JSON)
            .body(categoryXml)
            .header("Authorization", user.toHeaderValue())
            .queryParam(OFFICE, officeId)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/timeseries/category")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED));
        categoryXml = Formats.format(contentType, cat2);
        // Create Category 2
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(format)
            .contentType(Formats.JSON)
            .body(categoryXml)
            .header("Authorization", user2.toHeaderValue())
            .queryParam(OFFICE, officeId2)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/timeseries/category")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED));

        //Create Group
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(format)
            .contentType(Formats.JSON)
            .body(groupXml)
            .header("Authorization", user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/timeseries/group")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED));
        //Create Group 2
        loadSqlDataFromResource("cwms/cda/data/sql/create_test_group.sql");
        //Read
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(format)
            .contentType(Formats.JSON)
            .queryParam(OFFICE, officeId)
            .queryParam(CATEGORY_OFFICE_ID, officeId)
            .queryParam(GROUP_OFFICE_ID, officeId)
            .queryParam(CATEGORY_ID, group.getTimeSeriesCategory().getId())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/group/" + group.getId())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("office-id", equalTo(group.getOfficeId()))
            .body("id", equalTo(group.getId()))
            .body("description", equalTo(group.getDescription()));
        //Read
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(format)
            .contentType(Formats.JSON)
            .queryParam(OFFICE, officeId2)
            .queryParam(CATEGORY_ID, group2.getTimeSeriesCategory().getId())
            .queryParam(GROUP_OFFICE_ID, officeId2)
            .queryParam(CATEGORY_OFFICE_ID, officeId2)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/group/" + group2.getId())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("office-id", equalTo(group2.getOfficeId()))
            .body("id", equalTo(group2.getId()))
            .body("description", equalTo(group2.getDescription()));
        //Delete Group
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(format)
            .contentType(Formats.JSON)
            .header("Authorization", user.toHeaderValue())
            .queryParam(OFFICE, officeId)
            .queryParam(CATEGORY_ID, cat.getId())
            .queryParam(CASCADE_DELETE, "true")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("/timeseries/group/" + group.getId())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NO_CONTENT));
        //Delete Group
        loadSqlDataFromResource("cwms/cda/data/sql/delete_test_group.sql");

        //Read Empty
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(format)
            .contentType(Formats.JSON)
            .queryParam(OFFICE, officeId)
            .queryParam(CATEGORY_ID, group.getTimeSeriesCategory().getId())
            .queryParam(CATEGORY_OFFICE_ID, officeId)
            .queryParam(GROUP_OFFICE_ID, officeId)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/group/" + group.getId())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NOT_FOUND));
        //Read Empty
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(format)
            .contentType(Formats.JSON)
            .queryParam(OFFICE, officeId2)
            .queryParam(CATEGORY_ID, group2.getTimeSeriesCategory().getId())
            .queryParam(CATEGORY_OFFICE_ID, officeId2)
            .queryParam(GROUP_OFFICE_ID, officeId2)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/group/" + group2.getId())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NOT_FOUND));
    }

    @ParameterizedTest
    @ValueSource(strings = {Formats.JSON, Formats.DEFAULT})
    void test_rename_group(String format) throws Exception {
        String officeId = user.getOperatingOffice();
        String timeSeriesId = "Alder Springs.Precip-Cumulative.Inst.15Minutes.0.raw-cda";
        createLocation(timeSeriesId.split("\\.")[0],true,officeId);
        createTimeseries(officeId, timeSeriesId);

        TimeSeriesCategory cat = new TimeSeriesCategory(officeId, "test_rename_group_cat", "IntegrationTesting");
        categoriesToCleanup.add(cat);
        TimeSeriesGroup group = new TimeSeriesGroup(cat, officeId, "test_rename_group", "IntegrationTesting",
            "sharedTsAliasId", timeSeriesId);
        groupsToCleanup.add(group);
        List<AssignedTimeSeries> assignedTimeSeries = group.getAssignedTimeSeries();

        assignedTimeSeries.add(new AssignedTimeSeries(officeId,timeSeriesId, "AliasId", timeSeriesId, 1));
        ContentType contentType = Formats.parseHeader(Formats.JSON, TimeSeriesCategory.class);
        String categoryXml = Formats.format(contentType, cat);
        String groupXml = Formats.format(contentType, group);
        //Create Category
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(format)
            .contentType(Formats.JSON)
            .body(categoryXml)
            .header("Authorization", user.toHeaderValue())
            .queryParam(OFFICE, officeId)
            .queryParam(FAIL_IF_EXISTS, "false")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/timeseries/category/")
        .then()
            .assertThat()
            .log().ifValidationFails(LogDetail.ALL,true)
            .statusCode(is(HttpServletResponse.SC_CREATED));

        //Create Group
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(format)
            .contentType(Formats.JSON)
            .body(groupXml)
            .header("Authorization", user.toHeaderValue())
            .queryParam(FAIL_IF_EXISTS, "false")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/timeseries/group")
        .then()
            .assertThat()
            .log().ifValidationFails(LogDetail.ALL,true)
            .statusCode(is(HttpServletResponse.SC_CREATED));

        TimeSeriesGroup newGroup = new TimeSeriesGroup(cat, officeId, "test_rename_group_new", "Test group rename",
            "sharedTsAliasId2", timeSeriesId);
        String newGroupXml = Formats.format(contentType, newGroup);
        //Rename Group
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(format)
            .contentType(Formats.JSON)
            .body(newGroupXml)
            .header("Authorization", user.toHeaderValue())
            .header(CATEGORY_ID, group.getTimeSeriesCategory().getId())
            .queryParam(OFFICE, group.getOfficeId())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .patch("/timeseries/group/"+ group.getId())
        .then()
            .assertThat()
            .log().ifValidationFails(LogDetail.ALL,true)
            .statusCode(is(HttpServletResponse.SC_OK));
        groupsToCleanup.add(newGroup);
        groupsToCleanup.remove(group);
        //Read
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(format)
            .contentType(Formats.JSON)
            .queryParam(OFFICE, officeId)
            .queryParam(CATEGORY_OFFICE_ID, officeId)
            .queryParam(GROUP_OFFICE_ID, officeId)
            .queryParam(CATEGORY_ID, group.getTimeSeriesCategory().getId())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/group/" + newGroup.getId())
        .then()
            .assertThat()
            .log().ifValidationFails(LogDetail.ALL,true)
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("office-id", equalTo(newGroup.getOfficeId()))
            .body("id", equalTo(newGroup.getId()))
            .body("description", equalTo("Test group rename"))
            .body("assigned-time-series[0].timeseries-id", equalTo(timeSeriesId))
            .body("assigned-time-series[0].alias-id", equalTo("AliasId"))
            .body("assigned-time-series[0].ref-ts-id", equalTo(timeSeriesId));
        //Clear Assigned TS
        newGroup.getAssignedTimeSeries().clear();
        newGroupXml = Formats.format(contentType, newGroup);
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(format)
            .contentType(Formats.JSON)
            .body(newGroupXml)
            .header("Authorization", user.toHeaderValue())
            .queryParam(CATEGORY_ID, newGroup.getTimeSeriesCategory().getId())
            .queryParam(REPLACE_ASSIGNED_TS, true)
            .queryParam(OFFICE, newGroup.getOfficeId())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .patch("/timeseries/group/"+ newGroup.getId())
        .then()
            .assertThat()
            .log().ifValidationFails(LogDetail.ALL,true)
            .statusCode(is(HttpServletResponse.SC_OK));
        //Delete Group
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(format)
            .contentType(Formats.JSON)
            .header("Authorization", user.toHeaderValue())
            .queryParam(OFFICE, officeId)
            .queryParam(CATEGORY_ID, cat.getId())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("/timeseries/group/" + newGroup.getId())
        .then()
            .assertThat()
            .log().ifValidationFails(LogDetail.ALL,true)
            .statusCode(is(HttpServletResponse.SC_NO_CONTENT));
        //Delete Category
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(format)
            .contentType(Formats.JSON)
            .header("Authorization", user.toHeaderValue())
            .queryParam(OFFICE, officeId)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("/timeseries/category/" + group.getTimeSeriesCategory().getId())
        .then()
            .assertThat()
            .log().ifValidationFails(LogDetail.ALL,true)
            .statusCode(is(HttpServletResponse.SC_NO_CONTENT));
    }

    @ParameterizedTest
    @ValueSource(strings = {Formats.JSON, Formats.DEFAULT})
    void test_add_assigned_locs(String format) {
        String officeId = user.getOperatingOffice();
        String timeSeriesId = "Alder Springs.Precip-Cumulative.Inst.15Minutes.0.raw-cda";
        TimeSeriesCategory cat = new TimeSeriesCategory(officeId, "test_add_assigned_locs", "IntegrationTesting");
        categoriesToCleanup.add(cat);
        TimeSeriesGroup group = new TimeSeriesGroup(cat, officeId, "test_add_assigned_locs", "IntegrationTesting",
            "sharedTsAliasId", timeSeriesId);
        groupsToCleanup.add(group);
        List<AssignedTimeSeries> assignedTimeSeries = group.getAssignedTimeSeries();

        assignedTimeSeries.add(new AssignedTimeSeries(officeId, timeSeriesId, "AliasId", timeSeriesId, 1));
        ContentType contentType = Formats.parseHeader(Formats.JSON, TimeSeriesCategory.class);
        String categoryXml = Formats.format(contentType, cat);
        String groupXml = Formats.format(contentType, group);
        //Create Category
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(format)
            .contentType(Formats.JSON)
            .body(categoryXml)
            .header("Authorization", user.toHeaderValue())
            .queryParam(FAIL_IF_EXISTS, false)
            .queryParam(OFFICE, officeId)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/timeseries/category/")
        .then()
            .assertThat()
            .log().ifValidationFails(LogDetail.ALL,true)
            .statusCode(is(HttpServletResponse.SC_CREATED));
        //Create Group
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(format)
            .contentType(Formats.JSON)
            .body(groupXml)
            .header("Authorization", user.toHeaderValue())
            .queryParam(FAIL_IF_EXISTS, false)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/timeseries/group")
        .then()
            .assertThat()
            .log().ifValidationFails(LogDetail.ALL,true)
            .statusCode(is(HttpServletResponse.SC_CREATED));
        assignedTimeSeries.clear();
        String timeSeriesId2 = "Pine Flat-Outflow.Stage.Inst.15Minutes.0.raw-cda";
        assignedTimeSeries.add(new AssignedTimeSeries(officeId, timeSeriesId2, "AliasId2", timeSeriesId2, 2));
        groupXml = Formats.format(contentType, group);
        //Add Assigned Locs
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(format)
            .contentType(Formats.JSON)
            .body(groupXml)
            .header("Authorization", user.toHeaderValue())
            .queryParam(CATEGORY_ID, group.getTimeSeriesCategory().getId())
            .queryParam(REPLACE_ASSIGNED_LOCS, true)
            .queryParam(OFFICE, group.getOfficeId())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .patch("/timeseries/group/"+ group.getId())
        .then()
            .assertThat()
            .log().ifValidationFails(LogDetail.ALL,true)
            .statusCode(is(HttpServletResponse.SC_OK));
        //Read
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(format)
            .contentType(Formats.JSON)
            .queryParam(OFFICE, officeId)
            .queryParam(CATEGORY_OFFICE_ID, officeId)
            .queryParam(GROUP_OFFICE_ID, officeId)
            .queryParam(CATEGORY_ID, group.getTimeSeriesCategory().getId())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/group/" + group.getId())
        .then()
            .assertThat()
            .log().ifValidationFails(LogDetail.ALL,true)
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("office-id", equalTo(group.getOfficeId()))
            .body("id", equalTo(group.getId()))
            .body("description", equalTo(group.getDescription()))
            .body("assigned-time-series[1].timeseries-id", equalTo(timeSeriesId2))
            .body("assigned-time-series[1].alias-id", equalTo("AliasId2"))
            .body("assigned-time-series[1].ref-ts-id", equalTo(timeSeriesId2));
        //Clear Assigned TS
        group.getAssignedTimeSeries().clear();
        groupXml = Formats.format(contentType, group);
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(format)
            .contentType(Formats.JSON)
            .body(groupXml)
            .header("Authorization", user.toHeaderValue())
            .queryParam(CATEGORY_ID, group.getTimeSeriesCategory().getId())
            .queryParam(REPLACE_ASSIGNED_TS, true)
            .queryParam(OFFICE, group.getOfficeId())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .patch("/timeseries/group/"+ group.getId())
        .then()
            .assertThat()
            .log().ifValidationFails(LogDetail.ALL,true)
            .statusCode(is(HttpServletResponse.SC_OK));
        //Delete Group
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(format)
            .contentType(Formats.JSON)
            .header("Authorization", user.toHeaderValue())
            .queryParam(OFFICE, officeId)
            .queryParam(CATEGORY_ID, cat.getId())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("/timeseries/group/" + group.getId())
        .then()
            .assertThat()
            .log().ifValidationFails(LogDetail.ALL,true)
            .statusCode(is(HttpServletResponse.SC_NO_CONTENT));
        //Delete Category
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(format)
            .contentType(Formats.JSON)
            .header("Authorization", user.toHeaderValue())
            .queryParam(OFFICE, officeId)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("/timeseries/category/" + group.getTimeSeriesCategory().getId())
        .then()
            .assertThat()
            .log().ifValidationFails(LogDetail.ALL,true)
            .statusCode(is(HttpServletResponse.SC_NO_CONTENT));
    }

    @Disabled("CWMS_TS.UNASSIGN_TS_GROUP doesn't work for CWMS owned groups")  // https://github.com/USACE/cwms-data-api/issues/1631
    @ParameterizedTest
    @ValueSource(strings = {Formats.JSONV1, Formats.DEFAULT})
    void test_patch_permissions_CWMS(String format) throws Exception {
        ObjectMapper mapper = new ObjectMapper();

        InputStream resource = this.getClass().getResourceAsStream(
                "/cwms/cda/api/timeseries_create_SPK.json");
        assertNotNull(resource);
        String tsData = IOUtils.toString(resource, StandardCharsets.UTF_8)
                .replace("ZACK.Stage.Inst.5Minutes.0.ZSTORE_TS_TEST", "ZACK.Stage.Inst.5Minutes.0.ZSTORE_TS_TEST4");

        TimeSeries deserialize = Formats.parseContent(new ContentType(Formats.JSON), tsData, TimeSeries.class);
        timeSeriesToCleanup.add(deserialize);

        JsonNode ts = mapper.readTree(tsData);
        String location = ts.get("name").asText().split("\\.")[0];
        String officeId = ts.get("office-id").asText();
        try {
            createLocation(location, true, officeId);
        } catch (RuntimeException e) {
            // Location already exists
        }

        // inserting the time series
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(format)
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

        String categoryName = "Default";
        String groupId = "Default";
        String tsId = ts.get("name").asText();
        TimeSeriesCategory category = new TimeSeriesCategory(CWMS_OFFICE, categoryName, "Default");
        TimeSeriesGroup group = new TimeSeriesGroup(category, CWMS_OFFICE, groupId, "All Time Series", null, null);

        cwmsgroupsToSPKUnassign.add(group);
//        groupsToCleanup.add(group);  // This is a CWMS office.  We can't delete it
//        categoriesToCleanup.add(category);  // This is a CWMS office.  We can't delete it

        // Before we try to modify things - make sure there aren't any other SPK assignments
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(format)
            .contentType(Formats.JSONV1)
            .queryParam(OFFICE, officeId)   //  This param will get us only SPK assignments
            .queryParam(CATEGORY_OFFICE_ID, CWMS_OFFICE)
            .queryParam(GROUP_OFFICE_ID, CWMS_OFFICE)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/group/" + group.getId())
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("description", equalTo("All Time Series"))
            .body("assigned-time-series.size()", equalTo(0));

        AssignedTimeSeries assignedTimeSeries = new AssignedTimeSeries(officeId, tsId, null, null, null);
        TimeSeriesGroup newGroup = new TimeSeriesGroup(group, Collections.singletonList(assignedTimeSeries));

        String newGroupJson = Formats.format(new ContentType(Formats.JSONV1), newGroup);

        // Attempt a patch on TS owned by CWMS
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(format)
            .contentType(Formats.JSONV1)
            .header("Authorization", user2.toHeaderValue())
            .queryParam(OFFICE, officeId)
            .body(newGroupJson)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .patch("/timeseries/group/" + group.getId())
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK));

        // Retrieve the group and assert the changes
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(format)
            .contentType(Formats.JSONV1)
            .queryParam(OFFICE, officeId)
            .queryParam(GROUP_OFFICE_ID, CWMS_OFFICE)
            .queryParam(CATEGORY_OFFICE_ID, CWMS_OFFICE)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/group/" + group.getId())
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("description", equalTo("All Time Series"))
            .body("assigned-time-series.size()", equalTo(1))
            .body("assigned-time-series[0].timeseries-id", equalTo(tsId));
    }

    @Disabled("CWMS_TS.UNASSIGN_TS_GROUP doesn't work for CWMS owned groups")  // https://github.com/USACE/cwms-data-api/issues/1631
    @ParameterizedTest
    @ValueSource(strings = {Formats.JSONV1, Formats.DEFAULT})
    void test_patch_permissions_CWMS_with_replacement(String format) throws Exception {
        ObjectMapper mapper = new ObjectMapper();

        InputStream resource = this.getClass().getResourceAsStream(
                "/cwms/cda/api/timeseries_create_SPK.json");
        assertNotNull(resource);
        String tsData = IOUtils.toString(resource, StandardCharsets.UTF_8);
        String tsData2 = tsData
                .replace("ZACK.Stage.Inst.5Minutes.0.ZSTORE_TS_TEST", "ZACK.Stage.Inst.5Minutes.0.ZSTORE_TS_TEST2");

        TimeSeries deserialize = Formats.parseContent(new ContentType(Formats.JSON), tsData, TimeSeries.class);
        TimeSeries deserialize2 = Formats.parseContent(new ContentType(Formats.JSON), tsData2, TimeSeries.class);
        timeSeriesToCleanup.add(deserialize2);
        timeSeriesToCleanup.add(deserialize);

        JsonNode ts = mapper.readTree(tsData);
        JsonNode ts2 = mapper.readTree(tsData2);
        String location = ts.get("name").asText().split("\\.")[0];
        String officeId = ts.get("office-id").asText();
        try {
            createLocation(location, true, officeId);
        } catch (RuntimeException e) {
            // Location already exists
        }

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

        // inserting the time series
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .body(tsData2)
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

        String categoryName = "Default";
        String groupId = "Default";
        String tsId = ts.get("name").asText();
        String tsId2 = ts2.get("name").asText();
        TimeSeriesCategory category = new TimeSeriesCategory(CWMS_OFFICE, categoryName, "Default");
        TimeSeriesGroup group = new TimeSeriesGroup(category, CWMS_OFFICE, groupId, "All Time Series", null, null);

        cwmsgroupsToSPKUnassign.add(group); // can't delete CWMS groups but we should be able to unassign SPK assignments
//        categoriesToCleanup.add(category); // can't delete CWMS categories

        AssignedTimeSeries assignedTimeSeries = new AssignedTimeSeries(officeId, tsId, null, null, null);
        AssignedTimeSeries assignedTimeSeries2 = new AssignedTimeSeries(officeId, tsId2, null, null, null);
        TimeSeriesGroup newGroup = new TimeSeriesGroup(group, Arrays.asList(assignedTimeSeries2, assignedTimeSeries));

        String newGroupJson2 = Formats.format(new ContentType(Formats.JSONV1), newGroup);

//        groupsToCleanup.add(newGroup); // can't delete CWMS groups

        // Attempt a patch on TS owned by CWMS with replacement
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(format)
            .contentType(Formats.JSONV1)
            .header("Authorization", user.toHeaderValue())
            .queryParam(OFFICE, officeId)
            .queryParam(REPLACE_ASSIGNED_TS, true)
            .body(newGroupJson2)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .patch("/timeseries/group/" + newGroup.getId())
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK));

        // Retrieve the group and assert the changes
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(format)
            .contentType(Formats.JSONV1)
            .queryParam(OFFICE, officeId)
            .queryParam(GROUP_OFFICE_ID, CWMS_OFFICE)
            .queryParam(CATEGORY_OFFICE_ID, CWMS_OFFICE)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/group/" + newGroup.getId())
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("description", equalTo("All Time Series"))
            .body("assigned-time-series.size()", equalTo(2));
    }


    @ParameterizedTest
    @ValueSource(strings = {Formats.JSONV1, Formats.DEFAULT})
    void test_patch_district_permission(String format) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        InputStream resource = this.getClass().getResourceAsStream(
                "/cwms/cda/api/timeseries_create_SPK.json");
        assertNotNull(resource);
        String tsData = IOUtils.toString(resource, StandardCharsets.UTF_8)
                .replace("ZACK.Stage.Inst.5Minutes.0.ZSTORE_TS_TEST", "ZACK.Stage.Inst.5Minutes.0.ZSTORE_TS_TEST3");

        TimeSeries deserialize = Formats.parseContent(new ContentType(Formats.JSON), tsData, TimeSeries.class);
        timeSeriesToCleanup.add(deserialize);

        JsonNode ts = mapper.readTree(tsData);
        String location = ts.get("name").asText().split("\\.")[0];
        String officeId = ts.get("office-id").asText();
        try {
            createLocation(location, true, officeId);
        } catch (RuntimeException e) {
            // Location already exists
        }
        String tsId = ts.get("name").asText();

        TimeSeriesCategory category = new TimeSeriesCategory(CWMS_OFFICE, "Default", "Default");
        // also adds to cleanup
//        categoriesToCleanup.add(category); // cwms category, can't delete it.

        ContentType contentType = Formats.parseHeader(Formats.JSON, TimeSeriesCategory.class);
        String json = Formats.format(contentType, category);

        // Create Category
        given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(Formats.JSON)
                .contentType(Formats.JSON)
                .body(json)
                .header("Authorization", user.toHeaderValue())
                .when()
                .post("/timeseries/category")
                .then()
                .statusCode(anyOf(is(HttpServletResponse.SC_CREATED), is(HttpServletResponse.SC_CONFLICT)))
                ;

        TimeSeriesGroup districtGroup = new TimeSeriesGroup(category, CWMS_OFFICE, "Default", "All Time Series", null, null);

        cwmsgroupsToSPKUnassign.add(districtGroup);

        ContentType contentType1 = Formats.parseHeader(Formats.JSON, TimeSeriesGroup.class);
        String json1 = Formats.format(contentType1, districtGroup);

        // Create Group
        given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(Formats.JSON)
                .contentType(Formats.JSON)
                .body(json1)
                .header("Authorization", user.toHeaderValue())
                .when()
                .post("/timeseries/group")
                .then()
                .statusCode(anyOf(is(HttpServletResponse.SC_CREATED), is(HttpServletResponse.SC_CONFLICT)))
        ;

        AssignedTimeSeries assignedTimeSeries = new AssignedTimeSeries(officeId, tsId, null, null, null);
        TimeSeriesGroup newDistrictGroup = new TimeSeriesGroup(districtGroup, Collections.singletonList(assignedTimeSeries));

        String newDistrictGroupJson = Formats.format(new ContentType(Formats.JSONV1), newDistrictGroup);

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

        // Precondition - Verify the group is empty
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(format)
            .contentType(Formats.JSONV1)
            .queryParam(OFFICE, officeId) //limit retrieved assignments to office
            .queryParam(GROUP_OFFICE_ID, CWMS_OFFICE)
            .queryParam(CATEGORY_OFFICE_ID, CWMS_OFFICE)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/group/" + newDistrictGroup.getId())
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("id", equalTo("Default"))
            .body("assigned-time-series.size()", equalTo(0));

        // Attempt a patch on TS Group of assigned TS owned by SPK
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(format)
            .contentType(Formats.JSONV1)
            .header("Authorization", user.toHeaderValue())
            .queryParam(OFFICE, officeId)
            .body(newDistrictGroupJson)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .patch("/timeseries/group/" + districtGroup.getId())
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK));

        // Retrieve the group and assert the changes
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(format)
            .contentType(Formats.JSONV1)
            .queryParam(OFFICE, officeId)
            .queryParam(GROUP_OFFICE_ID, CWMS_OFFICE)
            .queryParam(CATEGORY_OFFICE_ID, CWMS_OFFICE)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/group/" + newDistrictGroup.getId())
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("id", equalTo("Default"))
            .body("assigned-time-series.size()", equalTo(1))
            .body("assigned-time-series[0].timeseries-id", equalTo(tsId));
    }

    @ParameterizedTest
    @ValueSource(strings = {Formats.JSON, Formats.DEFAULT})
    void testRetrieveOfficeParams(String format) throws Exception {
        String officeId = user.getOperatingOffice();
        String timeSeriesId = "Alder Springs.Precip-Cumulative.Inst.15Minutes.0.raw-cda";
        createLocation(timeSeriesId.split("\\.")[0],true,officeId);
        TimeSeriesCategory cat = new TimeSeriesCategory(CWMS_OFFICE, "Default", "Default");
        TimeSeriesGroup group = new TimeSeriesGroup(cat, officeId, "test_create_read_delete", "IntegrationTesting",
                "sharedTsAliasId", timeSeriesId);
        List<AssignedTimeSeries> assignedTimeSeries = group.getAssignedTimeSeries();

        groupsToCleanup.add(group);

        assignedTimeSeries.add(new AssignedTimeSeries(officeId,timeSeriesId, "AliasId", timeSeriesId, 1));
        ContentType contentType = Formats.parseHeader(Formats.JSON, TimeSeriesCategory.class);
        String groupXml = Formats.format(contentType, group);
        //Create Group
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(format)
            .contentType(Formats.JSON)
            .body(groupXml)
            .header("Authorization", user.toHeaderValue())
            .queryParam(FAIL_IF_EXISTS, false)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/timeseries/group")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED));
        //Read with specified office
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(format)
            .contentType(Formats.JSON)
            .queryParam(OFFICE, officeId)
            .queryParam(CATEGORY_OFFICE_ID, CWMS_OFFICE)
            .queryParam(GROUP_OFFICE_ID, officeId)
            .queryParam(CATEGORY_ID, group.getTimeSeriesCategory().getId())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/group/" + group.getId())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("office-id", equalTo(group.getOfficeId()))
            .body("id", equalTo(group.getId()))
            .body("description", equalTo(group.getDescription()))
            .body("assigned-time-series[0].timeseries-id", equalTo(timeSeriesId))
            .body("assigned-time-series[0].alias-id", equalTo("AliasId"))
            .body("assigned-time-series[0].ref-ts-id", equalTo(timeSeriesId));

        //Read without specified office
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(format)
            .contentType(Formats.JSON)
            .queryParam(CATEGORY_OFFICE_ID, CWMS_OFFICE)
            .queryParam(GROUP_OFFICE_ID, officeId)
            .queryParam(CATEGORY_ID, group.getTimeSeriesCategory().getId())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/group/" + group.getId())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("office-id", equalTo(group.getOfficeId()))
            .body("id", equalTo(group.getId()))
            .body("description", equalTo(group.getDescription()))
            .body("assigned-time-series[0].timeseries-id", equalTo(timeSeriesId))
            .body("assigned-time-series[0].alias-id", equalTo("AliasId"))
            .body("assigned-time-series[0].ref-ts-id", equalTo(timeSeriesId));
        //Clear Assigned TS
        group.getAssignedTimeSeries().clear();
        groupXml = Formats.format(contentType, group);
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(format)
            .contentType(Formats.JSON)
            .body(groupXml)
            .header("Authorization", user.toHeaderValue())
            .queryParam(CATEGORY_ID, group.getTimeSeriesCategory().getId())
            .queryParam(REPLACE_ASSIGNED_TS, "true")
            .queryParam(OFFICE, group.getOfficeId())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .patch("/timeseries/group/"+ group.getId())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK));
        //Delete Group
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(format)
            .contentType(Formats.JSON)
            .header("Authorization", user.toHeaderValue())
            .queryParam(OFFICE, officeId)
            .queryParam(CATEGORY_ID, cat.getId())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("/timeseries/group/" + group.getId())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NO_CONTENT));

        //Read Empty
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(format)
            .contentType(Formats.JSON)
            .queryParam(OFFICE, officeId)
            .queryParam(GROUP_OFFICE_ID, officeId)
            .queryParam(CATEGORY_OFFICE_ID, officeId)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/group/" + group.getId())
        .then()
        .assertThat()
            .log().ifValidationFails(LogDetail.ALL,true)
            .statusCode(is(HttpServletResponse.SC_NOT_FOUND));
    }

    @Test
    void testRetrievalTiming() {
        String officeId = user.getOperatingOffice();

        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSON)
            .contentType(Formats.JSON)
            .queryParam(GROUP_OFFICE_ID, officeId)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/group/")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("assigned-time-series.size()", greaterThan(0))
            .time(lessThan(500L)); // should be pretty quick, under 0.5 seconds. Old query was ~3 seconds
    }

    @ParameterizedTest
    @ValueSource(strings = {Formats.JSON, Formats.DEFAULT})
    void test_create_read_delete_null_attributes(String format) throws Exception {
        String officeId = user.getOperatingOffice();
        String timeSeriesId = "Alder Springs.Precip-Cumulative.Inst.1Day.0.cda-attr";
        createLocation(timeSeriesId.split("\\.")[0],true, officeId);
        TimeSeriesCategory cat = new TimeSeriesCategory(officeId, "test_attr", "NullTesting");
        TimeSeriesGroup group = new TimeSeriesGroup(cat, officeId, "test_attr", "NullTesting",
            null, null);
        List<AssignedTimeSeries> assignedTimeSeries = group.getAssignedTimeSeries();

        assignedTimeSeries.add(new AssignedTimeSeries(officeId,timeSeriesId, null, null, 0));
        ContentType contentType = Formats.parseHeader(Formats.JSON, TimeSeriesCategory.class);
        String categoryXml = Formats.format(contentType, cat);
        String groupXml = Formats.format(contentType, group);

        categoriesToCleanup.add(cat);
        groupsToCleanup.add(group);

        //Create Category
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(format)
            .contentType(Formats.JSON)
            .body(categoryXml)
            .header("Authorization", user.toHeaderValue())
            .queryParam(OFFICE, officeId)
            .queryParam(FAIL_IF_EXISTS, false)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/timeseries/category")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED));

        // insert the time series
        createTimeseries(officeId, timeSeriesId, 0);

        //Create Group
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(format)
            .contentType(Formats.JSON)
            .body(groupXml)
            .header("Authorization", user.toHeaderValue())
            .queryParam(FAIL_IF_EXISTS, false)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/timeseries/group")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED));

        //Read
        var tsGroup = given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(format)
            .contentType(Formats.JSON)
            .queryParam(OFFICE, officeId)
            .queryParam(CATEGORY_OFFICE_ID, officeId)
            .queryParam(GROUP_OFFICE_ID, officeId)
            .queryParam(CATEGORY_ID, group.getTimeSeriesCategory().getId())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/group/" + group.getId())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("office-id", equalTo(group.getOfficeId()))
            .body("id", equalTo(group.getId()))
            .body("description", equalTo(group.getDescription()))
            .body("assigned-time-series[0].timeseries-id", equalTo(timeSeriesId))
            .extract();

        var body = tsGroup.body().jsonPath().getList("assigned-time-series");
        for (Object o : body) {
            String content = o.toString();
            assertFalse(content.contains("ref-ts-id"));
            assertFalse(content.contains("ts-code"));
            assertFalse(content.contains("alias-id"));
            assertFalse(content.contains("null"));
        }

        //Clear Assigned TS
        group.getAssignedTimeSeries().clear();
        groupXml = Formats.format(contentType, group);
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(format)
            .contentType(Formats.JSON)
            .body(groupXml)
            .header("Authorization", user.toHeaderValue())
            .queryParam(CATEGORY_ID, group.getTimeSeriesCategory().getId())
            .queryParam(REPLACE_ASSIGNED_TS, true)
            .queryParam(OFFICE, group.getOfficeId())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .patch("/timeseries/group/"+ group.getId())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK));

        //Delete timeseries
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(format)
            .header("Authorization", user.toHeaderValue())
            .queryParam(OFFICE, officeId)
            .queryParam(BEGIN, "2025-05-08T11:00:00+00:00")
            .queryParam(END, "2025-05-19T11:00:00+00:00")
            .queryParam("start-time-inclusive", "true")
            .queryParam("end-time-inclusive", "true")
            .queryParam("override-protection", "true")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("/timeseries/" + timeSeriesId)
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK));

        //Delete Group
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(format)
            .contentType(Formats.JSON)
            .header("Authorization", user.toHeaderValue())
            .queryParam(OFFICE, officeId)
            .queryParam(CATEGORY_ID, cat.getId())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("/timeseries/group/" + group.getId())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NO_CONTENT));

        //Read Empty
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(format)
            .contentType(Formats.JSON)
            .queryParam(OFFICE, officeId)
            .queryParam(GROUP_OFFICE_ID, officeId)
            .queryParam(CATEGORY_OFFICE_ID, officeId)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/group/" + group.getId())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NOT_FOUND));

        //Delete Category
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(format)
            .contentType(Formats.JSON)
            .header("Authorization", user.toHeaderValue())
            .queryParam(OFFICE, officeId)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("/timeseries/category/" + group.getTimeSeriesCategory().getId())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NO_CONTENT));
    }

    @ParameterizedTest
    @ValueSource(strings = {Formats.JSON, Formats.DEFAULT})
    void test_delete_respects_cascade_delete_query_param(String format) throws Exception {
        String officeId = user.getOperatingOffice();
        String timeSeriesId = "Alder Springs.Precip-Cumulative.Inst.15Minutes.0.raw-cda";

        // Ensure TS exists (load_data usually covers this, but this makes the test resilient)
        createLocation(timeSeriesId.split("\\.")[0], true, officeId);
        createTimeseries(officeId, timeSeriesId);

        String suffix = String.valueOf(System.currentTimeMillis());
        suffix = suffix.substring(suffix.length() - 6);
        TimeSeriesCategory cat = new TimeSeriesCategory(officeId, "test_c_d_" + suffix, "IntegrationTesting");
        TimeSeriesGroup group = new TimeSeriesGroup(cat, officeId, "test_c_d_" + suffix, "IntegrationTesting",
                "sharedTsAliasId", timeSeriesId);

        group.getAssignedTimeSeries().add(new AssignedTimeSeries(officeId, timeSeriesId, "AliasId", timeSeriesId, 1));

        // Let @AfterEach clean up if we fail mid-test
        categoriesToCleanup.add(cat);
        groupsToCleanup.add(group);

        ContentType contentType = Formats.parseHeader(Formats.JSON, TimeSeriesCategory.class);
        String categoryBody = Formats.format(contentType, cat);
        String groupBody = Formats.format(contentType, group);

        // Create Category
        given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(format)
                .contentType(Formats.JSON)
                .body(categoryBody)
                .header("Authorization", user.toHeaderValue())
                .queryParam(OFFICE, officeId)
                .queryParam(FAIL_IF_EXISTS, false)
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .post("/timeseries/category")
            .then()
                .log().ifValidationFails(LogDetail.ALL, true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_CREATED));

        // Create Group (with an assigned time series)
        given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(format)
                .contentType(Formats.JSON)
                .body(groupBody)
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

        // Sanity check: group exists and has an assignment
        given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(format)
                .contentType(Formats.JSON)
                .queryParam(OFFICE, officeId)
                .queryParam(CATEGORY_OFFICE_ID, officeId)
                .queryParam(GROUP_OFFICE_ID, officeId)
                .queryParam(CATEGORY_ID, cat.getId())
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/timeseries/group/" + group.getId())
            .then()
                .log().ifValidationFails(LogDetail.ALL, true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
                .body("assigned-time-series.size()", greaterThan(0));

        // Delete WITHOUT cascade_delete: should NOT remove the group (assignments still exist)
        given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(format)
                .contentType(Formats.JSON)
                .header("Authorization", user.toHeaderValue())
                .queryParam(OFFICE, officeId)
                .queryParam(CATEGORY_ID, cat.getId())
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .delete("/timeseries/group/" + group.getId())
            .then()
                .log().ifValidationFails(LogDetail.ALL, true)
                .assertThat()
                .statusCode(not(is(HttpServletResponse.SC_NO_CONTENT)));

        // Still exists
        given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(format)
                .contentType(Formats.JSON)
                .queryParam(OFFICE, officeId)
                .queryParam(CATEGORY_OFFICE_ID, officeId)
                .queryParam(GROUP_OFFICE_ID, officeId)
                .queryParam(CATEGORY_ID, cat.getId())
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/timeseries/group/" + group.getId())
            .then()
                .log().ifValidationFails(LogDetail.ALL, true)
                .assertThat()
                .body("assigned-time-series.size()", greaterThan(0))
                .statusCode(is(HttpServletResponse.SC_OK));

        // Delete WITH cascade_delete=true: should succeed even with assignments present
        given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(format)
                .contentType(Formats.JSON)
                .header("Authorization", user.toHeaderValue())
                .queryParam(OFFICE, officeId)
                .queryParam(CATEGORY_ID, cat.getId())
                .queryParam(CASCADE_DELETE, true)
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .delete("/timeseries/group/" + group.getId())
            .then()
                .log().ifValidationFails(LogDetail.ALL, true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_NO_CONTENT));

        // Now it's gone
        given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(format)
                .contentType(Formats.JSON)
                .queryParam(OFFICE, officeId)
                .queryParam(CATEGORY_OFFICE_ID, officeId)
                .queryParam(GROUP_OFFICE_ID, officeId)
                .queryParam(CATEGORY_ID, cat.getId())
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/timeseries/group/" + group.getId())
            .then()
                .log().ifValidationFails(LogDetail.ALL, true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_NOT_FOUND));
    }

    @ParameterizedTest
    @ValueSource(strings = {Formats.JSON, Formats.DEFAULT})
    void test_create_with_ignore_nulls_parameter(String format) throws Exception {
        String officeId = user.getOperatingOffice();
        String timeSeriesId = "Alder Springs.Precip-Cumulative.Inst.15Minutes.0.raw-cda";
        createLocation(timeSeriesId.split("\\.")[0], true, officeId);

        String suffix = String.valueOf(System.currentTimeMillis());
        suffix = suffix.substring(suffix.length() - 6);
        String catId = "test_ignore_nulls_" + suffix;
        String groupId = "test_ignore_nulls_" + suffix;

        TimeSeriesCategory cat = new TimeSeriesCategory(officeId, catId, "Initial category description");
        TimeSeriesGroup group = new TimeSeriesGroup(cat, officeId, groupId, "Initial group description",
                "sharedTsAliasId", timeSeriesId);

        // Let @AfterEach clean up if we fail mid-test
        categoriesToCleanup.add(cat);
        groupsToCleanup.add(group);

        group.getAssignedTimeSeries().add(new AssignedTimeSeries(officeId, timeSeriesId, "AliasId", timeSeriesId, 1));

        ContentType contentType = Formats.parseHeader(Formats.JSON, TimeSeriesCategory.class);
        String categoryJson = Formats.format(contentType, cat);
        String groupJson = Formats.format(contentType, group);

        // Create Category
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(format)
            .contentType(Formats.JSON)
            .body(categoryJson)
            .header("Authorization", user.toHeaderValue())
            .queryParam(OFFICE, officeId)
            .queryParam(FAIL_IF_EXISTS, false)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/timeseries/category")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED));

        // Create Group initially with description and assigned time series
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(format)
            .contentType(Formats.JSON)
            .body(groupJson)
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

        // Verify initial state
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(format)
            .contentType(Formats.JSON)
            .queryParam(OFFICE, officeId)
            .queryParam(CATEGORY_OFFICE_ID, officeId)
            .queryParam(GROUP_OFFICE_ID, officeId)
            .queryParam(CATEGORY_ID, cat.getId())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/group/" + groupId)
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("description", equalTo("Initial group description"))
            .body("assigned-time-series.size()", equalTo(1))
            .body("assigned-time-series[0].timeseries-id", equalTo(timeSeriesId));

        // Re-create with ignore-nulls=true (default) and null description
        // Using JSON string to explicitly send null
        String nullDescriptionJson = "{\"office-id\":\"" + officeId + "\",\"id\":\"" + groupId
                + "\",\"description\":null,\"time-series-category\":{\"office-id\":\"" + officeId
                + "\",\"id\":\"" + catId + "\"}}";

        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(format)
            .contentType(Formats.JSON)
            .body(nullDescriptionJson)
            .header("Authorization", user.toHeaderValue())
            .queryParam(FAIL_IF_EXISTS, false)
            .queryParam(IGNORE_NULLS, true)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/timeseries/group")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED));

        // Verify description was NOT updated to null because ignore-nulls=true
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(format)
            .contentType(Formats.JSON)
            .queryParam(OFFICE, officeId)
            .queryParam(CATEGORY_OFFICE_ID, officeId)
            .queryParam(GROUP_OFFICE_ID, officeId)
            .queryParam(CATEGORY_ID, cat.getId())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/group/" + groupId)
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("description", equalTo("Initial group description"))
            .body("assigned-time-series.size()", equalTo(1));

        // Re-create with ignore-nulls=false and null description
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(format)
            .contentType(Formats.JSON)
            .body(nullDescriptionJson)
            .header("Authorization", user.toHeaderValue())
            .queryParam(FAIL_IF_EXISTS, false)
            .queryParam(IGNORE_NULLS, false)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/timeseries/group")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED));

        // Verify description WAS updated to null/empty
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(format)
            .contentType(Formats.JSON)
            .queryParam(OFFICE, officeId)
            .queryParam(CATEGORY_OFFICE_ID, officeId)
            .queryParam(GROUP_OFFICE_ID, officeId)
            .queryParam(CATEGORY_ID, cat.getId())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/group/" + groupId)
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("description", is(org.hamcrest.Matchers.anyOf(equalTo(""), org.hamcrest.Matchers.nullValue())));

        // Test ignore-nulls=false with empty assigned-time-series list
        // Re-create with original description to set it back
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(format)
            .contentType(Formats.JSON)
            .body(groupJson)
            .header("Authorization", user.toHeaderValue())
            .queryParam(FAIL_IF_EXISTS, false)
            .queryParam(IGNORE_NULLS, false)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/timeseries/group")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED));

        // Re-create with ignore-nulls=false and empty assigned-time-series
        String emptyAssignedTsJson = "{\"office-id\":\"" + officeId + "\",\"id\":\"" + groupId
                + "\",\"description\":\"Updated description\",\"time-series-category\":{\"office-id\":\"" + officeId
                + "\",\"id\":\"" + catId + "\"}" +
//                ",\"assigned-time-series\":[]" +  // empty is different than null.
                "}";

        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(format)
            .contentType(Formats.JSON)
            .body(emptyAssignedTsJson)
            .header("Authorization", user.toHeaderValue())
            .queryParam(FAIL_IF_EXISTS, false)
            .queryParam(IGNORE_NULLS, false)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/timeseries/group")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED));

        // Verify assigned time series list was cleared
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(format)
            .contentType(Formats.JSON)
            .queryParam(OFFICE, officeId)
            .queryParam(CATEGORY_OFFICE_ID, officeId)
            .queryParam(GROUP_OFFICE_ID, officeId)
            .queryParam(CATEGORY_ID, cat.getId())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/group/" + groupId)
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("description", equalTo("Updated description"))
            .body("assigned-time-series.size()", equalTo(0));

        // Re-create with ignore-nulls=true (default) and empty assigned-time-series
        // Should preserve the empty list since it's not a valid DB empty list
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(format)
            .contentType(Formats.JSON)
            .body(groupJson)
            .header("Authorization", user.toHeaderValue())
            .queryParam(FAIL_IF_EXISTS, false)
            .queryParam(IGNORE_NULLS, true)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/timeseries/group")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED));

        // Verify assigned time series were added back
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(format)
            .contentType(Formats.JSON)
            .queryParam(OFFICE, officeId)
            .queryParam(CATEGORY_OFFICE_ID, officeId)
            .queryParam(GROUP_OFFICE_ID, officeId)
            .queryParam(CATEGORY_ID, cat.getId())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/group/" + groupId)
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("assigned-time-series.size()", equalTo(1));

        // delete category and group gets done by afterEach.
    }

    @Test
    void test_get_all_groups_without_assigned_filters_by_office() {
        String officeId = user.getOperatingOffice();

        // Get all groups for this office with include-assigned=false
        // This tests the fix where groupOfficeId filter was not being applied when includeAssigned=false
        Response response = given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept("application/json")
            .queryParam(GROUP_OFFICE_ID, officeId)
            .queryParam("include-assigned", false)
        .when()
            .get("/timeseries/group")
        .then()
            .assertThat()
            .log().ifValidationFails(LogDetail.ALL, true)
            .statusCode(anyOf(is(200), is(404)))
        .extract()
            .response();

        // If we get a 404, that's fine (no groups for this office)
        // If we get a 200, verify all returned groups are from the specified office
        if (response.statusCode() == 200) {
            JsonPath jsonPathEval = response.jsonPath();
            List<String> officeIds = jsonPathEval.get("office-id");

            if (officeIds != null && !officeIds.isEmpty()) {
                // All returned groups should be from the specified office
                for (String returnedOfficeId : officeIds) {
                    assertThat("Expected all groups to be from office " + officeId + ", but found " + returnedOfficeId,
                            returnedOfficeId, equalTo(officeId));
                }
            }
        }
    }

}
