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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import cwms.cda.api.errors.NotFoundException;
import cwms.cda.data.dao.TimeSeriesCategoryDao;
import cwms.cda.data.dao.TimeSeriesDaoImpl;
import cwms.cda.data.dao.TimeSeriesGroupDao;
import cwms.cda.data.dto.TimeSeries;
import fixtures.CwmsDataApiSetupCallback;
import fixtures.TestAccounts;
import io.restassured.filter.log.LogDetail;
import io.restassured.path.json.JsonPath;
import io.restassured.response.Response;
import mil.army.usace.hec.test.database.CwmsDatabaseContainer;
import org.apache.commons.io.IOUtils;
import org.hamcrest.Matchers;
import org.jooq.Configuration;
import org.jooq.impl.DSL;
import org.jooq.util.oracle.OracleDSL;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import cwms.cda.data.dto.AssignedTimeSeries;
import cwms.cda.data.dto.TimeSeriesCategory;
import cwms.cda.data.dto.TimeSeriesGroup;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import usace.cwms.db.jooq.codegen.packages.CWMS_TS_PACKAGE;
import usace.cwms.db.jooq.codegen.packages.CWMS_UTIL_PACKAGE;

import javax.servlet.http.HttpServletResponse;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import static cwms.cda.api.Controllers.*;
import static cwms.cda.data.dao.JooqDao.formatBool;
import static io.restassured.RestAssured.given;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@Tag("integration")
class TimeSeriesGroupControllerTestIT extends DataApiTestIT {

    private List<TimeSeriesCategory> categoriesToCleanup = new ArrayList<>();
    private List<TimeSeriesGroup> groupsToCleanup = new ArrayList<>();
    private List<TimeSeries> timeSeriesToCleanup = new ArrayList<>();
    private static final Logger LOGGER = Logger.getLogger(TimeSeriesGroupControllerTestIT.class.getName());
    TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

    @BeforeAll
    public static void load_data() throws Exception {
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
    public static void tear_down() throws Exception {
        loadSqlDataFromResource("cwms/cda/data/sql/delete_mixed_ts_group.sql");
        loadSqlDataFromResource("cwms/cda/data/sql/delete_spk_aliases_and_groups.sql");
    }

    @AfterEach
    public void clear_data() throws Exception {
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection(c -> {
            Configuration configuration = DSL.using(c).configuration();
            TimeSeriesGroupDao groupDao = new TimeSeriesGroupDao(configuration.dsl());
            TimeSeriesCategoryDao categoryDao = new TimeSeriesCategoryDao(configuration.dsl());
            TimeSeriesDaoImpl timeSeriesDao = new TimeSeriesDaoImpl(configuration.dsl());

            for (TimeSeriesGroup group : groupsToCleanup) {
                try {
                   groupDao.unassignAllTs(group, "SPK");
                    if (!group.getOfficeId().equalsIgnoreCase(CWMS_OFFICE)) {
                        groupDao.delete(group.getTimeSeriesCategory().getId(), group.getId(), group.getOfficeId());
                    }
                } catch (NotFoundException e) {
                    LOGGER.log(Level.CONFIG, "Group not found", e);
                }
            }
            for (TimeSeriesCategory category : categoriesToCleanup) {
                try {
                    categoryDao.delete(category.getId(), true, category.getOfficeId());
                } catch (NotFoundException e) {
                    LOGGER.log(Level.CONFIG, "Category not found", e);
                }
            }
            for (TimeSeries ts : timeSeriesToCleanup) {
                try {
                    timeSeriesDao.delete(ts.getOfficeId(), ts.getName(), new TimeSeriesDaoImpl.DeleteOptions.Builder()
                            .withStartTimeInclusive(true).withEndTimeInclusive(true).withMaxVersion(false)
                            .withOverrideProtection(formatBool(true)).build());
                } catch (NotFoundException e) {
                    LOGGER.log(Level.CONFIG, "Time Series not found", e);
                }
            }
            groupsToCleanup.clear();
            categoriesToCleanup.clear();
            timeSeriesToCleanup.clear();
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    @Test
    void test_group_SPK() {

        Response response =
            given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept("application/json")
                .queryParam("office", user.getOperatingOffice())
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
    void test_group_CWMS() {

        Response response = 
            given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept("application/json")
                .queryParam("office", CWMS_OFFICE)
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

    @Test
    void test_create_read_delete() throws Exception {
        String officeId = user.getOperatingOffice();
        String timeSeriesId = "Alder Springs.Precip-Cumulative.Inst.15Minutes.0.raw-cda";
        createLocation(timeSeriesId.split("\\.")[0],true,officeId);
        createTimeseries(officeId,timeSeriesId);
        TimeSeriesCategory cat = new TimeSeriesCategory(officeId, "test_create_read_delete", "IntegrationTesting");
        TimeSeriesGroup group = new TimeSeriesGroup(cat, officeId, "test_create_read_delete", "IntegrationTesting",
            "sharedTsAliasId", timeSeriesId);
        List<AssignedTimeSeries> assignedTimeSeries = group.getAssignedTimeSeries();

        BigDecimal tsCode = getTsCode(officeId, timeSeriesId);
        assignedTimeSeries.add(new AssignedTimeSeries(officeId,timeSeriesId, tsCode, "AliasId", timeSeriesId, 1));
        ContentType contentType = Formats.parseHeader(Formats.JSON, TimeSeriesCategory.class);
        String categoryXml = Formats.format(contentType, cat);
        String groupXml = Formats.format(contentType, group);
        //Create Category
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSON)
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
            .assertThat()
            .log().ifValidationFails(LogDetail.ALL,true)
            .statusCode(is(HttpServletResponse.SC_CREATED));
        //Read
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSON)
            .contentType(Formats.JSON)
            .queryParam(OFFICE, officeId)
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
            .body("assigned-time-series[0].ref-ts-id", equalTo(timeSeriesId));
        //Clear Assigned TS
        group.getAssignedTimeSeries().clear();
        groupXml = Formats.format(contentType, group);
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSON)
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
            .accept(Formats.JSON)
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
            .accept(Formats.JSON)
            .contentType(Formats.JSON)
            .queryParam(OFFICE, officeId)
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
            .accept(Formats.JSON)
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

    private static BigDecimal getTsCode(String officeId, String timeSeriesId) throws SQLException {
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        return db.connection(c -> {
            Configuration configuration = OracleDSL.using(c).configuration();
            BigDecimal officeCode = CWMS_UTIL_PACKAGE.call_GET_OFFICE_CODE(configuration, officeId);
            return CWMS_TS_PACKAGE.call_GET_TS_CODE(configuration, timeSeriesId, officeCode);
        }, db.getPdUser());
    }

    @Test
    void test_rename_group() throws Exception {
        String officeId = user.getOperatingOffice();
        String timeSeriesId = "Alder Springs.Precip-Cumulative.Inst.15Minutes.0.raw-cda";
        createLocation(timeSeriesId.split("\\.")[0],true,officeId);
        createTimeseries(officeId, timeSeriesId);

        TimeSeriesCategory cat = new TimeSeriesCategory(officeId, "test_rename_group_cat", "IntegrationTesting");
        TimeSeriesGroup group = new TimeSeriesGroup(cat, officeId, "test_rename_group", "IntegrationTesting",
            "sharedTsAliasId", timeSeriesId);
        List<AssignedTimeSeries> assignedTimeSeries = group.getAssignedTimeSeries();

        BigDecimal tsCode = getTsCode(officeId, timeSeriesId);
        assignedTimeSeries.add(new AssignedTimeSeries(officeId,timeSeriesId, tsCode, "AliasId", timeSeriesId, 1));
        ContentType contentType = Formats.parseHeader(Formats.JSON, TimeSeriesCategory.class);
        String categoryXml = Formats.format(contentType, cat);
        String groupXml = Formats.format(contentType, group);
        //Create Category
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSON)
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
            .accept(Formats.JSON)
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
        TimeSeriesGroup newGroup = new TimeSeriesGroup(cat, officeId, "test_rename_group_new", "IntegrationTesting",
            "sharedTsAliasId2", timeSeriesId);
        String newGroupXml = Formats.format(contentType, newGroup);
        //Rename Group
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSON)
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
        //Read
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSON)
            .contentType(Formats.JSON)
            .queryParam(OFFICE, officeId)
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
            .body("description", equalTo(newGroup.getDescription()))
            .body("assigned-time-series[0].timeseries-id", equalTo(timeSeriesId))
            .body("assigned-time-series[0].alias-id", equalTo("AliasId"))
            .body("assigned-time-series[0].ref-ts-id", equalTo(timeSeriesId));
        //Clear Assigned TS
        newGroup.getAssignedTimeSeries().clear();
        newGroupXml = Formats.format(contentType, newGroup);
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSON)
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
            .accept(Formats.JSON)
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
            .accept(Formats.JSON)
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

    @Test
    void test_add_assigned_locs() throws Exception {
        String officeId = user.getOperatingOffice();
        String timeSeriesId = "Alder Springs.Precip-Cumulative.Inst.15Minutes.0.raw-cda";
        TimeSeriesCategory cat = new TimeSeriesCategory(officeId, "test_add_assigned_locs", "IntegrationTesting");
        TimeSeriesGroup group = new TimeSeriesGroup(cat, officeId, "test_add_assigned_locs", "IntegrationTesting",
            "sharedTsAliasId", timeSeriesId);
        List<AssignedTimeSeries> assignedTimeSeries = group.getAssignedTimeSeries();

        BigDecimal tsCode = getTsCode(officeId, timeSeriesId);
        assignedTimeSeries.add(new AssignedTimeSeries(officeId, timeSeriesId, tsCode, "AliasId", timeSeriesId, 1));
        ContentType contentType = Formats.parseHeader(Formats.JSON, TimeSeriesCategory.class);
        String categoryXml = Formats.format(contentType, cat);
        String groupXml = Formats.format(contentType, group);
        //Create Category
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSON)
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
            .assertThat()
            .log().ifValidationFails(LogDetail.ALL,true)
            .statusCode(is(HttpServletResponse.SC_CREATED));
        assignedTimeSeries.clear();
        String timeSeriesId2 = "Pine Flat-Outflow.Stage.Inst.15Minutes.0.raw-cda";
        BigDecimal tsCode2 = getTsCode(officeId, timeSeriesId2);
        assignedTimeSeries.add(new AssignedTimeSeries(officeId, timeSeriesId2, tsCode2, "AliasId2", timeSeriesId2, 2));
        groupXml = Formats.format(contentType, group);
        //Add Assigned Locs
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSON)
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
            .accept(Formats.JSON)
            .contentType(Formats.JSON)
            .queryParam(OFFICE, officeId)
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
            .accept(Formats.JSON)
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
            .accept(Formats.JSON)
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
            .accept(Formats.JSON)
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

    @Test
    void test_patch_permissions_CWMS() throws Exception {
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
        createLocation(location, true, officeId);

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

        String categoryName = "Default";
        String groupId = "Default";
        String tsId = ts.get("name").asText();
        TimeSeriesCategory category = new TimeSeriesCategory(CWMS_OFFICE, categoryName, "Default");
        TimeSeriesGroup group = new TimeSeriesGroup(category, CWMS_OFFICE, groupId, "All Time Series", null, null);
        AssignedTimeSeries assignedTimeSeries = new AssignedTimeSeries(officeId, tsId, null, null, null, null);
        TimeSeriesGroup newGroup = new TimeSeriesGroup(group, Collections.singletonList(assignedTimeSeries));

        String newGroupJson = Formats.format(new ContentType(Formats.JSONV1), newGroup);

        groupsToCleanup.add(newGroup);

        // Retrieve the group and assert it's empty
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV1)
            .contentType(Formats.JSONV1)
            .queryParam(OFFICE, CWMS_OFFICE)
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

        // Attempt a patch on TS owned by CWMS
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV1)
            .contentType(Formats.JSONV1)
            .header("Authorization", user.toHeaderValue())
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
            .accept(Formats.JSONV1)
            .contentType(Formats.JSONV1)
            .queryParam(OFFICE, CWMS_OFFICE)
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

    @Test
    void test_patch_permissions_CWMS_with_replacement() throws Exception {
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
        createLocation(location, true, officeId);

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
        AssignedTimeSeries assignedTimeSeries = new AssignedTimeSeries(officeId, tsId, null, null, null, null);
        AssignedTimeSeries assignedTimeSeries2 = new AssignedTimeSeries(officeId, tsId2, null, null, null, null);
        TimeSeriesGroup newGroup = new TimeSeriesGroup(group, Arrays.asList(assignedTimeSeries2, assignedTimeSeries));

        String newGroupJson2 = Formats.format(new ContentType(Formats.JSONV1), newGroup);

        groupsToCleanup.add(newGroup);

        // Attempt a patch on TS owned by CWMS with replacement
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV1)
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
            .accept(Formats.JSONV1)
            .contentType(Formats.JSONV1)
            .queryParam(OFFICE, CWMS_OFFICE)
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

    @Test
    void test_patch_district_permission() throws Exception {
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
        createLocation(location, true, officeId);
        String tsId = ts.get("name").asText();

        TimeSeriesCategory category = new TimeSeriesCategory(CWMS_OFFICE, "Default", "Default");
        TimeSeriesGroup districtGroup = new TimeSeriesGroup(category, CWMS_OFFICE, "Default", "All Time Series", null, null);
        AssignedTimeSeries assignedTimeSeries = new AssignedTimeSeries(officeId, tsId, null, null, null, null);
        TimeSeriesGroup newDistrictGroup = new TimeSeriesGroup(districtGroup, Collections.singletonList(assignedTimeSeries));
        groupsToCleanup.add(newDistrictGroup);

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

        // Verify the group is empty
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV1)
            .contentType(Formats.JSONV1)
            .header("Authorization", user.toHeaderValue())
            .queryParam(OFFICE, CWMS_OFFICE)
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
            .accept(Formats.JSONV1)
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
            .accept(Formats.JSONV1)
            .contentType(Formats.JSONV1)
            .header("Authorization", user.toHeaderValue())
            .queryParam(OFFICE, CWMS_OFFICE)
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
}
