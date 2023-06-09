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

package cwms.radar.api;

import cwms.radar.data.dto.AssignedTimeSeries;
import cwms.radar.data.dto.TimeSeriesCategory;
import cwms.radar.data.dto.TimeSeriesGroup;
import cwms.radar.formatters.ContentType;
import cwms.radar.formatters.Formats;
import fixtures.CwmsDataApiSetupCallback;
import fixtures.TestAccounts;
import io.restassured.path.json.JsonPath;
import io.restassured.response.Response;
import mil.army.usace.hec.test.database.CwmsDatabaseContainer;
import org.hamcrest.Matchers;
import org.jooq.Configuration;
import org.jooq.util.oracle.OracleDSL;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import usace.cwms.db.jooq.codegen.packages.CWMS_TS_PACKAGE;
import usace.cwms.db.jooq.codegen.packages.CWMS_UTIL_PACKAGE;

import javax.servlet.http.HttpServletResponse;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.List;

import static cwms.radar.api.Controllers.*;
import static io.restassured.RestAssured.given;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@Tag("integration")
@Disabled() // not clearing groups correctly.
class TimeSeriesGroupControllerTestIT extends DataApiTestIT {

    public static final String CWMS_OFFICE = "CWMS";

    @BeforeAll
    public static void load_data() throws Exception {
        createLocation("Alder Springs",true,"SPK");
        createLocation("Wet Meadows",true,"SPK");
        createLocation("Pine Flat-Outflow",true,"SPK");
        createTimeseries("SPK","Alder Springs.Precip-Cumulative.Inst.15Minutes.0.raw-radar");
        createTimeseries("SPK","Alder Springs.Precip-INC.Total.15Minutes.15Minutes.calc-radar");
        createTimeseries("SPK","Pine Flat-Outflow.Stage.Inst.15Minutes.0.raw-radar");
        createTimeseries("SPK","Wet Meadows.Depth-SWE.Inst.15Minutes.0.raw-radar");
        createLocation("Clear Creek",true,"LRL");
        createTimeseries("LRL","Clear Creek.Precip-Cumulative.Inst.15Minutes.0.raw-radar");
        loadSqlDataFromResource("cwms/cda/data/sql/mixed_ts_group.sql");
        loadSqlDataFromResource("cwms/cda/data/sql/spk_aliases_and_groups.sql");
    }

    @Test
    void test_group_SPK(){

        Response response = given()
                .accept("application/json")
                .queryParam("office", "SPK")
                .get("/timeseries/group");

        response.then().assertThat()
        .statusCode(is(200))
        .body(
                "$.size()", is(1),
                "[0].time-series-category.office-id", is("SPK"),
                "[0].office-id", is("SPK")
        );

        JsonPath jsonPathEval = response.jsonPath();
        List<String> ids = jsonPathEval.get("id");

        String testGroupId = "Test Group";
        assertThat("Response does not contain " + testGroupId, ids, Matchers.contains(testGroupId));
    }

    @Test
    void test_group_CWMS(){

        Response response = given()
                .accept("application/json")
                .queryParam("office", CWMS_OFFICE)
                .get("/timeseries/group");

        JsonPath jsonPathEval = response.jsonPath();
        response.then().assertThat()
                .statusCode(is(200))
                .body("$.size()",greaterThan(0))
        ;

        List<String> ids = jsonPathEval.get("id");

        String testGroupId = "Test Group2";
        assertThat("Response does not contain " + testGroupId, ids, Matchers.hasItem(testGroupId));

        int itemIndex = ids.indexOf(testGroupId);

        assertThat(jsonPathEval.get("[" + itemIndex + "].time-series-category.office-id"), Matchers.is("CWMS"));

        List<String> tsIds = jsonPathEval.get("[" + itemIndex + "].assigned-time-series.timeseries-id");
        assertNotNull(tsIds);
        assertFalse(tsIds.isEmpty());

        String[] lookFor = {"Clear Creek.Precip-Cumulative.Inst.15Minutes.0.raw-radar",
                "Alder Springs.Precip-Cumulative.Inst.15Minutes.0.raw-radar"};

        for(final String tsId : lookFor)
        {
            assertThat("Response did not contain expected item", tsIds, Matchers.hasItem(tsId));
        }
    }

    @Test
    void test_create_read_delete() throws Exception {
        String officeId = "SPK";
        String timeSeriesId = "Alder Springs.Precip-Cumulative.Inst.15Minutes.0.raw-radar";
        createLocation(timeSeriesId.split("\\.")[0],true,officeId);
        createTimeseries(officeId,timeSeriesId);
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;
        TimeSeriesCategory cat = new TimeSeriesCategory(officeId, "test_create_read_delete", "IntegrationTesting");
        TimeSeriesGroup group = new TimeSeriesGroup(cat, officeId, "test_create_read_delete", "IntegrationTesting",
            "sharedTsAliasId", timeSeriesId);
        List<AssignedTimeSeries> assignedTimeSeries = group.getAssignedTimeSeries();

        BigDecimal tsCode = getTsCode(officeId, timeSeriesId);
        assignedTimeSeries.add(new AssignedTimeSeries(timeSeriesId, tsCode, "AliasId", timeSeriesId, 1));
        ContentType contentType = Formats.parseHeaderAndQueryParm(Formats.JSON, null);
        String categoryXml = Formats.format(contentType, cat);
        String groupXml = Formats.format(contentType, group);
        //Create Category
        given()
            .accept(Formats.JSON)
            .contentType(Formats.JSON)
            .body(categoryXml)
            .header("Authorization", user.toHeaderValue())
            .queryParam(OFFICE, officeId)
            .queryParam(FAIL_IF_EXISTS, "false")
            .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/timeseries/category")
            .then()
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED));
        //Create Group
        given()
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
            .statusCode(is(HttpServletResponse.SC_CREATED));
        //Read
        given()
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
            .log().body().log().everything(true)
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
            .statusCode(is(HttpServletResponse.SC_ACCEPTED));
        //Delete Group
        given()
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
            .log().body().log().everything(true)
            .statusCode(is(HttpServletResponse.SC_NO_CONTENT));

        //Read Empty
        given()
            .accept(Formats.JSON)
            .contentType(Formats.JSON)
            .queryParam("office", officeId)
            .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/group/" + group.getId())
            .then()
            .assertThat()
            .log().body().log().everything(true)
            .statusCode(is(HttpServletResponse.SC_NOT_FOUND));
        //Delete Category
        given()
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
            .log().body().log().everything(true)
            .statusCode(is(HttpServletResponse.SC_NO_CONTENT));
    }

    private static BigDecimal getTsCode(String officeId, String timeSeriesId) throws SQLException {
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        BigDecimal tsCode = db.connection((c) -> {
            Configuration configuration = OracleDSL.using(c).configuration();
            BigDecimal officeCode = CWMS_UTIL_PACKAGE.call_GET_OFFICE_CODE(configuration, officeId);
            return CWMS_TS_PACKAGE.call_GET_TS_CODE(configuration, timeSeriesId, officeCode);
        }, db.getPdUser());
        return tsCode;
    }

    @Test
    void test_rename_group() throws Exception {
        String officeId = "SPK";
        String timeSeriesId = "Alder Springs.Precip-Cumulative.Inst.15Minutes.0.raw-radar";
        createLocation(timeSeriesId.split("\\.")[0],true,officeId);
        createTimeseries(officeId, timeSeriesId);
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;
        TimeSeriesCategory cat = new TimeSeriesCategory(officeId, "test_rename_group_cat", "IntegrationTesting");
        TimeSeriesGroup group = new TimeSeriesGroup(cat, officeId, "test_rename_group", "IntegrationTesting",
            "sharedTsAliasId", timeSeriesId);
        List<AssignedTimeSeries> assignedTimeSeries = group.getAssignedTimeSeries();

        BigDecimal tsCode = getTsCode(officeId, timeSeriesId);
        assignedTimeSeries.add(new AssignedTimeSeries(timeSeriesId, tsCode, "AliasId", timeSeriesId, 1));
        ContentType contentType = Formats.parseHeaderAndQueryParm(Formats.JSON, null);
        String categoryXml = Formats.format(contentType, cat);
        String groupXml = Formats.format(contentType, group);
        //Create Category
        given()
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
            .statusCode(is(HttpServletResponse.SC_CREATED));
        //Create Group
        given()
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
            .statusCode(is(HttpServletResponse.SC_CREATED));
        TimeSeriesGroup newGroup = new TimeSeriesGroup(cat, officeId, "test_rename_group_new", "IntegrationTesting",
            "sharedTsAliasId2", timeSeriesId);
        String newGroupXml = Formats.format(contentType, newGroup);
        //Rename Group
        given()
            .accept(Formats.JSON)
            .contentType(Formats.JSON)
            .body(newGroupXml)
            .header("Authorization", user.toHeaderValue())
            .header(CATEGORY_ID, group.getTimeSeriesCategory().getId())
            .header(OFFICE, group.getOfficeId())
            .when()
            .redirects().follow(true)
            .redirects().max(3)
            .patch("/timeseries/group/"+ group.getId())
            .then()
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_ACCEPTED));
        //Read
        given()
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
            .log().body().log().everything(true)
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
            .accept(Formats.JSON)
            .contentType(Formats.JSON)
            .body(newGroupXml)
            .header("Authorization", user.toHeaderValue())
            .queryParam(CATEGORY_ID, newGroup.getTimeSeriesCategory().getId())
            .queryParam(REPLACE_ASSIGNED_TS, "true")
            .queryParam(OFFICE, newGroup.getOfficeId())
            .when()
            .redirects().follow(true)
            .redirects().max(3)
            .patch("/timeseries/group/"+ newGroup.getId())
            .then()
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_ACCEPTED));
        //Delete Group
        given()
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
            .log().body().log().everything(true)
            .statusCode(is(HttpServletResponse.SC_NO_CONTENT));
        //Delete Category
        given()
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
            .log().body().log().everything(true)
            .statusCode(is(HttpServletResponse.SC_NO_CONTENT));
    }

    @Test
    void test_add_assigned_locs() throws Exception {
        String officeId = "SPK";
        String timeSeriesId = "Alder Springs.Precip-Cumulative.Inst.15Minutes.0.raw-radar";
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;
        TimeSeriesCategory cat = new TimeSeriesCategory(officeId, "test_add_assigned_locs", "IntegrationTesting");
        TimeSeriesGroup group = new TimeSeriesGroup(cat, officeId, "test_add_assigned_locs", "IntegrationTesting",
            "sharedTsAliasId", timeSeriesId);
        List<AssignedTimeSeries> assignedTimeSeries = group.getAssignedTimeSeries();

        BigDecimal tsCode = getTsCode(officeId, timeSeriesId);
        assignedTimeSeries.add(new AssignedTimeSeries(timeSeriesId, tsCode, "AliasId", timeSeriesId, 1));
        ContentType contentType = Formats.parseHeaderAndQueryParm(Formats.JSON, null);
        String categoryXml = Formats.format(contentType, cat);
        String groupXml = Formats.format(contentType, group);
        //Create Category
        given()
            .accept(Formats.JSON)
            .contentType(Formats.JSON)
            .body(categoryXml)
            .header("Authorization", user.toHeaderValue())
            .queryParam(FAIL_IF_EXISTS, "false")
            .queryParam(OFFICE, officeId)
            .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/timeseries/category/")
            .then()
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED));
        //Create Group
        given()
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
            .statusCode(is(HttpServletResponse.SC_CREATED));
        assignedTimeSeries.clear();
        String timeSeriesId2 = "Pine Flat-Outflow.Stage.Inst.15Minutes.0.raw-radar";
        BigDecimal tsCode2 = getTsCode(officeId, timeSeriesId2);
        assignedTimeSeries.add(new AssignedTimeSeries(timeSeriesId2, tsCode2, "AliasId2", timeSeriesId2, 2));
        groupXml = Formats.format(contentType, group);
        //Add Assigned Locs
        given()
            .accept(Formats.JSON)
            .contentType(Formats.JSON)
            .body(groupXml)
            .header("Authorization", user.toHeaderValue())
            .queryParam(CATEGORY_ID, group.getTimeSeriesCategory().getId())
            .queryParam(REPLACE_ASSIGNED_LOCS, "true")
            .queryParam(OFFICE, group.getOfficeId())
            .when()
            .redirects().follow(true)
            .redirects().max(3)
            .patch("/timeseries/group/"+ group.getId())
            .then()
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_ACCEPTED));
        //Read
        given()
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
            .log().body().log().everything(true)
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
            .statusCode(is(HttpServletResponse.SC_ACCEPTED));
        //Delete Group
        given()
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
            .log().body().log().everything(true)
            .statusCode(is(HttpServletResponse.SC_NO_CONTENT));
        //Delete Category
        given()
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
            .log().body().log().everything(true)
            .statusCode(is(HttpServletResponse.SC_NO_CONTENT));
    }
}
