/*
 * MIT License
 *
 * Copyright (c) 2026 Hydrologic Engineering Center
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

import static cwms.cda.api.Controllers.CASCADE_DELETE;
import static cwms.cda.api.Controllers.CATEGORY_ID;
import static cwms.cda.api.Controllers.CATEGORY_OFFICE_ID;
import static cwms.cda.api.Controllers.FAIL_IF_EXISTS;
import static cwms.cda.api.Controllers.OFFICE;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;

import com.google.common.flogger.FluentLogger;
import cwms.cda.api.errors.NotFoundException;
import cwms.cda.data.dao.TimeSeriesCategoryDao;
import cwms.cda.data.dao.TimeSeriesGroupDao;
import cwms.cda.data.dto.AssignedTimeSeries;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.TimeSeriesCategory;
import cwms.cda.data.dto.timeseriesgroup.TimeSeriesGroup;
import cwms.cda.data.dto.timeseriesgroup.Membership;
import cwms.cda.data.dto.timeseriesgroup.TimeSeriesGroupPatch;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import fixtures.CwmsDataApiSetupCallback;
import fixtures.TestAccounts;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import javax.servlet.http.HttpServletResponse;
import mil.army.usace.hec.test.database.CwmsDatabaseContainer;
import org.jooq.Configuration;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for the v2 Timeseries Group controller ({@code /v2/timeseries/group}).
 * Create, delete, and retrieve are shared with v1 (via {@code AbstractTimeSeriesGroupController}),
 * so {@link #test_v2_create_read_delete()} exercises those the same way
 * {@link TimeSeriesGroupControllerV1TestIT} does for v1, just against the v2 routes. The remaining
 * tests focus on what's unique to v2: PATCH driven by a {@code membership} of time series ids to
 * assign/unassign, instead of a full list of assigned time series.
 */
@Tag("integration")
final class TimeSeriesGroupControllerV2TestIT extends DataApiTestIT {
    private static final FluentLogger LOGGER = FluentLogger.forEnclosingClass();
    private static final String V2_GROUP_PATH = "/v2/timeseries/group";

    private static final String LOCATION = "TsGroupV2Test";
    private static final String TS1 = LOCATION + ".Precip-Cumulative.Inst.15Minutes.0.raw-cda";
    private static final String TS2 = LOCATION + ".Precip-INC.Total.15Minutes.15Minutes.calc-cda";
    private static final String TS3 = LOCATION + ".Stage.Inst.15Minutes.0.raw-cda";

    private final List<TimeSeriesCategory> categoriesToCleanup = new ArrayList<>();
    private final List<TimeSeriesGroup> groupsToCleanup = new ArrayList<>();

    TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

    @BeforeAll
    static void load_data() throws Exception {
        createLocation(LOCATION, true, "SPK");
        createTimeseries("SPK", TS1);
        createTimeseries("SPK", TS2);
        createTimeseries("SPK", TS3);
    }

    @AfterEach
    void clear_data() throws Exception {
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection(c -> {
            Configuration configuration = DSL.using(c).configuration();
            TimeSeriesGroupDao groupDao = new TimeSeriesGroupDao(configuration.dsl());
            TimeSeriesCategoryDao categoryDao = new TimeSeriesCategoryDao(configuration.dsl());

            for (TimeSeriesGroup group : groupsToCleanup) {
                String assignOffice = group.getOfficeId();
                try {
                    groupDao.unassignForOffice(group.getTimeSeriesCategory().getId(), group.getId(),
                            group.getOfficeId(), assignOffice);
                } catch (NotFoundException e) {
                    LOGGER.atConfig().withCause(e).log("Group not found");
                } catch (DataAccessException e) {
                    LOGGER.atInfo().withCause(e).log("Failed to unassign time series in office %s", assignOffice);
                }

                try {
                    groupDao.delete(group.getTimeSeriesCategory().getId(), group.getId(), group.getOfficeId(), true);
                } catch (NotFoundException e) {
                    LOGGER.atConfig().withCause(e).log("Group not found");
                } catch (DataAccessException e) {
                    LOGGER.atInfo().withCause(e).log("Failed to delete group in office %s", group.getOfficeId());
                }
            }
            for (TimeSeriesCategory category : categoriesToCleanup) {
                try {
                    categoryDao.delete(category.getId(), true, category.getOfficeId());
                } catch (NotFoundException e) {
                    LOGGER.atConfig().withCause(e).log("Category not found");
                }
            }
            groupsToCleanup.clear();
            categoriesToCleanup.clear();
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    private TimeSeriesCategory createCategory(String officeId, String catId) throws Exception {
        TimeSeriesCategory cat = new TimeSeriesCategory(officeId, catId, "IntegrationTesting");
        categoriesToCleanup.add(cat);
        ContentType contentType = Formats.parseHeader(Formats.JSON, TimeSeriesCategory.class);
        String categoryJson = Formats.format(contentType, cat);
        given()
                .log().ifValidationFails()
                .accept(Formats.JSON)
                .contentType(Formats.JSON)
                .body(categoryJson)
                .header("Authorization", user.toHeaderValue())
                .queryParam(OFFICE, officeId)
                .queryParam(FAIL_IF_EXISTS, false)
            .when()
                .post("/timeseries/category")
            .then()
                .log().ifValidationFails()
            .assertThat()
                .statusCode(is(HttpServletResponse.SC_CREATED));
        return cat;
    }

    private void createGroup(TimeSeriesGroup group) {
        ContentType contentType = Formats.parseHeader(Formats.JSON, TimeSeriesGroup.class);
        String groupJson = Formats.format(contentType, group);
        given()
                .log().ifValidationFails()
                .accept(Formats.JSON)
                .contentType(Formats.JSON)
                .body(groupJson)
                .header("Authorization", user.toHeaderValue())
                .queryParam(FAIL_IF_EXISTS, false)
            .when()
                .post(V2_GROUP_PATH + "/" + group.getOfficeId())
            .then()
                .log().ifValidationFails()
            .assertThat()
                .statusCode(is(HttpServletResponse.SC_CREATED));
    }

    private String patchBody(TimeSeriesGroupPatch patch) {
        ContentType contentType = Formats.parseHeader(Formats.JSON, TimeSeriesGroupPatch.class);
        return Formats.format(contentType, patch);
    }

    @Test
    void test_v2_create_read_delete() throws Exception {
        String officeId = user.getOperatingOffice();
        TimeSeriesCategory cat = createCategory(officeId, "test_v2_create_read_delete");
        TimeSeriesGroup group = new TimeSeriesGroup(cat, officeId, "test_v2_create_read_delete",
                "IntegrationTesting", "sharedTsAliasId", TS1);
        group.getAssignedTimeSeries().add(new AssignedTimeSeries(officeId, TS1, "AliasId", TS1, 1));
        groupsToCleanup.add(group);

        createGroup(group);

        given()
                .log().ifValidationFails()
                .accept(Formats.JSON)
                .queryParam(OFFICE, officeId)
                .queryParam(CATEGORY_OFFICE_ID, officeId)
                .queryParam(CATEGORY_ID, cat.getId())
            .when()
                .get(V2_GROUP_PATH + "/" + officeId + "/" + group.getId())
            .then()
                .log().ifValidationFails()
            .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
                .body("office-id", equalTo(group.getOfficeId()))
                .body("id", equalTo(group.getId()))
                .body("assigned-time-series[0].timeseries-id", equalTo(TS1));

        given()
                .log().ifValidationFails()
                .accept(Formats.JSON)
                .header("Authorization", user.toHeaderValue())
                .queryParam(CATEGORY_ID, cat.getId())
                .queryParam(CASCADE_DELETE, "true")
            .when()
                .delete(V2_GROUP_PATH + "/" + officeId + "/" + group.getId())
            .then()
                .log().ifValidationFails()
            .assertThat()
                .statusCode(is(HttpServletResponse.SC_NO_CONTENT));

        given()
                .log().ifValidationFails()
                .accept(Formats.JSON)
                .queryParam(OFFICE, officeId)
                .queryParam(CATEGORY_OFFICE_ID, officeId)
            .when()
                .get(V2_GROUP_PATH + "/" + officeId + "/" + group.getId())
            .then()
                .log().ifValidationFails()
            .assertThat()
                .statusCode(is(HttpServletResponse.SC_NOT_FOUND));
    }

    @Test
    void test_v2_patch_assign_time_series() throws Exception {
        String officeId = user.getOperatingOffice();
        TimeSeriesCategory cat = createCategory(officeId, "test_v2_patch_assign");
        TimeSeriesGroup group = new TimeSeriesGroup(cat, officeId, "test_v2_patch_assign",
                "IntegrationTesting", "sharedTsAliasId", TS1);
        group.getAssignedTimeSeries().add(new AssignedTimeSeries(officeId, TS1, "AliasId", TS1, 1));
        groupsToCleanup.add(group);
        createGroup(group);

        Membership membership = new Membership.Builder()
                .withAssign(Arrays.asList(
                        new AssignedTimeSeries(officeId, TS2, "AliasId2", TS2, 2),
                        new AssignedTimeSeries(officeId, TS3, "AliasId3", TS3, 3)))
                .withUnassign(Collections.emptyList())
                .build();
        TimeSeriesGroupPatch patch = new TimeSeriesGroupPatch.Builder()
                .withOfficeId(officeId)
                .withId(group.getId())
                .withTimeSeriesCategory(cat)
                .withDescription(group.getDescription())
                .withSharedAliasId(group.getSharedAliasId())
                .withSharedRefTsId(group.getSharedRefTsId())
                .withMembership(membership)
                .build();

        given()
                .log().ifValidationFails()
                .accept(Formats.JSON)
                .contentType(Formats.JSON)
                .body(patchBody(patch))
                .header("Authorization", user.toHeaderValue())
            .when()
                .patch(V2_GROUP_PATH + "/" + officeId + "/" + group.getId())
            .then()
                .log().ifValidationFails()
            .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK));

        given()
                .log().ifValidationFails()
                .accept(Formats.JSON)
                .queryParam(OFFICE, officeId)
                .queryParam(CATEGORY_OFFICE_ID, officeId)
                .queryParam(CATEGORY_ID, cat.getId())
            .when()
                .get(V2_GROUP_PATH + "/" + officeId + "/" + group.getId())
            .then()
                .log().ifValidationFails()
            .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
                .body("assigned-time-series.size()", is(3))
                .body("assigned-time-series.timeseries-id", hasItem(TS1))
                .body("assigned-time-series.timeseries-id", hasItem(TS2))
                .body("assigned-time-series.timeseries-id", hasItem(TS3))
                .body("assigned-time-series.alias-id", hasItem("AliasId2"))
                .body("assigned-time-series.alias-id", hasItem("AliasId3"));
    }

    @Test
    void test_v2_patch_unassign_time_series() throws Exception {
        String officeId = user.getOperatingOffice();
        TimeSeriesCategory cat = createCategory(officeId, "test_v2_patch_unassign");
        TimeSeriesGroup group = new TimeSeriesGroup(cat, officeId, "test_v2_patch_unassign",
                "IntegrationTesting", "sharedTsAliasId", TS1);
        List<AssignedTimeSeries> assigned = group.getAssignedTimeSeries();
        assigned.add(new AssignedTimeSeries(officeId, TS1, "AliasId1", TS1, 1));
        assigned.add(new AssignedTimeSeries(officeId, TS2, "AliasId2", TS2, 2));
        assigned.add(new AssignedTimeSeries(officeId, TS3, "AliasId3", TS3, 3));
        groupsToCleanup.add(group);
        createGroup(group);

        // Only specify the ids to unassign - not the full set of assigned time series.
        Membership membership = new Membership.Builder()
                .withAssign(Collections.emptyList())
                .withUnassign(Arrays.asList(CwmsId.buildCwmsId(officeId, TS1), CwmsId.buildCwmsId(officeId, TS2)))
                .build();
        TimeSeriesGroupPatch patch = new TimeSeriesGroupPatch.Builder()
                .withOfficeId(officeId)
                .withId(group.getId())
                .withTimeSeriesCategory(cat)
                .withDescription(group.getDescription())
                .withSharedAliasId(group.getSharedAliasId())
                .withSharedRefTsId(group.getSharedRefTsId())
                .withMembership(membership)
                .build();

        given()
                .log().ifValidationFails()
                .accept(Formats.JSON)
                .contentType(Formats.JSON)
                .body(patchBody(patch))
                .header("Authorization", user.toHeaderValue())
            .when()
                .patch(V2_GROUP_PATH + "/" + officeId + "/" + group.getId())
            .then()
                .log().ifValidationFails()
            .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK));

        given()
                .log().ifValidationFails()
                .accept(Formats.JSON)
                .queryParam(OFFICE, officeId)
                .queryParam(CATEGORY_OFFICE_ID, officeId)
                .queryParam(CATEGORY_ID, cat.getId())
            .when()
                .get(V2_GROUP_PATH + "/" + officeId + "/" + group.getId())
            .then()
                .log().ifValidationFails()
            .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
                .body("id", equalTo(group.getId()))
                .body("assigned-time-series.size()", is(1))
                .body("assigned-time-series[0].timeseries-id", equalTo(TS3));
    }

    @Test
    void test_v2_patch_assign_and_unassign_together() throws Exception {
        String officeId = user.getOperatingOffice();
        TimeSeriesCategory cat = createCategory(officeId, "test_v2_patch_combo");
        TimeSeriesGroup group = new TimeSeriesGroup(cat, officeId, "test_v2_patch_combo",
                "IntegrationTesting", "sharedTsAliasId", TS1);
        List<AssignedTimeSeries> assigned = group.getAssignedTimeSeries();
        assigned.add(new AssignedTimeSeries(officeId, TS1, "AliasId1", TS1, 1));
        assigned.add(new AssignedTimeSeries(officeId, TS2, "AliasId2", TS2, 2));
        groupsToCleanup.add(group);
        createGroup(group);

        // Unassign TS1 while assigning TS3, in a single request.
        Membership membership = new Membership.Builder()
                .withAssign(Collections.singletonList(new AssignedTimeSeries(officeId, TS3, "AliasId3", TS3, 3)))
                .withUnassign(Collections.singletonList(CwmsId.buildCwmsId(officeId, TS1)))
                .build();
        TimeSeriesGroupPatch patch = new TimeSeriesGroupPatch.Builder()
                .withOfficeId(officeId)
                .withId(group.getId())
                .withTimeSeriesCategory(cat)
                .withDescription(group.getDescription())
                .withSharedAliasId(group.getSharedAliasId())
                .withSharedRefTsId(group.getSharedRefTsId())
                .withMembership(membership)
                .build();

        given()
                .log().ifValidationFails()
                .accept(Formats.JSON)
                .contentType(Formats.JSON)
                .body(patchBody(patch))
                .header("Authorization", user.toHeaderValue())
            .when()
                .patch(V2_GROUP_PATH + "/" + officeId + "/" + group.getId())
            .then()
                .log().ifValidationFails()
            .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK));

        given()
                .log().ifValidationFails()
                .accept(Formats.JSON)
                .queryParam(OFFICE, officeId)
                .queryParam(CATEGORY_OFFICE_ID, officeId)
                .queryParam(CATEGORY_ID, cat.getId())
            .when()
                .get(V2_GROUP_PATH + "/" + officeId + "/" + group.getId())
            .then()
                .log().ifValidationFails()
            .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
                .body("assigned-time-series.size()", is(2))
                .body("assigned-time-series.timeseries-id", hasItem(TS2))
                .body("assigned-time-series.timeseries-id", hasItem(TS3));
    }

    @Test
    void test_v2_patch_rejects_ts_in_both_assign_and_unassign() throws Exception {
        String officeId = user.getOperatingOffice();
        TimeSeriesCategory cat = createCategory(officeId, "test_v2_patch_overlap");
        TimeSeriesGroup group = new TimeSeriesGroup(cat, officeId, "test_v2_patch_overlap",
                "IntegrationTesting", "sharedTsAliasId", TS1);
        group.getAssignedTimeSeries().add(new AssignedTimeSeries(officeId, TS1, "AliasId", TS1, 1));
        groupsToCleanup.add(group);
        createGroup(group);

        // TS1 appears in both the assign and unassign lists - this should be rejected outright.
        Membership membership = new Membership.Builder()
                .withAssign(Collections.singletonList(new AssignedTimeSeries(officeId, TS1, "AliasId", TS1, 1)))
                .withUnassign(Collections.singletonList(CwmsId.buildCwmsId(officeId, TS1)))
                .build();
        TimeSeriesGroupPatch patch = new TimeSeriesGroupPatch.Builder()
                .withOfficeId(officeId)
                .withId(group.getId())
                .withTimeSeriesCategory(cat)
                .withDescription(group.getDescription())
                .withSharedAliasId(group.getSharedAliasId())
                .withSharedRefTsId(group.getSharedRefTsId())
                .withMembership(membership)
                .build();

        given()
                .log().ifValidationFails()
                .accept(Formats.JSON)
                .contentType(Formats.JSON)
                .body(patchBody(patch))
                .header("Authorization", user.toHeaderValue())
            .when()
                .patch(V2_GROUP_PATH + "/" + officeId + "/" + group.getId())
            .then()
                .log().ifValidationFails()
            .assertThat()
                .statusCode(is(HttpServletResponse.SC_BAD_REQUEST));

        // Confirm nothing changed, since the invalid request should not have been processed.
        given()
                .log().ifValidationFails()
                .accept(Formats.JSON)
                .queryParam(OFFICE, officeId)
                .queryParam(CATEGORY_OFFICE_ID, officeId)
                .queryParam(CATEGORY_ID, cat.getId())
            .when()
                .get(V2_GROUP_PATH + "/" + officeId + "/" + group.getId())
            .then()
                .log().ifValidationFails()
            .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
                .body("assigned-time-series.size()", is(1))
                .body("assigned-time-series[0].timeseries-id", equalTo(TS1));
    }

    @Test
    void test_v2_patch_rename_and_description() throws Exception {
        String officeId = user.getOperatingOffice();
        TimeSeriesCategory cat = createCategory(officeId, "test_v2_patch_rename");
        TimeSeriesGroup group = new TimeSeriesGroup(cat, officeId, "test_v2_patch_rename_orig",
                "Original description", "sharedTsAliasId", TS1);
        group.getAssignedTimeSeries().add(new AssignedTimeSeries(officeId, TS1, "AliasId", TS1, 1));
        createGroup(group);

        String newGroupId = "test_v2_patch_rename_new";
        TimeSeriesGroup renamedGroupForCleanup = new TimeSeriesGroup(cat, officeId, newGroupId,
                "Updated description", "sharedTsAliasId", TS1);
        groupsToCleanup.add(renamedGroupForCleanup);

        TimeSeriesGroupPatch patch = new TimeSeriesGroupPatch.Builder()
                .withOfficeId(officeId)
                .withId(newGroupId)
                .withTimeSeriesCategory(cat)
                .withDescription("Updated description")
                .withSharedAliasId(group.getSharedAliasId())
                .withSharedRefTsId(group.getSharedRefTsId())
                .build();

        given()
                .log().ifValidationFails()
                .accept(Formats.JSON)
                .contentType(Formats.JSON)
                .body(patchBody(patch))
                .header("Authorization", user.toHeaderValue())
            .when()
                .patch(V2_GROUP_PATH + "/" + officeId + "/" + group.getId())
            .then()
                .log().ifValidationFails()
            .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK));

        given()
                .log().ifValidationFails()
                .accept(Formats.JSON)
                .queryParam(OFFICE, officeId)
                .queryParam(CATEGORY_OFFICE_ID, officeId)
                .queryParam(CATEGORY_ID, cat.getId())
            .when()
                .get(V2_GROUP_PATH + "/" + officeId + "/" + newGroupId)
            .then()
                .log().ifValidationFails()
            .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
                .body("id", equalTo(newGroupId))
                .body("description", equalTo("Updated description"))
                .body("assigned-time-series[0].timeseries-id", equalTo(TS1));

        given()
                .log().ifValidationFails()
                .accept(Formats.JSON)
                .queryParam(OFFICE, officeId)
                .queryParam(CATEGORY_OFFICE_ID, officeId)
            .when()
                .get(V2_GROUP_PATH + "/" + officeId + "/" + group.getId())
            .then()
                .log().ifValidationFails()
            .assertThat()
                .statusCode(is(HttpServletResponse.SC_NOT_FOUND));
    }

    @Test
    void test_v2_patch_requires_matching_office() throws Exception {
        // v2 primary-resource standard: office is a required path segment. This test verifies
        // that the office in the path must match the office embedded in the request body.
        String officeId = user.getOperatingOffice();
        TimeSeriesCategory cat = createCategory(officeId, "test_v2_patch_requires_office");
        TimeSeriesGroup group = new TimeSeriesGroup(cat, officeId, "test_v2_patch_requires_office",
                "IntegrationTesting", "sharedTsAliasId", TS1);
        group.getAssignedTimeSeries().add(new AssignedTimeSeries(officeId, TS1, "AliasId", TS1, 1));
        groupsToCleanup.add(group);
        createGroup(group);

        Membership membership = new Membership.Builder()
                .withAssign(Collections.emptyList())
                .withUnassign(Collections.singletonList(CwmsId.buildCwmsId(officeId, TS1)))
                .build();
        TimeSeriesGroupPatch patch = new TimeSeriesGroupPatch.Builder()
                .withOfficeId(officeId)
                .withId(group.getId())
                .withTimeSeriesCategory(cat)
                .withDescription(group.getDescription())
                .withSharedAliasId(group.getSharedAliasId())
                .withSharedRefTsId(group.getSharedRefTsId())
                .withMembership(membership)
                .build();

        // Path office ("WRONG_OFFICE") does not match the body's office (officeId).
        given()
                .log().ifValidationFails()
                .accept(Formats.JSON)
                .contentType(Formats.JSON)
                .body(patchBody(patch))
                .header("Authorization", user.toHeaderValue())
            .when()
                .patch(V2_GROUP_PATH + "/WRONG_OFFICE/" + group.getId())
            .then()
                .log().ifValidationFails()
            .assertThat()
                .statusCode(is(HttpServletResponse.SC_BAD_REQUEST))
                .body("message", equalTo("Bad Request"));

        // Confirm the time series is still assigned, since the mismatched request should not
        // have been processed.
        given()
                .log().ifValidationFails()
                .accept(Formats.JSON)
                .queryParam(OFFICE, officeId)
                .queryParam(CATEGORY_OFFICE_ID, officeId)
                .queryParam(CATEGORY_ID, cat.getId())
            .when()
                .get(V2_GROUP_PATH + "/" + officeId + "/" + group.getId())
            .then()
                .log().ifValidationFails()
            .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
                .body("assigned-time-series[0].timeseries-id", equalTo(TS1));
    }

    @Test
    void test_v2_patch_only_supports_plain_json() throws Exception {
        String officeId = user.getOperatingOffice();
        TimeSeriesCategory cat = createCategory(officeId, "test_v2_patch_json_only");
        TimeSeriesGroup group = new TimeSeriesGroup(cat, officeId, "test_v2_patch_json_only",
                "IntegrationTesting", "sharedTsAliasId", TS1);
        group.getAssignedTimeSeries().add(new AssignedTimeSeries(officeId, TS1, "AliasId", TS1, 1));
        groupsToCleanup.add(group);
        createGroup(group);

        Membership membership = new Membership.Builder()
                .withAssign(Collections.emptyList())
                .withUnassign(Collections.singletonList(CwmsId.buildCwmsId(officeId, TS1)))
                .build();
        TimeSeriesGroupPatch patch = new TimeSeriesGroupPatch.Builder()
                .withOfficeId(officeId)
                .withId(group.getId())
                .withTimeSeriesCategory(cat)
                .withDescription(group.getDescription())
                .withSharedAliasId(group.getSharedAliasId())
                .withSharedRefTsId(group.getSharedRefTsId())
                .withMembership(membership)
                .build();

        // The v2 patch DTO only supports plain JSON - the versioned JSONV1 content type should
        // not be resolvable for it.
        given()
                .log().ifValidationFails()
                .accept(Formats.JSON)
                .contentType(Formats.JSONV1)
                .body(patchBody(patch))
                .header("Authorization", user.toHeaderValue())
            .when()
                .patch(V2_GROUP_PATH + "/" + officeId + "/" + group.getId())
            .then()
                .log().ifValidationFails()
            .assertThat()
                .statusCode(is(HttpServletResponse.SC_NOT_ACCEPTABLE));

        // Confirm the time series is still assigned, since the malformed request should not
        // have been processed.
        given()
                .log().ifValidationFails()
                .accept(Formats.JSON)
                .queryParam(OFFICE, officeId)
                .queryParam(CATEGORY_OFFICE_ID, officeId)
                .queryParam(CATEGORY_ID, cat.getId())
            .when()
                .get(V2_GROUP_PATH + "/" + officeId + "/" + group.getId())
            .then()
                .log().ifValidationFails()
            .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
                .body("assigned-time-series[0].timeseries-id", equalTo(TS1));
    }
}
