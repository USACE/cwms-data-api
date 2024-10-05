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

import cwms.cda.api.errors.NotFoundException;
import cwms.cda.data.dao.LocationCategoryDao;
import cwms.cda.data.dao.LocationGroupDao;
import fixtures.CwmsDataApiSetupCallback;
import fixtures.TestAccounts;
import io.restassured.filter.log.LogDetail;

import mil.army.usace.hec.test.database.CwmsDatabaseContainer;
import org.jooq.Configuration;
import org.jooq.impl.DSL;
import org.jooq.util.oracle.OracleDSL;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import cwms.cda.data.dto.AssignedLocation;
import cwms.cda.data.dto.LocationCategory;
import cwms.cda.data.dto.LocationGroup;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import usace.cwms.db.jooq.codegen.packages.CWMS_ENV_PACKAGE;

import javax.servlet.http.HttpServletResponse;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import static cwms.cda.api.Controllers.*;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.*;

@Tag("integration")
class LocationGroupControllerTestIT extends DataApiTestIT {
    private static final Logger LOGGER = Logger.getLogger(LocationGroupControllerTestIT.class.getName());
    private List<LocationGroup> groupsToCleanup = new ArrayList<>();
    private List<LocationCategory> categoriesToCleanup = new ArrayList<>();
    TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

    @AfterEach
    void tearDown() throws Exception {
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection(c -> {

            Configuration configuration = OracleDSL.using(c).configuration();
            CWMS_ENV_PACKAGE.call_SET_SESSION_OFFICE_ID(DSL.using(c).configuration(), "SPK");
            LocationCategoryDao locationCategoryDao = new LocationCategoryDao(DSL.using(configuration));
            LocationGroupDao locationGroupDao = new LocationGroupDao(DSL.using(configuration));
            for (LocationGroup group : groupsToCleanup) {
                try {
                    locationGroupDao.unassignAllLocs(group, "SPK");
                    if (!group.getOfficeId().equalsIgnoreCase(CWMS_OFFICE) || !group.getId().equalsIgnoreCase("Default")) {
                        locationGroupDao.delete(group.getLocationCategory().getId(), group.getId(), true, group.getOfficeId());
                    }
                } catch (NotFoundException e) {
                    LOGGER.log(Level.CONFIG, String.format("Failed to delete location group: %s", group.getId()), e);
                }
            }
            for (LocationCategory category : categoriesToCleanup) {
                try {
                    locationCategoryDao.delete(category.getId(), true, category.getOfficeId());
                } catch (NotFoundException e) {
                    LOGGER.log(Level.CONFIG, String.format("Failed to delete location category: %s", category.getId()), e);
                }
            }
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    @Test
    void test_getall() throws Exception {
        String locationId = "LocationGroupTest";
        String officeId = user.getOperatingOffice();
        createLocation(locationId, true, officeId);
        AssignedLocation assignLoc = new AssignedLocation(locationId, officeId, "AliasId", 1, locationId);
        LocationCategory cat = new LocationCategory(officeId, "TestCategory", "IntegrationTesting");
        LocationGroup group = new LocationGroup(new LocationGroup(cat, officeId, LocationGroupControllerTestIT.class.getSimpleName(), "IntegrationTesting",
                "sharedLocAliasId", locationId, 123), Collections.singletonList(assignLoc));
        ContentType contentType = Formats.parseHeader(Formats.JSON, LocationCategory.class);
        String categoryXml = Formats.format(contentType, cat);
        String groupXml = Formats.format(contentType, group);
        groupsToCleanup.add(group);
        categoriesToCleanup.add(cat);
        //Create Category
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSON)
            .contentType(Formats.JSON)
            .body(categoryXml)
            .header("Authorization", user.toHeaderValue())
            .queryParam(OFFICE, officeId)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/location/category")
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
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/location/group")
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
            .queryParam(INCLUDE_ASSIGNED, false)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/location/group")
        .then()
            .assertThat()
            .log().ifValidationFails(LogDetail.ALL,true)
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("[0].office-id", notNullValue())
            .body("[0].id", notNullValue());
        //Delete Category
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSON)
            .contentType(Formats.JSON)
            .header("Authorization", user.toHeaderValue())
            .queryParam(OFFICE, officeId)
            .queryParam(CASCADE_DELETE, "true")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("/location/category/" + group.getLocationCategory().getId())
        .then()
            .assertThat()
            .log().ifValidationFails(LogDetail.ALL,true)
            .statusCode(is(HttpServletResponse.SC_NO_CONTENT));
    }

    @Test
    void test_create_read_delete() throws Exception {
        String officeId = user.getOperatingOffice();
        String locationId = "LocationGroupTest";
        createLocation(locationId, true, officeId);
        LocationCategory cat = new LocationCategory(officeId, "TestCategory2", "IntegrationTesting");
        AssignedLocation assignLoc = new AssignedLocation(locationId, officeId, "AliasId", 1, locationId);
        LocationGroup group = new LocationGroup(new LocationGroup(cat, officeId, LocationGroupControllerTestIT.class.getSimpleName(), "IntegrationTesting",
            "sharedLocAliasId", locationId, 123), Collections.singletonList(assignLoc));
        ContentType contentType = Formats.parseHeader(Formats.JSON, LocationCategory.class);
        String categoryXml = Formats.format(contentType, cat);
        String groupXml = Formats.format(contentType, group);
        groupsToCleanup.add(group);
        categoriesToCleanup.add(cat);
        //Create Category
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSON)
            .contentType(Formats.JSON)
            .body(categoryXml)
            .header("Authorization", user.toHeaderValue())
            .queryParam(OFFICE, officeId)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/location/category")
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
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/location/group")
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
            .queryParam(CATEGORY_ID, group.getLocationCategory().getId())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/location/group/" + group.getId())
        .then()
            .assertThat()
            .log().ifValidationFails(LogDetail.ALL,true)
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("office-id", equalTo(group.getOfficeId()))
            .body("id", equalTo(group.getId()))
            .body("description", equalTo(group.getDescription()))
            .body("assigned-locations[0].location-id", equalTo(locationId))
            .body("assigned-locations[0].alias-id", equalTo("AliasId"))
            .body("assigned-locations[0].ref-location-id", equalTo(locationId));
        //Delete Group
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSON)
            .contentType(Formats.JSON)
            .header("Authorization", user.toHeaderValue())
            .queryParam(OFFICE, officeId)
            .queryParam(CATEGORY_ID, cat.getId())
            .queryParam(CASCADE_DELETE, "true")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("/location/group/" + group.getId())
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
            .queryParam(CATEGORY_ID, group.getLocationCategory().getId())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/location/group/" + group.getId())
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
            .queryParam(CASCADE_DELETE, "true")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("/location/category/" + group.getLocationCategory().getId())
        .then()
            .assertThat()
            .log().ifValidationFails(LogDetail.ALL,true)
            .statusCode(is(HttpServletResponse.SC_NO_CONTENT));
    }

    @Test
    void test_rename_group() throws Exception {
        String officeId = user.getOperatingOffice();
        String locationId = "LocationGroupTest";
        createLocation(locationId, true, officeId);
        AssignedLocation assignLoc = new AssignedLocation(locationId, officeId, "AliasId", 1, locationId);
        LocationCategory cat = new LocationCategory(officeId, "test_rename_group", "IntegrationTesting");
        LocationGroup group = new LocationGroup(new LocationGroup(cat, officeId, "test_rename_group", "IntegrationTesting",
            "sharedLocAliasId", locationId, 123), Collections.singletonList(assignLoc));
        groupsToCleanup.add(group);
        categoriesToCleanup.add(cat);
        ContentType contentType = Formats.parseHeader(Formats.JSON, LocationCategory.class);
        String categoryXml = Formats.format(contentType, cat);
        String groupXml = Formats.format(contentType, group);
        //Create Category
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSON)
            .contentType(Formats.JSON)
            .body(categoryXml)
            .header("Authorization", user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/location/category/")
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
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/location/group")
        .then()
            .assertThat()
            .log().ifValidationFails(LogDetail.ALL,true)
            .statusCode(is(HttpServletResponse.SC_CREATED));
        LocationGroup newGroup = new LocationGroup(cat, officeId, "test_rename_group_new", "IntegrationTesting",
            "sharedLocAliasId", locationId, 123);
        groupsToCleanup.add(newGroup);
        String newGroupXml = Formats.format(contentType, newGroup);
        //Rename Group
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSON)
            .contentType(Formats.JSON)
            .body(newGroupXml)
            .header("Authorization", user.toHeaderValue())
            .header(CATEGORY_ID, group.getLocationCategory().getId())
            .queryParam(OFFICE, group.getOfficeId())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .patch("/location/group/"+ group.getId())
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
            .queryParam(CATEGORY_ID, group.getLocationCategory().getId())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/location/group/" + newGroup.getId())
        .then()
            .assertThat()
            .log().ifValidationFails(LogDetail.ALL,true)
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("office-id", equalTo(newGroup.getOfficeId()))
            .body("id", equalTo(newGroup.getId()))
            .body("description", equalTo(newGroup.getDescription()))
            .body("assigned-locations[0].location-id", equalTo(locationId))
            .body("assigned-locations[0].alias-id", equalTo("AliasId"))
            .body("assigned-locations[0].ref-location-id", equalTo(locationId));
        //Delete Category
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSON)
            .contentType(Formats.JSON)
            .header("Authorization", user.toHeaderValue())
            .queryParam(OFFICE, officeId)
            .queryParam(CASCADE_DELETE, "true")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("/location/category/" + group.getLocationCategory().getId())
        .then()
            .assertThat()
            .log().ifValidationFails(LogDetail.ALL,true)
            .statusCode(is(HttpServletResponse.SC_NO_CONTENT));
    }

    @Test
    void test_add_assigned_locs() throws Exception {
        String officeId = user.getOperatingOffice();
        String locationId = "LocationGroupTest";
        createLocation(locationId, true, officeId);
        String locationId2 = "LocationGroupTest2";
        createLocation(locationId2, true, officeId);
        AssignedLocation assignLoc = new AssignedLocation(locationId, officeId, "AliasId", 1, locationId);
        LocationCategory cat = new LocationCategory(officeId, "test_add_assigned_locs", "IntegrationTesting");
        LocationGroup group = new LocationGroup(new LocationGroup(cat, officeId, "test_add_assigned_locs", "IntegrationTesting",
            "sharedLocAliasId", locationId, 123), Collections.singletonList(assignLoc));
        List<AssignedLocation> assignedLocations = group.getAssignedLocations();
        ContentType contentType = Formats.parseHeader(Formats.JSON, LocationCategory.class);
        String categoryXml = Formats.format(contentType, cat);
        String groupXml = Formats.format(contentType, group);
        groupsToCleanup.add(group);
        categoriesToCleanup.add(cat);
        //Create Category
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSON)
            .contentType(Formats.JSON)
            .body(categoryXml)
            .header("Authorization", user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/location/category/")
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
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/location/group")
        .then()
            .assertThat()
            .log().ifValidationFails(LogDetail.ALL,true)
            .statusCode(is(HttpServletResponse.SC_CREATED));
        assignedLocations.clear();
        assignedLocations.add(new AssignedLocation(locationId2, officeId, "AliasId2", 2, locationId2));
        LocationGroup newGroup = new LocationGroup(group, assignedLocations);
        groupsToCleanup.add(newGroup);
        String newGroupJson = Formats.format(contentType, newGroup);
        //Add Assigned Locs
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSON)
            .contentType(Formats.JSON)
            .body(newGroupJson)
            .header("Authorization", user.toHeaderValue())
            .queryParam(CATEGORY_ID, group.getLocationCategory().getId())
            .queryParam(REPLACE_ASSIGNED_LOCS, "true")
            .queryParam(OFFICE, group.getOfficeId())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .patch("/location/group/"+ group.getId())
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
            .queryParam(CATEGORY_ID, group.getLocationCategory().getId())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/location/group/" + group.getId())
        .then()
            .assertThat()
            .log().ifValidationFails(LogDetail.ALL,true)
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("office-id", equalTo(group.getOfficeId()))
            .body("id", equalTo(group.getId()))
            .body("description", equalTo(group.getDescription()))
            .body("assigned-locations[0].location-id", equalTo(locationId2))
            .body("assigned-locations[0].alias-id", equalTo("AliasId2"))
            .body("assigned-locations[0].ref-location-id", equalTo(locationId2));
        //Delete Category
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSON)
            .contentType(Formats.JSON)
            .header("Authorization", user.toHeaderValue())
            .queryParam(OFFICE, officeId)
            .queryParam(CASCADE_DELETE, true)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("/location/category/" + group.getLocationCategory().getId())
        .then()
            .assertThat()
            .log().ifValidationFails(LogDetail.ALL,true)
            .statusCode(is(HttpServletResponse.SC_NO_CONTENT));
    }

    @Test
    void test_district_permissions() throws Exception {
        String officeId = user.getOperatingOffice();
        String locationId = "LocationGroupTest";
        createLocation(locationId, true, officeId);

        LocationCategory cat = new LocationCategory(officeId, "test_CWMS_permissions", "Loc Group Integration Testing");

        AssignedLocation assignLoc = new AssignedLocation(locationId, officeId, null, null, null);
        LocationGroup group = new LocationGroup(cat, officeId, "test_CWMS_permissions", "Loc Group Integration Testing",
            null, locationId, null);
        LocationGroup newLocGroup = new LocationGroup(new LocationGroup(cat, officeId, "test_new_CWMS_permissions", "Second Loc Group IT",
            null, locationId, null), Collections.singletonList(assignLoc));
        groupsToCleanup.add(group);
        categoriesToCleanup.add(cat);
        groupsToCleanup.add(newLocGroup);

        String catJson = Formats.format(new ContentType(Formats.JSON), cat);
        String groupJson = Formats.format(new ContentType(Formats.JSON), group);
        String newGroupJson = Formats.format(new ContentType(Formats.JSON), newLocGroup);

        // create a location category
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSON)
            .contentType(Formats.JSON)
            .body(catJson)
            .header("Authorization", user.toHeaderValue())
            .queryParam(FAIL_IF_EXISTS, false)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/location/category/")
        .then()
            .assertThat()
            .log().ifValidationFails(LogDetail.ALL,true)
            .statusCode(is(HttpServletResponse.SC_CREATED));

        // create a location group
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSON)
            .contentType(Formats.JSON)
            .body(groupJson)
            .header("Authorization", user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/location/group/")
        .then()
            .assertThat()
            .log().ifValidationFails(LogDetail.ALL,true)
            .statusCode(is(HttpServletResponse.SC_CREATED));

        // patch the location group owned by district office
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSON)
            .contentType(Formats.JSON)
            .body(newGroupJson)
            .header("Authorization", user.toHeaderValue())
            .queryParam(FAIL_IF_EXISTS, false)
            .queryParam(OFFICE, officeId)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .patch("/location/group/" + group.getId())
        .then()
            .assertThat()
            .log().ifValidationFails(LogDetail.ALL,true)
            .statusCode(is(HttpServletResponse.SC_OK));

        // get the location group and assert that changes were made
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSON)
            .contentType(Formats.JSON)
            .queryParam(OFFICE, officeId)
            .queryParam(CATEGORY_ID, newLocGroup.getLocationCategory().getId())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/location/group/" + newLocGroup.getId())
        .then()
            .assertThat()
            .log().ifValidationFails(LogDetail.ALL,true)
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("office-id", equalTo(newLocGroup.getOfficeId()))
            .body("id", equalTo(newLocGroup.getId()))
            .body("description", equalTo(newLocGroup.getDescription()))
            .body("assigned-locations[0].location-id", equalTo(locationId))
            .body("assigned-locations[0].alias-id", nullValue())
            .body("assigned-locations[0].ref-location-id", nullValue());
    }

    @Test
    void test_CWMS_permissions() throws Exception {
        String officeId = user.getOperatingOffice();
        String locationId = "LocationGroupTest";
        createLocation(locationId, true, officeId);

        LocationCategory cat = new LocationCategory(CWMS_OFFICE, "Default", "Default");

        AssignedLocation assignLoc = new AssignedLocation(locationId, officeId, null, null,  null);
        LocationGroup group = new LocationGroup(cat, CWMS_OFFICE, "Default", "All Locations",
                null, null, null);
        LocationGroup newLocGroup = new LocationGroup(group, Collections.singletonList(assignLoc));

        groupsToCleanup.add(group);

        String newGroupJson = Formats.format(new ContentType(Formats.JSON), newLocGroup);
        String groupJson = Formats.format(new ContentType(Formats.JSON), group);

        // get the location group and assert that changes were made
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSON)
            .contentType(Formats.JSON)
            .queryParam(OFFICE, CWMS_OFFICE)
            .queryParam(CATEGORY_ID, cat.getId())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/location/group/" + group.getId())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("assigned-locations.size()", equalTo(0));

        // patch the location group owned by CWMS office
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSON)
            .contentType(Formats.JSON)
            .body(newGroupJson)
            .header("Authorization", user.toHeaderValue())
            .queryParam(REPLACE_ASSIGNED_LOCS, true)
            .queryParam(OFFICE, officeId)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .patch("/location/group/" + newLocGroup.getId())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK));

        // get the location group and assert that changes were made
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSON)
            .contentType(Formats.JSON)
            .queryParam(OFFICE, CWMS_OFFICE)
            .queryParam(CATEGORY_ID, cat.getId())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/location/group/" + newLocGroup.getId())
        .then()
                .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("office-id", equalTo(group.getOfficeId()))
            .body("id", equalTo(group.getId()))
            .body("description", equalTo(newLocGroup.getDescription()))
            .body("assigned-locations[0].location-id", equalTo(locationId))
            .body("assigned-locations[0].alias-id", nullValue())
            .body("assigned-locations[0].ref-location-id", nullValue());

        // patch the location group owned by CWMS office
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSON)
            .contentType(Formats.JSON)
            .body(groupJson)
            .header("Authorization", user.toHeaderValue())
            .queryParam(REPLACE_ASSIGNED_LOCS, true)
            .queryParam(OFFICE, officeId)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .patch("/location/group/" + newLocGroup.getId())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK));

        // get the location group and assert that there are no assigned locations
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSON)
            .contentType(Formats.JSON)
            .queryParam(OFFICE, CWMS_OFFICE)
            .queryParam(CATEGORY_ID, cat.getId())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/location/group/" + group.getId())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("assigned-locations.size()", equalTo(0));
    }

    @Test
    void test_CWMS_permissions_with_replacement() throws Exception {
        String officeId = user.getOperatingOffice();
        String locationId = "LocationGroupTest";
        createLocation(locationId, true, officeId);

        LocationCategory cat = new LocationCategory(CWMS_OFFICE, "Default", "Default");

        AssignedLocation assignLoc = new AssignedLocation(locationId, officeId, null, null, null);
        LocationGroup group = new LocationGroup(cat, CWMS_OFFICE, "Default", "All Locations",
                null, null, null);
        LocationGroup newLocGroup = new LocationGroup(group, Collections.singletonList(assignLoc));

        groupsToCleanup.add(group);

        String newGroupJson = Formats.format(new ContentType(Formats.JSON), newLocGroup);
        String groupJson = Formats.format(new ContentType(Formats.JSON), group);

        // get the location group and assert that changes were made
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSON)
            .contentType(Formats.JSON)
            .queryParam(OFFICE, CWMS_OFFICE)
            .queryParam(CATEGORY_ID, cat.getId())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/location/group/" + group.getId())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("assigned-locations.size()", equalTo(0));

        // patch the location group owned by CWMS office
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSON)
            .contentType(Formats.JSON)
            .body(newGroupJson)
            .header("Authorization", user.toHeaderValue())
            .queryParam(REPLACE_ASSIGNED_LOCS, true)
            .queryParam(OFFICE, officeId)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .patch("/location/group/" + group.getId())
        .then()
        .assertThat()
            .log().ifValidationFails(LogDetail.ALL,true)
            .statusCode(is(HttpServletResponse.SC_OK));

        // get the location group and assert that changes were made
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSON)
            .contentType(Formats.JSON)
            .queryParam(OFFICE, CWMS_OFFICE)
            .queryParam(CATEGORY_ID, cat.getId())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/location/group/" + group.getId())
        .then()
        .assertThat()
            .log().ifValidationFails(LogDetail.ALL,true)
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("office-id", equalTo(group.getOfficeId()))
            .body("id", equalTo(group.getId()))
            .body("description", equalTo(newLocGroup.getDescription()))
            .body("assigned-locations[0].location-id", equalTo(locationId))
            .body("assigned-locations[0].alias-id", nullValue())
            .body("assigned-locations[0].ref-location-id", nullValue());

        // patch the location group owned by CWMS office
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSON)
            .contentType(Formats.JSON)
            .body(groupJson)
            .header("Authorization", user.toHeaderValue())
            .queryParam(REPLACE_ASSIGNED_LOCS, true)
            .queryParam(OFFICE, officeId)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .patch("/location/group/" + group.getId())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK));

        // get the location group and assert that changes were made
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSON)
            .contentType(Formats.JSON)
            .queryParam(OFFICE, CWMS_OFFICE)
            .queryParam(CATEGORY_ID, cat.getId())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/location/group/" + group.getId())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("assigned-locations.size()", equalTo(0));
    }
}
