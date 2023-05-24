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

import static cwms.radar.api.Controllers.*;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import cwms.radar.data.dto.AssignedLocation;
import cwms.radar.data.dto.LocationCategory;
import cwms.radar.data.dto.LocationGroup;
import cwms.radar.formatters.ContentType;
import cwms.radar.formatters.Formats;
import fixtures.RadarApiSetupCallback;
import fixtures.TestAccounts;
import java.util.List;
import javax.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@Tag("integration")
@ExtendWith(RadarApiSetupCallback.class)
class LocationGroupControllerTestIT extends DataApiTestIT
{

	@Test
	void test_create_read_delete() throws Exception {
		String officeId = "SPK";
		String locationId = "LocationGroupTest";
		createLocation(locationId, true, officeId);
		TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;
		LocationCategory cat = new LocationCategory(officeId, "TestCategory", "IntegrationTesting");
		LocationGroup group = new LocationGroup(cat, officeId, LocationGroupControllerTestIT.class.getSimpleName(), "IntegrationTesting",
			"sharedLocAliasId", locationId, 123);
		List<AssignedLocation> assignedLocations = group.getAssignedLocations();
		assignedLocations.add(new AssignedLocation(locationId, officeId, "AliasId", 1, locationId));
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
			.when()
			.redirects().follow(true)
			.redirects().max(3)
			.post("/location/category")
			.then()
			.assertThat()
			.statusCode(is(HttpServletResponse.SC_CREATED));
		//Create Group
		given()
			.accept(Formats.JSON)
			.contentType(Formats.JSON)
			.queryParam(OFFICE, officeId)
			.body(groupXml)
			.header("Authorization", user.toHeaderValue())
			.when()
			.redirects().follow(true)
			.redirects().max(3)
			.post("/location/group")
			.then()
			.assertThat()
			.statusCode(is(HttpServletResponse.SC_CREATED));
		//Read
		given()
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
			.log().body().log().everything(true)
			.statusCode(is(HttpServletResponse.SC_OK))
			.body("office-id", equalTo(group.getOfficeId()))
			.body("id", equalTo(group.getId()))
			.body("description", equalTo(group.getDescription()))
			.body("assigned-locations[0].location-id", equalTo(locationId))
			.body("assigned-locations[0].alias-id", equalTo("AliasId"))
			.body("assigned-locations[0].ref-location-id", equalTo(locationId));
		//Delete Group
		given()
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
			.get("/location/group/" + group.getId())
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
			.queryParam(CASCADE_DELETE, "true")
			.when()
			.redirects().follow(true)
			.redirects().max(3)
			.delete("/location/category/" + group.getLocationCategory().getId())
			.then()
			.assertThat()
			.log().body().log().everything(true)
			.statusCode(is(HttpServletResponse.SC_NO_CONTENT));
	}

	@Test
	void test_rename_group() throws Exception {
		String officeId = "SPK";
		String locationId = "LocationGroupTest";
		createLocation(locationId, true, officeId);
		TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;
		LocationCategory cat = new LocationCategory(officeId, "test_rename_group", "IntegrationTesting");
		LocationGroup group = new LocationGroup(cat, officeId, "test_rename_group", "IntegrationTesting",
			"sharedLocAliasId", locationId, 123);
		List<AssignedLocation> assignedLocations = group.getAssignedLocations();
		assignedLocations.add(new AssignedLocation(locationId, officeId, "AliasId", 1, locationId));
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
			.when()
			.redirects().follow(true)
			.redirects().max(3)
			.post("/location/category/")
			.then()
			.assertThat()
			.statusCode(is(HttpServletResponse.SC_CREATED));
		//Create Group
		given()
			.accept(Formats.JSON)
			.contentType(Formats.JSON)
			.body(groupXml)
			.queryParam(OFFICE,officeId)
			.header("Authorization", user.toHeaderValue())
			.when()
			.redirects().follow(true)
			.redirects().max(3)
			.post("/location/group")
			.then()
			.assertThat()
			.statusCode(is(HttpServletResponse.SC_CREATED));
		LocationGroup newGroup = new LocationGroup(cat, officeId, "test_rename_group_new", "IntegrationTesting",
			"sharedLocAliasId", locationId, 123);
		String newGroupXml = Formats.format(contentType, newGroup);
		//Rename Group
		given()
			.accept(Formats.JSON)
			.contentType(Formats.JSON)
			.body(newGroupXml)
			.header("Authorization", user.toHeaderValue())
			.header(CATEGORY_ID, group.getLocationCategory().getId())
			.header(OFFICE, group.getOfficeId())
			.when()
			.redirects().follow(true)
			.redirects().max(3)
			.patch("/location/group/"+ group.getId())
			.then()
			.assertThat()
			.statusCode(is(HttpServletResponse.SC_ACCEPTED));
		//Read
		given()
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
			.log().body().log().everything(true)
			.statusCode(is(HttpServletResponse.SC_OK))
			.body("office-id", equalTo(newGroup.getOfficeId()))
			.body("id", equalTo(newGroup.getId()))
			.body("description", equalTo(newGroup.getDescription()))
			.body("assigned-locations[0].location-id", equalTo(locationId))
			.body("assigned-locations[0].alias-id", equalTo("AliasId"))
			.body("assigned-locations[0].ref-location-id", equalTo(locationId));
		//Delete Category
		given()
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
			.log().body().log().everything(true)
			.statusCode(is(HttpServletResponse.SC_NO_CONTENT));
	}

	@Test
	void test_add_assigned_locs() throws Exception {
		String officeId = "SPK";
		String locationId = "LocationGroupTest";
		createLocation(locationId, true, officeId);
		String locationId2 = "LocationGroupTest2";
		createLocation(locationId2, true, officeId);
		TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;
		LocationCategory cat = new LocationCategory(officeId, "test_add_assigned_locs", "IntegrationTesting");
		LocationGroup group = new LocationGroup(cat, officeId, "test_add_assigned_locs", "IntegrationTesting",
			"sharedLocAliasId", locationId, 123);
		List<AssignedLocation> assignedLocations = group.getAssignedLocations();
		assignedLocations.add(new AssignedLocation(locationId, officeId, "AliasId", 1, locationId));
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
			.when()
			.redirects().follow(true)
			.redirects().max(3)
			.post("/location/category/")
			.then()
			.assertThat()
			.statusCode(is(HttpServletResponse.SC_CREATED));
		//Create Group
		given()
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
			.statusCode(is(HttpServletResponse.SC_CREATED));
		assignedLocations.clear();
		assignedLocations.add(new AssignedLocation(locationId2, officeId, "AliasId2", 2, locationId2));
		groupXml = Formats.format(contentType, group);
		//Add Assigned Locs
		given()
			.accept(Formats.JSON)
			.contentType(Formats.JSON)
			.body(groupXml)
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
			.statusCode(is(HttpServletResponse.SC_ACCEPTED));
		//Read
		given()
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
			.log().body().log().everything(true)
			.statusCode(is(HttpServletResponse.SC_OK))
			.body("office-id", equalTo(group.getOfficeId()))
			.body("id", equalTo(group.getId()))
			.body("description", equalTo(group.getDescription()))
			.body("assigned-locations[0].location-id", equalTo(locationId2))
			.body("assigned-locations[0].alias-id", equalTo("AliasId2"))
			.body("assigned-locations[0].ref-location-id", equalTo(locationId2));
		//Delete Category
		given()
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
			.log().body().log().everything(true)
			.statusCode(is(HttpServletResponse.SC_NO_CONTENT));
	}


}