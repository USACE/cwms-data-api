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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import cwms.cda.data.dto.AssignedLocation;
import cwms.cda.data.dto.LocationCategory;
import cwms.cda.data.dto.LocationGroup;
import cwms.cda.formatters.ContentType;
import fixtures.TestAccounts.KeyUser;
import io.restassured.filter.log.LogDetail;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import cwms.cda.data.dto.Location;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.json.JsonV1;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.testcontainers.shaded.org.apache.commons.lang3.RandomStringUtils;

import javax.servlet.http.HttpServletResponse;

import static cwms.cda.api.Controllers.*;
import static cwms.cda.data.dao.JsonRatingUtilsTest.loadResourceAsString;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

@Tag("integration")
class LocationControllerTestIT extends DataApiTestIT {

    private final List<LocationCategory> categoriesToCleanup = new ArrayList<>();
    private final List<LocationGroup> groupsToCleanup = new ArrayList<>();

    @AfterEach
    void cleanup()
    {
        KeyUser user = KeyUser.SPK_NORMAL;

        for (LocationGroup group : groupsToCleanup) {
            try {
                // Delete Group
                given()
                    .log().ifValidationFails(LogDetail.ALL, true)
                    .accept(Formats.JSON)
                    .contentType(Formats.JSON)
                    .header("Authorization", user.toHeaderValue())
                    .queryParam(OFFICE, group.getOfficeId())
                    .queryParam(CATEGORY_ID, group.getLocationCategory().getId())
                    .queryParam(CASCADE_DELETE, "true")
                .when()
                    .redirects().follow(true)
                    .redirects().max(3)
                    .delete("/location/group/" + group.getId())
                .then()
                    .log().ifValidationFails(LogDetail.ALL, true)
                .assertThat()
                    .statusCode(is(HttpServletResponse.SC_NO_CONTENT));
            } catch (Exception ex) {
                // ignore
            }
        }

        for (LocationCategory category : categoriesToCleanup) {
            try {
                //Delete Category
                given()
                    .log().ifValidationFails(LogDetail.ALL,true)
                    .accept(Formats.JSON)
                    .contentType(Formats.JSON)
                    .header("Authorization", user.toHeaderValue())
                    .queryParam(OFFICE, category.getOfficeId())
                    .queryParam(CASCADE_DELETE, "true")
                .when()
                    .redirects().follow(true)
                    .redirects().max(3)
                    .delete("/location/category/" + category.getId())
                .then()
                    .log().ifValidationFails(LogDetail.ALL,true)
                .assertThat()
                    .statusCode(is(HttpServletResponse.SC_NO_CONTENT));
            } catch (Exception ex) {
                // ignore
            }
        }
    }

    @Test
    void test_location_create_get_delete() throws Exception {
        String officeId = "SPK";
        String json = loadResourceAsString("cwms/cda/api/location_create_spk.json");
        Location location = new Location.Builder(Formats.parseContent(Formats.parseHeader(Formats.JSON, Location.class),
            json, Location.class))
                .withOfficeId(officeId)
                //withName(getClass().getSimpleName())
                .build();
        String serializedLocation = JsonV1.buildObjectMapper().writeValueAsString(location);

        KeyUser user = KeyUser.SPK_NORMAL;
        // create location
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSON)
            .contentType(Formats.JSON)
            .body(serializedLocation)
            .header("Authorization", user.toHeaderValue())
            .queryParam(FAIL_IF_EXISTS, false)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/locations")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED))
                .body("office-id", equalTo(officeId))
                .body("message", equalTo("Created Location"))
                .body("identifier", equalTo("LOC_TEST"));
        //Create associated time series so delete fails without cascade
        try {
            createTimeseries(officeId, location.getName() + ".Flow.Inst.~1Hour.0.cda-test");
        } catch (Exception ex) {
            // ignore
        }

        // get it back
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSON)
            .header("Authorization", user.toHeaderValue())
            .queryParam(OFFICE, officeId)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/locations/" + location.getName())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK));

        // delete without cascade should fail
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSON)
            .header("Authorization", user.toHeaderValue())
            .queryParam(OFFICE, officeId)
            .queryParam(CASCADE_DELETE, false)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("/locations/" + location.getName())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_CONFLICT));

        // delete with cascade should succeed
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSON)
            .header("Authorization", user.toHeaderValue())
            .queryParam(OFFICE, officeId)
            .queryParam(CASCADE_DELETE, true)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("/locations/" + location.getName())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
                .body("office-id", equalTo(officeId))
                .body("message", equalTo("Deleted CWMS Location"))
                .body("identifier", equalTo("LOC_TEST"));

        // get it back
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSON)
            .header("Authorization", user.toHeaderValue())
            .queryParam(OFFICE, officeId)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/locations/" + location.getName())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_NOT_FOUND));
    }

    @Test
    void test_location_create_aliased() throws Exception {
        // Tests for https://github.com/USACE/cwms-data-api/issues/1080
        String officeId = "SPK";
        KeyUser user = KeyUser.SPK_NORMAL;
        String json = loadResourceAsString("cwms/cda/api/location_failure_test_spk.json");
        Location location = new Location.Builder(Formats.parseContent(Formats.parseHeader(Formats.JSON, Location.class),
                json, Location.class))
                .withOfficeId(officeId)
                .build();
        String serializedLocation = JsonV1.buildObjectMapper().writeValueAsString(location);

        // create location
        String locationId = "Test_Location_1080";
        createLocation(locationId, true, officeId);

        // create location group with location
        AssignedLocation assignLoc = new AssignedLocation(locationId, officeId, location.getName(), 1, locationId);
        LocationCategory cat = new LocationCategory(officeId, "TestCategory", "IntegrationTesting");
        LocationGroup group = new LocationGroup(
            new LocationGroup(cat, officeId, LocationGroupControllerTestIT.class.getSimpleName(), "IntegrationTesting",
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
            .queryParam(CATEGORY_OFFICE_ID, officeId)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/location/category")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
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
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED));

        // attempt to create location of the same name as alias
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSON)
            .contentType(Formats.JSON)
            .body(serializedLocation)
            .header("Authorization", user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/locations")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CONFLICT))
            .body("details.message",
                equalTo(String.format("The location with alias: '%s' and proper name: '%s' already exists in office: '%s'.",
                    location.getName(), locationId, officeId)));

        // get the existing location
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSON)
            .header("Authorization", user.toHeaderValue())
            .queryParam("office", officeId)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/locations/" + locationId)
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK));

        // get the location by alias, expect not to be found
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSON)
            .header("Authorization", user.toHeaderValue())
            .queryParam("office", officeId)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/locations/" + location.getName())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NOT_FOUND));
    }

    @Test
    void test_location_create_get_bad_units_delete() throws Exception {
        String officeId = "SPK";
        String json = loadResourceAsString("cwms/cda/api/location_create_spk.json");
        Location location = new Location.Builder(Formats.parseContent(Formats.parseHeader(Formats.JSON, Location.class),
                json, Location.class))
                .withOfficeId(officeId)
                //withName(getClass().getSimpleName())
                .build();
        String serializedLocation = JsonV1.buildObjectMapper().writeValueAsString(location);

        KeyUser user = KeyUser.SPK_NORMAL;
        // create location
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSON)
            .contentType(Formats.JSON)
            .body(serializedLocation)
            .header("Authorization", user.toHeaderValue())
            .queryParam(FAIL_IF_EXISTS, false)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/locations")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED));

        // get it back
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSON)
            .header("Authorization", user.toHeaderValue())
            .queryParam(OFFICE, officeId)
            .queryParam(UNIT, "m")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/locations/" + location.getName())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NOT_FOUND));

        // delete location
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSON)
            .header("Authorization", user.toHeaderValue())
            .queryParam(OFFICE, officeId)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("/locations/" + location.getName())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
                .body("office-id", equalTo(officeId))
                .body("message", equalTo("Deleted CWMS Location"))
                .body("identifier", equalTo("LOC_TEST"));

        // get it back
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSON)
            .header("Authorization", user.toHeaderValue())
            .queryParam(OFFICE, officeId)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/locations/" + location.getName())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NOT_FOUND));
    }

    @Test
    void test_create_update() throws Exception {
        String locationName = "TestUpdateLoc";
        KeyUser user = KeyUser.SPK_NORMAL;

        String serializedLocation = loadResourceAsString("cwms/cda/api/location_create_spk.json")
            .replace("LOC_TEST", locationName);

        // create location
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSON)
            .contentType(Formats.JSON)
            .body(serializedLocation)
            .header("Authorization", user.toHeaderValue())
            .queryParam(FAIL_IF_EXISTS, false)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/locations")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED));

        // get it back
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSON)
            .header("Authorization", user.toHeaderValue())
            .queryParam(OFFICE, user.getOperatingOffice())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/locations/" + locationName)
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("name", equalTo(locationName));

        // update location
        String updatedLocationName = locationName + "_UPDATED";
        String updatedSerializedLocation = serializedLocation
            .replace(locationName, updatedLocationName);

        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSON)
            .contentType(Formats.JSON)
            .body(updatedSerializedLocation)
            .header("Authorization", user.toHeaderValue())
            .queryParam(FAIL_IF_EXISTS, false)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .patch("/locations/" + locationName)
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
                .body("office-id", equalTo(user.getOperatingOffice()))
                .body("message", equalTo("Updated and renamed Location"))
                .body("identifier", equalTo(updatedLocationName));

        // get it back
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSON)
            .header("Authorization", user.toHeaderValue())
            .queryParam(OFFICE, user.getOperatingOffice())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/locations/" + updatedLocationName)
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("name", equalTo(updatedLocationName));

        // delete location
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSON)
            .header("Authorization", user.toHeaderValue())
            .queryParam(OFFICE, user.getOperatingOffice())
            .queryParam(CASCADE_DELETE, true)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("/locations/" + updatedLocationName)
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
                .body("office-id", equalTo(user.getOperatingOffice()))
                .body("message", equalTo("Deleted CWMS Location"))
                .body("identifier", equalTo(updatedLocationName));
    }

    @Test
    void test_delete_location_that_does_not_exist() {
        final String officeId = "SPK";
        final String locationName = "I do not exit";
        final KeyUser user = KeyUser.SPK_NORMAL;

        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSON)
            .header("Authorization", user.toHeaderValue())
            .queryParam(OFFICE, officeId)
            .queryParam(CASCADE_DELETE, true)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("/locations/{loc}", locationName)
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_NOT_FOUND));
    }

    @ParameterizedTest
    @EnumSource(GetAllTest.class)
    void test_get_all_locations(GetAllTest test)
    {
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(test._accept)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/locations/")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .contentType(is(test._expectedContentType));
    }

    @ParameterizedTest
    @EnumSource(GetAllLegacyTest.class)
    void test_get_all_locations_legacy_types(GetAllLegacyTest test)
    {
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .queryParam(FORMAT, test._accept)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/locations/")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .contentType(is(test._expectedContentType));
    }

    enum GetAllLegacyTest
    {
        JSON(Formats.JSON_LEGACY, Formats.JSON),
        CSV(Formats.CSV_LEGACY, Formats.CSV),
        XML(Formats.XML_LEGACY, Formats.XML),
        TAB(Formats.TAB_LEGACY, Formats.TAB),
        GEOJSON(Formats.GEOJSON_LEGACY, Formats.GEOJSON),
        ;

        final String _accept;
        final String _expectedContentType;

        GetAllLegacyTest(String accept, String expectedContentType)
        {
            _accept = accept;
            _expectedContentType = expectedContentType;
        }
    }

    @Test
    void test_name_too_long() throws Exception
    {
        String officeId = "SPK";
        String invalidLongName = RandomStringUtils.randomAlphabetic(200);
        String json = loadResourceAsString("cwms/cda/api/location_create_spk.json");
        Location location = new Location.Builder(Formats.parseContent(Formats.parseHeader(Formats.JSON, Location.class),
                json, Location.class))
                .withOfficeId(officeId)
                .withName(invalidLongName)
                .build();
        String serializedLocation = JsonV1.buildObjectMapper().writeValueAsString(location);

        KeyUser user = KeyUser.SPK_NORMAL;
        // create location
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSON)
            .contentType(Formats.JSON)
            .body(serializedLocation)
            .header("Authorization", user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/locations")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_BAD_REQUEST))
            .body(containsString("One or more provided values exceeds the maximum length for the parameter."));
    }

    enum GetAllTest
    {
        DEFAULT(Formats.DEFAULT, Formats.JSONV2),
        JSON(Formats.JSON, Formats.JSONV2),
        JSONV1(Formats.JSONV1, Formats.JSONV1),
        JSONV2(Formats.JSONV2, Formats.JSONV2),
        GEOJSON(Formats.GEOJSON, Formats.GEOJSON),
        XML(Formats.XML, Formats.XMLV2),
        XMLV1(Formats.XMLV1, Formats.XMLV1),
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
