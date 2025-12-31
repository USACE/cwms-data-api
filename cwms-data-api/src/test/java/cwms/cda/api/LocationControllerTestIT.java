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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import cwms.cda.data.dao.LocationCategoryDao;
import cwms.cda.data.dao.LocationGroupDao;
import fixtures.CwmsDataApiSetupCallback;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import cwms.cda.data.dto.AssignedLocation;
import cwms.cda.data.dto.LocationCategory;
import cwms.cda.data.dto.LocationGroup;
import cwms.cda.formatters.ContentType;
import fixtures.TestAccounts.KeyUser;
import io.restassured.filter.log.LogDetail;
import org.jooq.DSLContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import cwms.cda.data.dto.Location;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.json.JsonV1;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.apache.commons.lang3.RandomStringUtils;

import javax.servlet.http.HttpServletResponse;

import static cwms.cda.api.Controllers.*;
import static cwms.cda.data.dao.JsonRatingUtilsTest.loadResourceAsString;
import static io.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isOneOf;

@Tag("integration")
class LocationControllerTestIT extends DataApiTestIT {

    private final List<LocationCategory> categoriesToCleanup = new ArrayList<>();
    private final List<LocationGroup> groupsToCleanup = new ArrayList<>();

    private static final String OFFICE_ID = "office-id";
    private static final String MESSAGE = "message";
    private static final String IDENTIFIER = "identifier";

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
        String expectedIdentifier = "LOC_TEST";

        create_get_delete(json, officeId, expectedIdentifier);
    }

    @Test
    void test_location_create_get_delete_slash() throws Exception {
        String officeId = "SPK";
        String json = loadResourceAsString("cwms/cda/api/location_create_spk_slash.json");
        String expectedIdentifier = "/SLASH_TEST"; // needs to match id in json

        create_get_delete(json, officeId, expectedIdentifier);
    }


    private static void create_get_delete(String json, String officeId, String expectedIdentifier) throws JsonProcessingException {
        Location location = new Location.Builder(Formats.parseContent(Formats.parseHeader(Formats.JSON, Location.class),
                json, Location.class))
                .withOfficeId(officeId)
                //withName(getClass().getSimpleName())
                .build();
        String serializedLocation = JsonV1.buildObjectMapper().registerModule(new Jdk8Module()).writeValueAsString(location);

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
            .body(OFFICE_ID, equalTo(officeId))
            .body(MESSAGE, equalTo("Created Location"))
            .body(IDENTIFIER, equalTo(expectedIdentifier));
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
            .queryParam(OFFICE, officeId)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/locations/{loc-id}" , location.getName())
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
            .delete("/locations/{loc-id}" , location.getName())
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
            .delete("/locations/{loc-id}" , location.getName())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body(OFFICE_ID, equalTo(officeId))
            .body(MESSAGE, equalTo("Deleted CWMS Location"))
            .body(IDENTIFIER, equalTo(expectedIdentifier));

        // get it back
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSON)
            .queryParam(OFFICE, officeId)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/locations/{loc-id}" , location.getName())
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
        String serializedLocation = JsonV1.buildObjectMapper().registerModule(new Jdk8Module()).writeValueAsString(location);

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
        String serializedLocation = JsonV1.buildObjectMapper().registerModule(new Jdk8Module()).writeValueAsString(location);

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
            .body(OFFICE_ID, equalTo(officeId))
            .body(MESSAGE, equalTo("Deleted CWMS Location"))
            .body(IDENTIFIER, equalTo("LOC_TEST"));

        // get it back
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSON)
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
            .body(OFFICE_ID, equalTo(user.getOperatingOffice()))
            .body(MESSAGE, equalTo("Updated and renamed Location"))
            .body(IDENTIFIER, equalTo(updatedLocationName));

        // get it back
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSON)
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
            .body(OFFICE_ID, equalTo(user.getOperatingOffice()))
            .body(MESSAGE, equalTo("Deleted CWMS Location"))
            .body(IDENTIFIER, equalTo(updatedLocationName));
    }

    @Test
    void test_create_update_null_elev_units() throws Exception {
        String locationName = "TestUpdateLoc1";
        KeyUser user = KeyUser.SPK_NORMAL;

        String serializedLocation = loadResourceAsString("cwms/cda/api/location_create_spk.json")
            .replace("LOC_TEST", locationName);

        String serializedUpdateLocation = loadResourceAsString("cwms/cda/api/location_create_spk_no_elev_units.json")
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
        String updatedSerializedLocation = serializedUpdateLocation
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
            .body(OFFICE_ID, equalTo(user.getOperatingOffice()))
            .body(MESSAGE, equalTo("Updated and renamed Location"))
            .body(IDENTIFIER, equalTo(updatedLocationName));

        // get it back
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSON)
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
            .body(OFFICE_ID, equalTo(user.getOperatingOffice()))
            .body(MESSAGE, equalTo("Deleted CWMS Location"))
            .body(IDENTIFIER, equalTo(updatedLocationName));
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
        String serializedLocation = JsonV1.buildObjectMapper().registerModule(new Jdk8Module()).writeValueAsString(location);

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
            .body("source", equalTo("User Input"))
            .body("message", equalTo("One or more provided values exceeds the maximum length for the parameter."));
    }

    @Test
    void testIncludeAliases() throws Exception {
        String officeId = "SPK";
        String locationName = "TestBaseLocation";
        String controlLocationName = "TestBaseLocControl";
        createLocation(controlLocationName, true, officeId);
        createLocation(locationName, true, officeId);

        String categoryName = "TestAliasesCategory1";
        String groupName1 = "TestAliasesGroup3";
        String groupName2 = "TestAliasesGroup4";
        String sharedLocAlias1 = "TESTBASELOCALIAS1";
        String sharedLocAlias2 = "LOCALIAS1";

        CwmsDataApiSetupCallback.getDatabaseLink().connection(c -> {
            DSLContext dsl = dslContext(c, officeId);
            LocationCategory category = new LocationCategory(officeId, categoryName, "A test category");
            LocationCategoryDao catDao = new LocationCategoryDao(dsl);

            catDao.create(category);
            categoriesToCleanup.add(category);

            LocationGroupDao groupDao = new LocationGroupDao(dsl);
            LocationGroup baseGroup1 = new LocationGroup(category, officeId, groupName1, "A test group",
                sharedLocAlias1, null, 0);
            LocationGroup baseGroup2 = new LocationGroup(category, officeId, groupName2, "Another test group",
                sharedLocAlias2, null, 0);

            groupDao.create(baseGroup1);
            groupDao.create(baseGroup2);
            groupsToCleanup.add(baseGroup1);
            groupsToCleanup.add(baseGroup2);

            List<AssignedLocation> locations = new ArrayList<>();
            AssignedLocation assignedLocation = new AssignedLocation(locationName, officeId, sharedLocAlias1, null, null);
            locations.add(assignedLocation);
            LocationGroup group = new LocationGroup(baseGroup1, locations);
            groupDao.assignLocs(group, officeId);

            locations = new ArrayList<>();
            assignedLocation = new AssignedLocation(locationName, officeId, sharedLocAlias2, null, null);
            locations.add(assignedLocation);
            LocationGroup group2 = new LocationGroup(baseGroup2, locations);
            groupDao.assignLocs(group2, officeId);
        });

        // verify that the control location can be retrieved
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .queryParam(OFFICE, officeId)
            .queryParam(UNIT, "SI")
            .queryParam(INCLUDE_ALIASES, "false")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/locations/" + controlLocationName)
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("$", not(hasKey("aliases")))
        ;

        // verify that the aliased level can be retrieved
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .queryParam(OFFICE, officeId)
            .queryParam(UNIT, "SI")
            .queryParam(INCLUDE_ALIASES, "true")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/locations/" + locationName)
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("aliases.size()", is(2))
            .body("aliases[0].value", isOneOf(sharedLocAlias1, sharedLocAlias2))
            .body("aliases[0].name", isOneOf(categoryName + "-" + groupName1, categoryName + "-" + groupName2))
            .body("aliases[1].name", isOneOf(categoryName + "-" + groupName1, categoryName + "-" + groupName2))
            .body("aliases[1].value", isOneOf(sharedLocAlias1, sharedLocAlias2))
        ;

        // verify that alias as location ID does not return results
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .queryParam(OFFICE, officeId)
            .queryParam(UNIT, "SI")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/locations/" + sharedLocAlias1)
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NOT_FOUND))
        ;

        // verify that alias as location ID will not return results
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .queryParam(OFFICE, officeId)
            .queryParam(UNIT, "SI")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/locations/" + sharedLocAlias2)
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NOT_FOUND))
        ;
    }

    @Test
    void testAlreadyExists() throws Exception {
        KeyUser user = KeyUser.SPK_NORMAL;

        Location location = new Location
            .Builder("Putah_Creek", "STREAM",
                ZoneId.of("UTC"), 38.55, -121.74,
                "NGVD29", user.getOperatingOffice())
            .withActive(true)
            .withNearestCity("Davis")
            .withOfficeId(user.getOperatingOffice())
            .withBoundingOfficeId(user.getOperatingOffice())
            .build();
        createLocation(location.getName(), location.getActive(), location.getOfficeId(), location.getLatitude(),
            location.getLongitude(), location.getHorizontalDatum(), location.getTimezoneName(), location.getLocationKind());

        String locationString = Formats.format(new ContentType(Formats.JSON), location);

        // attempt to create location of the same name as existing location
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSON)
            .contentType(Formats.JSON)
            .body(locationString)
            .header("Authorization", user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/locations")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CONFLICT))
            .body("message", equalTo("Already exists"))
            .body("source", equalTo("Database"))
            .body("details.message",
                equalTo(String.format("The location with name: %s already exists in office: %s",
                    location.getName(), user.getOperatingOffice())));
    }

    @Test
    void testDeleteConflict() throws Exception {
        KeyUser user = KeyUser.SPK_NORMAL;

        Location location = new Location
            .Builder("Putah_Creek", "STREAM",
            ZoneId.of("UTC"), 38.55, -121.74,
            "NGVD29", user.getOperatingOffice())
            .withActive(true)
            .withNearestCity("Davis")
            .build();
        createLocation(location.getName(), location.getActive(), location.getOfficeId(), location.getLatitude(),
            location.getLongitude(), location.getHorizontalDatum(), location.getTimezoneName(), location.getLocationKind());

        String timeseriesId = "Putah_Creek.Elev.Ave.30Minutes.30Minutes.Raw";
        createTimeseries(user.getOperatingOffice(), timeseriesId);

        // attempt to delete location that is referenced by TS
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSON)
            .contentType(Formats.JSON)
            .queryParam(OFFICE, user.getOperatingOffice())
            .header("Authorization", user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("/locations/" + location.getName())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CONFLICT))
            .body("source", equalTo("Database"))
            .body("message",
                equalTo("Cannot delete this record because it is linked to other data in CWMS"))
            .body("details.message", equalTo("Unable to delete requested location: "
                + "Putah_Creek for office: SPK: ORA-20031: CAN_NOT_DELETE_LOC_1: "
                + "Can not delete location: \"Putah_Creek\" because Timeseries Identifiers exist."));
    }

    @Test
    void testNotFound() {
        String location = "NonExistentLoc123";
        String officeId = "SPK";
        String unitSystem = "SI";

        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .queryParam(OFFICE, officeId)
            .queryParam(UNIT, unitSystem)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/locations/" + location)
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NOT_FOUND))
            .body("message", equalTo(String.format("Location not found for office:%s and unit system:%s and id:%s",
                officeId, unitSystem, location)))
            .body("source", equalTo("Database"))
        ;
    }

    @Test
    void testRequiredQueryParam() throws Exception {
        String location = "Putah_Creek_Basin";
        String officeId = "SPK";

        createLocation(location, true, officeId);

        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/locations/" + location)
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_BAD_REQUEST))
            .body("message", equalTo("Bad Request"))
            .body("source", equalTo("User Input"))
            .body("details.'missing query parameters'", equalTo("office"))
        ;
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
