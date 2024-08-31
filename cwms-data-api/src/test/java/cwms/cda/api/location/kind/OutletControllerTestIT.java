/*
 * MIT License
 * Copyright (c) 2024 Hydrologic Engineering Center
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package cwms.cda.api.location.kind;

import com.google.common.flogger.FluentLogger;
import cwms.cda.api.Controllers;
import cwms.cda.api.errors.NotFoundException;
import cwms.cda.data.dao.DeleteRule;
import cwms.cda.data.dao.LocationGroupDao;
import cwms.cda.data.dao.location.kind.OutletDao;
import cwms.cda.data.dao.location.kind.ProjectStructureIT;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.Location;
import cwms.cda.data.dto.LocationGroup;
import cwms.cda.data.dto.location.kind.Outlet;
import cwms.cda.formatters.Formats;
import fixtures.CwmsDataApiSetupCallback;
import io.restassured.filter.log.LogDetail;
import java.io.IOException;
import javax.servlet.http.HttpServletResponse;
import mil.army.usace.hec.test.database.CwmsDatabaseContainer;
import org.jooq.DSLContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static cwms.cda.api.Controllers.*;
import static cwms.cda.data.dao.DaoTest.getDslContext;
import static cwms.cda.security.KeyAccessManager.AUTH_HEADER;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.*;

class OutletControllerTestIT extends ProjectStructureIT {
    private static final FluentLogger LOGGER = FluentLogger.forEnclosingClass();
    private static final CwmsId CONDUIT_GATE_RATING_GROUP = new CwmsId.Builder().withName(
            "Rating-" + PROJECT_1_ID.getName() + "-ConduitGate").withOfficeId(OFFICE_ID).build();
    private static final CwmsId MODIFIED_CONDUIT_GATE_RATING_GROUP = new CwmsId.Builder().withName(
            "Rating-" + PROJECT_1_ID.getName() + "-ConduitGateModified").withOfficeId(OFFICE_ID).build();
    private static final String OUTLET_KIND = "OUTLET";

    private static final Location NEW_CONDUIT_GATE_1 = buildProjectStructureLocation(PROJECT_1_ID.getName() + "-CG10",
                                                                                     OUTLET_KIND);
    private static final Outlet NEW_CONDUIT_GATE_1_OUTLET = buildTestOutlet(NEW_CONDUIT_GATE_1, PROJECT_LOC,
                                                                            CONDUIT_GATE_RATING_GROUP);

    private static final Location EXISTING_CONDUIT_GATE = buildProjectStructureLocation(
            PROJECT_1_ID.getName() + "-CG20", OUTLET_KIND);
    private static final Outlet EXISTING_CONDUIT_GATE_OUTLET = buildTestOutlet(EXISTING_CONDUIT_GATE, PROJECT_LOC,
                                                                               CONDUIT_GATE_RATING_GROUP);

    private static final Location NEW_CONDUIT_GATE_2 = buildProjectStructureLocation(PROJECT_1_ID.getName() + "-CG30",
                                                                                     OUTLET_KIND);
    private static final Outlet NEW_CONDUIT_GATE_2_OUTLET = buildTestOutlet(NEW_CONDUIT_GATE_2, PROJECT_LOC,
                                                                            CONDUIT_GATE_RATING_GROUP);
    private static final CwmsId RENAMED_CONDUIT_GATE = new CwmsId.Builder().withName(
            NEW_CONDUIT_GATE_2.getName() + "_Renamed").withOfficeId(OFFICE_ID).build();

    private static final String RATING_SPEC_ID_CONTROLLED = PROJECT_1_ID.getName()
            + ".Opening-Low_Flow_Gates,Elev;Flow-Low_Flow_Gates.Standard.Production";
    private static final CwmsId RATING_GROUP_CONTROLLED = new CwmsId.Builder().withName("Rating-" + PROJECT_1_ID.getName()
            + "-Controlled").withOfficeId(OFFICE_ID).build();
    private static final Location RATED_OUTLET_LOCATION_CONTROLLED
            = buildProjectStructureLocation(PROJECT_1_ID.getName() + "-TG1", OUTLET_KIND);
    private static final Outlet NEW_RATED_OUTLET_CONTROLLED
            = buildTestOutlet(RATED_OUTLET_LOCATION_CONTROLLED, PROJECT_LOC, RATING_GROUP_CONTROLLED, RATING_SPEC_ID_CONTROLLED);
    private static final String RATING_SPEC_ID_UNCONTROLLED = PROJECT_1_ID.getName()
            + ".Elev;Flow-Spillway.Standard.Production";
    private static final CwmsId RATING_GROUP_UNCONTROLLED = new CwmsId.Builder().withName("Rating-" + PROJECT_2_ID.getName()
            + "-Uncontrolled").withOfficeId(OFFICE_ID).build();
    private static final Location RATED_OUTLET_LOCATION_UNCONTROLLED
            = buildProjectStructureLocation(PROJECT_2_ID.getName() + "-TG2", OUTLET_KIND);
    private static final Outlet NEW_RATED_OUTLET_UNCONTROLLED
            = buildTestOutlet(RATED_OUTLET_LOCATION_UNCONTROLLED, PROJECT_LOC, RATING_GROUP_UNCONTROLLED, RATING_SPEC_ID_UNCONTROLLED);

    @BeforeAll
    public static void setup() throws Exception {
        setupProject();
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, OFFICE_ID);
            OutletDao outletDao = new OutletDao(context);
            try {
                deleteLocationGroup(context, NEW_RATED_OUTLET_CONTROLLED);
                deleteLocationGroup(context, NEW_RATED_OUTLET_UNCONTROLLED);
                deleteLocationGroup(context, NEW_CONDUIT_GATE_1_OUTLET);
                deleteLocationGroup(context, NEW_CONDUIT_GATE_2_OUTLET);
                deleteLocation(context, NEW_CONDUIT_GATE_1.getOfficeId(), NEW_CONDUIT_GATE_1.getName());
                deleteLocation(context, RENAMED_CONDUIT_GATE.getOfficeId(), RENAMED_CONDUIT_GATE.getName());
                storeLocation(context, NEW_CONDUIT_GATE_2);
                storeLocation(context, RATED_OUTLET_LOCATION_CONTROLLED);
                storeLocation(context, RATED_OUTLET_LOCATION_UNCONTROLLED);
                storeLocation(context, EXISTING_CONDUIT_GATE);
                outletDao.storeOutlet(EXISTING_CONDUIT_GATE_OUTLET, false);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    @AfterAll
    public static void tearDown() throws Exception {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, OFFICE_ID);
            OutletDao outletDao = new OutletDao(context);
            outletDao.deleteOutlet(EXISTING_CONDUIT_GATE.getOfficeId(), EXISTING_CONDUIT_GATE.getName(),
                                   DeleteRule.DELETE_ALL);
            deleteLocationGroup(context, NEW_RATED_OUTLET_CONTROLLED);
            deleteLocationGroup(context, NEW_RATED_OUTLET_UNCONTROLLED);
            deleteLocationGroup(context, EXISTING_CONDUIT_GATE_OUTLET);
            deleteLocationGroup(context, NEW_CONDUIT_GATE_1_OUTLET);
            deleteLocationGroup(context, NEW_CONDUIT_GATE_2_OUTLET);
            deleteLocation(context, NEW_CONDUIT_GATE_1.getOfficeId(), NEW_CONDUIT_GATE_1.getName());
            deleteLocation(context, NEW_CONDUIT_GATE_2.getOfficeId(), NEW_CONDUIT_GATE_2.getName());
            deleteLocation(context, RENAMED_CONDUIT_GATE.getOfficeId(), RENAMED_CONDUIT_GATE.getName());
            deleteLocation(context, EXISTING_CONDUIT_GATE.getOfficeId(), EXISTING_CONDUIT_GATE.getName());
            deleteLocation(context, RATED_OUTLET_LOCATION_CONTROLLED.getOfficeId(),
                           RATED_OUTLET_LOCATION_CONTROLLED.getName());
            deleteLocation(context, RATED_OUTLET_LOCATION_UNCONTROLLED.getOfficeId(),
                            RATED_OUTLET_LOCATION_UNCONTROLLED.getName());
        }, CwmsDataApiSetupCallback.getWebUser());
        tearDownProject();
    }

    @Disabled("Disabled due to a DB issue.  See https://jira.hecdev.net/browse/CWDB-296")
    @Test
    void test_outlet_rename() {
        String json = Formats.format(Formats.parseHeader(Formats.JSONV1, Outlet.class), NEW_CONDUIT_GATE_2_OUTLET);

        //Create the outlet
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.JSONV1)
            .body(json)
            .header(AUTH_HEADER, USER.toHeaderValue())
            .queryParam(FAIL_IF_EXISTS, "false")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("projects/outlets")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED));

        //Get the newly created outlet
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV1)
            .queryParam(Controllers.OFFICE, OFFICE_ID)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("projects/outlets/" + NEW_CONDUIT_GATE_2.getName())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
        ;

        //Rename new outlet
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV1)
            .header(AUTH_HEADER, USER.toHeaderValue())
            .queryParam(Controllers.OFFICE, OFFICE_ID)
            .queryParam(Controllers.NAME, RENAMED_CONDUIT_GATE.getName())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .patch("projects/outlets/" + NEW_CONDUIT_GATE_2.getName())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_NO_CONTENT))
        ;

        //Fail to retrieve old outlet name
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV1)
            .queryParam(Controllers.OFFICE, OFFICE_ID)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("projects/outlets/" + NEW_CONDUIT_GATE_2.getName())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_NOT_FOUND))
        ;

        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV1)
            .queryParam(Controllers.OFFICE, OFFICE_ID)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("projects/outlets/" + RENAMED_CONDUIT_GATE.getName())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
        ;
    }

    @Test
    void test_rating_spec_id_uncontrolled() {
        // Structure of test:
        // 1) Create the Outlet
        // 2) Retrieve the Outlet and assert that it has a null RatingSpecId
        // 3) Retrieve the location group associated with the outlet
        // 4) Modify the location group to have a rating spec id
        // 5) Store the location group back to the db
        // 6) Retrieve the Outlet and assert that it has the rating spec id
        // 7) Delete the outlet
        // 8) Delete the location group

        String json = Formats.format(Formats.parseHeader(Formats.JSONV1, Outlet.class), NEW_RATED_OUTLET_UNCONTROLLED);

        // Create the outlet
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.JSONV1)
            .body(json)
            .header(AUTH_HEADER, USER.toHeaderValue())
            .queryParam(FAIL_IF_EXISTS, "false")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("projects/outlets")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED));

        // Get the outlet, assert that it has null rating spec id
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV1)
            .queryParam(Controllers.OFFICE, OFFICE_ID)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("projects/outlets/" + RATED_OUTLET_LOCATION_UNCONTROLLED.getName())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("rating-spec-id", nullValue())
        ;

        // Assert the locationGroup has rating spec id
        LocationGroup locGroup = given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV1)
            .queryParam(OFFICE, OFFICE_ID)
            .queryParam(CATEGORY_ID, NEW_RATED_OUTLET_UNCONTROLLED.getRatingCategoryId().getName())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/location/group/" + RATING_GROUP_UNCONTROLLED.getName())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("shared-loc-alias-id", nullValue())
            .extract().as(LocationGroup.class);

        LocationGroup modifiedLocGroup = modifyRatingSpecId(locGroup, RATING_SPEC_ID_UNCONTROLLED);

        json = Formats.format(Formats.parseHeader(Formats.JSONV1, LocationGroup.class), modifiedLocGroup);

        // Update the location group - delete with cascade and create a new one
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .contentType(Formats.JSONV1)
            .header(AUTH_HEADER, USER.toHeaderValue())
            .queryParam(OFFICE, OFFICE_ID)
            .queryParam(CATEGORY_ID, NEW_RATED_OUTLET_UNCONTROLLED.getRatingCategoryId().getName())
            .queryParam(CASCADE_DELETE, true)
            .queryParam(GROUP_ID, RATING_GROUP_UNCONTROLLED.getName())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("/location/group/" + RATING_GROUP_UNCONTROLLED.getName())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NO_CONTENT));

        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .contentType(Formats.JSONV1)
            .body(json)
            .header(AUTH_HEADER, USER.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/location/group")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED));

        // Get the newly updated outlet, assert that it has rating spec id
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV1)
            .queryParam(Controllers.OFFICE, OFFICE_ID)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("projects/outlets/" + RATED_OUTLET_LOCATION_UNCONTROLLED.getName())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("rating-spec-id", equalTo(RATING_SPEC_ID_UNCONTROLLED))
        ;

        // Delete the Outlet
        given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .queryParam(Controllers.OFFICE, OFFICE_ID)
                .header(AUTH_HEADER, USER.toHeaderValue())
                .queryParam(OFFICE, OFFICE_ID)
                .queryParam(METHOD, "DELETE_KEY")
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .delete("projects/outlets/" + RATED_OUTLET_LOCATION_UNCONTROLLED.getName())
            .then()
                .log().ifValidationFails(LogDetail.ALL,true)
            .assertThat()
                .statusCode(is(HttpServletResponse.SC_NO_CONTENT));

        // Delete the LocationGroup
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .queryParam(Controllers.OFFICE, OFFICE_ID)
            .header(AUTH_HEADER, USER.toHeaderValue())
            .queryParam(OFFICE, OFFICE_ID)
            .queryParam(CATEGORY_ID, "Rating")
            .queryParam(CASCADE_DELETE, "true")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("/location/group/" + RATING_GROUP_UNCONTROLLED.getName())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NO_CONTENT));
    }

    @Test
    void test_rating_spec_id_controlled() {
        // Structure of test:
        // 1) Create the Outlet
        // 2) Retrieve the Outlet and assert that it has a null RatingSpecId
        // 3) Retrieve the location group associated with the outlet
        // 4) Modify the location group to have a rating spec id
        // 5) Store the location group back to the db
        // 6) Retrieve the Outlet and assert that it has the rating spec id
        // 7) Delete the outlet
        // 8) Delete the location group

        String json = Formats.format(Formats.parseHeader(Formats.JSONV1, Outlet.class), NEW_RATED_OUTLET_CONTROLLED);

        // Create the outlet
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.JSONV1)
        .body(json)
            .header(AUTH_HEADER, USER.toHeaderValue())
            .queryParam(FAIL_IF_EXISTS, "false")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("projects/outlets")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED));

        // Get the outlet, assert that it has null rating spec id
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV1)
            .queryParam(Controllers.OFFICE, OFFICE_ID)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("projects/outlets/" + RATED_OUTLET_LOCATION_CONTROLLED.getName())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("rating-spec-id", nullValue())
        ;

        // Assert the locationGroup has rating spec id
        LocationGroup locGroup = given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV1)
            .queryParam(OFFICE, OFFICE_ID)
            .queryParam(CATEGORY_ID, NEW_RATED_OUTLET_CONTROLLED.getRatingCategoryId().getName())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/location/group/" + RATING_GROUP_CONTROLLED.getName())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("shared-loc-alias-id", nullValue())
            .extract().as(LocationGroup.class);

        LocationGroup modifiedLocGroup = modifyRatingSpecId(locGroup, RATING_SPEC_ID_CONTROLLED);

        json = Formats.format(Formats.parseHeader(Formats.JSONV1, LocationGroup.class), modifiedLocGroup);

        // Update the location group - delete with cascade and create a new one
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .contentType(Formats.JSONV1)
            .header(AUTH_HEADER, USER.toHeaderValue())
            .queryParam(OFFICE, OFFICE_ID)
            .queryParam(CATEGORY_ID, NEW_RATED_OUTLET_CONTROLLED.getRatingCategoryId().getName())
            .queryParam(CASCADE_DELETE, true)
            .queryParam(GROUP_ID, RATING_GROUP_CONTROLLED.getName())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("/location/group/" + RATING_GROUP_CONTROLLED.getName())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NO_CONTENT));

        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .contentType(Formats.JSONV1)
            .body(json)
            .header(AUTH_HEADER, USER.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/location/group")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED));

        // Get the newly updated outlet, assert that it has rating spec id
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV1)
            .queryParam(Controllers.OFFICE, OFFICE_ID)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("projects/outlets/" + RATED_OUTLET_LOCATION_CONTROLLED.getName())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("rating-spec-id", equalTo(RATING_SPEC_ID_CONTROLLED))
        ;

        // Delete the Outlet
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .queryParam(Controllers.OFFICE, OFFICE_ID)
            .header(AUTH_HEADER, USER.toHeaderValue())
            .queryParam(OFFICE, OFFICE_ID)
            .queryParam(METHOD, "DELETE_KEY")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("projects/outlets/" + RATED_OUTLET_LOCATION_CONTROLLED.getName())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NO_CONTENT));

        // Delete the LocationGroup
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .queryParam(Controllers.OFFICE, OFFICE_ID)
            .header(AUTH_HEADER, USER.toHeaderValue())
            .queryParam(OFFICE, OFFICE_ID)
            .queryParam(CATEGORY_ID, "Rating")
            .queryParam(CASCADE_DELETE, "true")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("/location/group/" + RATING_GROUP_CONTROLLED.getName())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NO_CONTENT));
    }

    @Test
    void test_outlet_get_all() {
        //Get the newly created outlet
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV1)
            .queryParam(Controllers.OFFICE, OFFICE_ID)
            .queryParam(Controllers.PROJECT_ID, PROJECT_1_ID.getName())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("projects/outlets")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("location.name", contains(EXISTING_CONDUIT_GATE.getName()))
            .body("project-id.name", contains(EXISTING_CONDUIT_GATE_OUTLET.getProjectId().getName()))
            .body("project-id.office-id", contains(EXISTING_CONDUIT_GATE_OUTLET.getProjectId().getOfficeId()))
            .body("rating-group-id.name", contains(EXISTING_CONDUIT_GATE_OUTLET.getRatingGroupId().getName()))
            .body("rating-group-id.office-id", contains(EXISTING_CONDUIT_GATE_OUTLET.getRatingGroupId().getOfficeId()))
            .body("rating-spec-id", contains(EXISTING_CONDUIT_GATE_OUTLET.getRatingSpecId()))
            .body("rating-category-id.name", contains(EXISTING_CONDUIT_GATE_OUTLET.getRatingCategoryId().getName()))
            .body("rating-category-id.office-id", contains(EXISTING_CONDUIT_GATE_OUTLET.getRatingCategoryId().getOfficeId())
            );

    }

    @Test
    void test_outlet_crud() throws Exception {
        // Structure of test:
        // 1)Create the Outlet - TG3 does not exist in the db.
        // 2)Retrieve the Outlet and assert that it exists
        // 3)Delete the Outlet
        // 4)Retrieve the Outlet and assert that it does not exist
        String json = Formats.format(Formats.parseHeader(Formats.JSONV1, Outlet.class), NEW_CONDUIT_GATE_1_OUTLET);

        //Create the outlet
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.JSONV1)
            .body(json)
            .header(AUTH_HEADER, USER.toHeaderValue())
            .queryParam(FAIL_IF_EXISTS, "false")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("projects/outlets")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED));

        //Get the newly created outlet
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV1)
            .queryParam(Controllers.OFFICE, OFFICE_ID)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("projects/outlets/" + NEW_CONDUIT_GATE_1_OUTLET.getLocation().getName())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("location.name", equalTo(NEW_CONDUIT_GATE_1.getName()))
            .body("location.office-id", equalTo(NEW_CONDUIT_GATE_1.getOfficeId()))
            .body("project-id.name", equalTo(NEW_CONDUIT_GATE_1_OUTLET.getProjectId().getName()))
            .body("project-id.office-id", equalTo(NEW_CONDUIT_GATE_1_OUTLET.getProjectId().getOfficeId()))
            .body("rating-group-id.name", equalTo(NEW_CONDUIT_GATE_1_OUTLET.getRatingGroupId().getName()))
            .body("rating-group-id.office-id", equalTo(NEW_CONDUIT_GATE_1_OUTLET.getRatingGroupId().getOfficeId()))
            ;

        //Update the outlet rating group
        Outlet modifiedOutlet = new Outlet.Builder(NEW_CONDUIT_GATE_1_OUTLET)
                .withRatingGroupId(MODIFIED_CONDUIT_GATE_RATING_GROUP)
                .build();
        json = Formats.format(Formats.parseHeader(Formats.JSONV1, Outlet.class), modifiedOutlet);
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.JSONV1)
            .body(json)
            .header(AUTH_HEADER, USER.toHeaderValue())
            .queryParam(FAIL_IF_EXISTS, "false")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("projects/outlets")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED));

        //Get the newly modified outlet
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV1)
            .queryParam(Controllers.OFFICE, OFFICE_ID)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("projects/outlets/" + NEW_CONDUIT_GATE_1_OUTLET.getLocation().getName())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("location.name", equalTo(modifiedOutlet.getLocation().getName()))
            .body("location.office-id", equalTo(modifiedOutlet.getLocation().getOfficeId()))
            .body("project-id.name", equalTo(modifiedOutlet.getProjectId().getName()))
            .body("project-id.office-id", equalTo(modifiedOutlet.getProjectId().getOfficeId()))
            .body("rating-group-id.name", equalTo(modifiedOutlet.getRatingGroupId().getName()))
            .body("rating-group-id.office-id", equalTo(modifiedOutlet.getRatingGroupId().getOfficeId()))
        ;

        //Delete the Outlet
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .queryParam(Controllers.OFFICE, OFFICE_ID)
            .header(AUTH_HEADER, USER.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("projects/outlets/" + NEW_CONDUIT_GATE_1_OUTLET.getLocation().getName())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_NO_CONTENT));

        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, OFFICE_ID);
            deleteLocationGroup(context, modifiedOutlet);
        }, CwmsDataApiSetupCallback.getWebUser());

        //Re-retrieve, and ensure it doesn't exist anymore.
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV1)
            .queryParam(Controllers.OFFICE, OFFICE_ID)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("projects/outlets/" + NEW_CONDUIT_GATE_1_OUTLET.getLocation().getName())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_NOT_FOUND));
    }

    private static LocationGroup modifyRatingSpecId(LocationGroup locGroup, String ratingSpecId) {
        LocationGroup modifiedLocGroup = new LocationGroup(locGroup.getLocationCategory(), locGroup.getOfficeId(),
                locGroup.getId(), locGroup.getDescription(), ratingSpecId,
                locGroup.getSharedRefLocationId(),
                locGroup.getLocGroupAttribute());
        modifiedLocGroup = new LocationGroup(modifiedLocGroup, locGroup.getAssignedLocations());
        return modifiedLocGroup;
    }

    private static Outlet buildTestOutlet(Location outletLoc, Location projectLoc, CwmsId ratingId, String ratingSpecId) {
        return new Outlet.Builder().withProjectId(new CwmsId.Builder().withName(projectLoc.getName())
                                                                      .withOfficeId(projectLoc.getOfficeId())
                                                                      .build())
                                   .withLocation(outletLoc)
                                   .withRatingGroupId(ratingId)
                                   .withRatingSpecId(ratingSpecId)
                                   .build();
    }

    private static Outlet buildTestOutlet(Location outletLoc, Location projectLoc, CwmsId ratingId) {
        return buildTestOutlet(outletLoc, projectLoc, ratingId, null);
    }
}