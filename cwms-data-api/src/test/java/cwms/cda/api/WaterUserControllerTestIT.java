/*
 *
 * MIT License
 *
 * Copyright (c) 2024 Hydrologic Engineering Center
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
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
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE
 * SOFTWARE.
 */

package cwms.cda.api;

import static cwms.cda.api.Controllers.*;
import static cwms.cda.data.dao.DaoTest.getDslContext;
import static cwms.cda.security.KeyAccessManager.AUTH_HEADER;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import cwms.cda.api.enums.Nation;
import cwms.cda.data.dao.DeleteRule;
import cwms.cda.data.dao.LocationsDaoImpl;
import cwms.cda.data.dao.project.ProjectDao;
import cwms.cda.data.dto.Location;
import cwms.cda.data.dto.project.Project;
import cwms.cda.data.dto.watersupply.WaterUser;
import cwms.cda.data.dto.watersupply.WaterUserContract;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.json.JsonV1;
import fixtures.CwmsDataApiSetupCallback;
import fixtures.TestAccounts;
import io.restassured.filter.log.LogDetail;
import mil.army.usace.hec.test.database.CwmsDatabaseContainer;
import org.apache.commons.io.IOUtils;
import org.jooq.DSLContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;


@Tag("integration")
class WaterUserControllerTestIT extends DataApiTestIT {
    private static final String OFFICE_ID = "SWT";
    private static final WaterUserContract CONTRACT;
    static {
        try (InputStream userStream
                     = WaterUserContract.class.getResourceAsStream("/cwms/cda/api/waterusercontract.json")) {
            assert userStream != null;
            String contractJson = IOUtils.toString(userStream, StandardCharsets.UTF_8);
            CONTRACT = Formats.parseContent(new ContentType(Formats.JSONV1), contractJson, WaterUserContract.class);
        } catch(Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @BeforeAll
    static void setup() throws Exception {

        // Create test locations and project
        Location contractLocation = new Location.Builder(CONTRACT.getContractId().getOfficeId(),
                CONTRACT.getContractId().getName()).withLocationKind("PROJECT")
                .withTimeZoneName(ZoneId.of("UTC"))
                .withHorizontalDatum("WGS84").withLongitude(78.0).withLatitude(67.9).withVerticalDatum("WGS84")
                .withLongName("TEST CONTRACT LOCATION").withActive(true).withMapLabel("LABEL").withNation(Nation.US)
                .withElevation(456.7).withElevationUnits("m").withPublishedLongitude(78.9).withPublishedLatitude(45.3)
                .withLocationType("PROJECT").withDescription("TEST PROJECT").build();
        Location parentLocation = new Location.Builder(CONTRACT.getWaterUser().getProjectId().getOfficeId(),
                CONTRACT.getWaterUser().getProjectId().getName()).withLocationKind("PROJECT")
                .withTimeZoneName(ZoneId.of("UTC")).withHorizontalDatum("WGS84")
                .withLongitude(38.0).withLatitude(56.5).withVerticalDatum("WGS84")
                .withLongName("TEST CONTRACT LOCATION").withActive(true).withMapLabel("LABEL").withNation(Nation.US)
                .withElevation(456.7).withElevationUnits("m").withPublishedLongitude(78.9).withPublishedLatitude(45.3)
                .withLocationType("PROJECT").withDescription("TEST PROJECT").build();

        Project project = new Project.Builder().withLocation(parentLocation)
                .withFederalCost(BigDecimal.valueOf(123456789))
                .withAuthorizingLaw("NEW LAW").withCostUnit("$")
                .withProjectOwner(CONTRACT.getWaterUser().getEntityName())
                .build();

        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext ctx = getDslContext(c, OFFICE_ID);
            LocationsDaoImpl locationsDao = new LocationsDaoImpl(ctx);
            ProjectDao projectDao = new ProjectDao(ctx);
            try {
                locationsDao.storeLocation(contractLocation);
                locationsDao.storeLocation(parentLocation);
                projectDao.store(project, true);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    @AfterAll
    static void cleanup() throws Exception {

        Location contractLocation = new Location.Builder(CONTRACT.getContractId().getOfficeId(),
                CONTRACT.getContractId().getName()).withLocationKind("PROJECT")
                .withTimeZoneName(ZoneId.of("UTC"))
                .withHorizontalDatum("WGS84").withLongitude(78.0).withLatitude(67.9).build();
        Location parentLocation = new Location.Builder(CONTRACT.getWaterUser().getProjectId().getOfficeId(),
                CONTRACT.getWaterUser().getProjectId().getName()).withLocationKind("PROJECT")
                .withTimeZoneName(ZoneId.of("UTC")).withHorizontalDatum("WGS84")
                .withLongitude(38.0).withLatitude(56.5).build();

        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext ctx = getDslContext(c, OFFICE_ID);
            LocationsDaoImpl locationsDao = new LocationsDaoImpl(ctx);
            ProjectDao projectDao = new ProjectDao(ctx);
            projectDao.delete(CONTRACT.getOfficeId(), CONTRACT.getWaterUser().getProjectId().getName(),
                    DeleteRule.DELETE_ALL);
            locationsDao.deleteLocation(contractLocation.getName(), contractLocation.getOfficeId(), true);
            locationsDao.deleteLocation(parentLocation.getName(), parentLocation.getOfficeId(), true);
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    @Test
    void test_create_get_delete_WaterUser() throws Exception {
        // Test Structure
        // 1) Create a WaterUser
        // 2) Get the WaterUser, assert that it exists
        // 3) Delete the WaterUser
        // 4) Get the WaterUser, assert that it does not exist

        TestAccounts.KeyUser user = TestAccounts.KeyUser.SWT_NORMAL;
        String json = JsonV1.buildObjectMapper().writeValueAsString(CONTRACT.getWaterUser());

        // create WaterUser
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.JSONV1)
            .accept(Formats.JSONV1)
            .body(json)
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/projects/" + OFFICE_ID + "/" + CONTRACT.getWaterUser().getProjectId().getName() + "/water-user")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED))
        ;

        // get WaterUser, assert that it is correct
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.JSONV1)
            .accept(Formats.JSONV1)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(LOCATION_ID, CONTRACT.getWaterUser().getProjectId().getName())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/projects/" + OFFICE_ID + "/" + CONTRACT.getWaterUser().getProjectId().getName()
                    + "/water-user/" + CONTRACT.getWaterUser().getEntityName())
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("entity-name", equalTo(CONTRACT.getWaterUser().getEntityName()))
            .body("project-id.name", equalTo(CONTRACT.getWaterUser().getProjectId().getName()))
            .body("project-id.office-id", equalTo(CONTRACT.getWaterUser().getProjectId().getOfficeId()))
            .body("water-right", equalTo(CONTRACT.getWaterUser().getWaterRight()))
        ;

        // delete WaterUser
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.JSONV1)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(LOCATION_ID, CONTRACT.getWaterUser().getProjectId().getName())
            .queryParam(DELETE_MODE, "DELETE ALL")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("/projects/" + OFFICE_ID + "/" + CONTRACT.getWaterUser().getProjectId().getName()
                    + "/water-user/" + CONTRACT.getWaterUser().getEntityName())
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NO_CONTENT))
        ;

        // get water user and assert that it does not exist
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.JSONV1)
            .accept(Formats.JSONV1)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(LOCATION_ID, CONTRACT.getWaterUser().getProjectId().getName())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/projects/" + OFFICE_ID + "/" + CONTRACT.getWaterUser().getProjectId().getName()
                    + "/water-user/" + CONTRACT.getWaterUser().getEntityName())
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NOT_FOUND))
        ;
    }

    @Test
    void test_rename_WaterUser() throws Exception {
        // Test Structure
        // 1) Create WaterUser
        // 2) Rename WaterUser
        // 3) Get WaterUser, assert name has changed
        // 4) Delete WaterUser

        TestAccounts.KeyUser user = TestAccounts.KeyUser.SWT_NORMAL;
        String json = JsonV1.buildObjectMapper().writeValueAsString(CONTRACT.getWaterUser());

        // Create WaterUser
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.JSONV1)
            .body(json)
            .accept(Formats.JSONV1)
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/projects/" + OFFICE_ID + "/" + CONTRACT.getWaterUser().getProjectId().getName()
                    + "/water-user")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED))
        ;

        // Rename WaterUser
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.JSONV1)
            .body(json)
            .accept(Formats.JSONV1)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(LOCATION_ID, CONTRACT.getWaterUser().getProjectId().getName())
            .queryParam(NAME, "NEW USER NAME")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .patch("/projects/" + OFFICE_ID + "/" + CONTRACT.getWaterUser().getProjectId().getName()
                    + "/water-user/" + CONTRACT.getWaterUser().getEntityName())
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NO_CONTENT))
        ;

        // Get WaterUser, assert name has changed
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.JSONV1)
            .accept(Formats.JSONV1)
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/projects/" + OFFICE_ID + "/" + CONTRACT.getWaterUser().getProjectId().getName()
                    + "/water-user/" + "NEW USER NAME")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("entity-name", equalTo("NEW USER NAME"))
        ;

        // get old water user and assert that it does not exist
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.JSONV1)
            .accept(Formats.JSONV1)
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/projects/" + OFFICE_ID + "/" + CONTRACT.getWaterUser().getProjectId().getName()
                    + "/water-user/" + CONTRACT.getWaterUser().getEntityName())
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NOT_FOUND))
        ;

        // delete WaterUser
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.JSONV1)
            .accept(Formats.JSONV1)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(LOCATION_ID, CONTRACT.getWaterUser().getProjectId().getName())
            .queryParam(DELETE_MODE, "DELETE ALL")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("/projects/" + OFFICE_ID + "/" + CONTRACT.getWaterUser().getProjectId().getName()
                    + "/water-user/" + CONTRACT.getWaterUser().getEntityName())
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NO_CONTENT))
        ;

    }

    @Test
    void test_getAllWaterUsers() throws Exception {
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SWT_NORMAL;
        String json = JsonV1.buildObjectMapper().writeValueAsString(CONTRACT.getWaterUser());

        WaterUser waterUser = new WaterUser.Builder().withEntityName("ENTITY_NAME")
                .withProjectId(CONTRACT.getWaterUser().getProjectId()).withWaterRight("WATER_RIGHT").build();
        String json2 = JsonV1.buildObjectMapper().writeValueAsString(waterUser);

        // Create WaterUser
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.JSONV1)
            .body(json)
            .accept(Formats.JSONV1)
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/projects/" + OFFICE_ID + "/" + CONTRACT.getWaterUser().getProjectId().getName() + "/water-user")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED))
        ;

        // Create WaterUser
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.JSONV1)
            .body(json2)
            .accept(Formats.JSONV1)
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/projects/" + OFFICE_ID + "/" + CONTRACT.getWaterUser().getProjectId().getName() + "/water-user")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED))
        ;

        // get water users
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV1)
            .header(AUTH_HEADER, TestAccounts.KeyUser.SWT_NORMAL.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/projects/" + OFFICE_ID + "/" + CONTRACT.getWaterUser().getProjectId().getName() + "/water-user")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("[0].entity-name", equalTo(waterUser.getEntityName()))
            .body("[0].project-id.name", equalTo(waterUser.getProjectId().getName()))
            .body("[0].project-id.office-id", equalTo(waterUser.getProjectId().getOfficeId()))
            .body("[0].water-right", equalTo(waterUser.getWaterRight()))
            .body("[1].entity-name", equalTo(CONTRACT.getWaterUser().getEntityName()))
            .body("[1].project-id.name", equalTo(CONTRACT.getWaterUser().getProjectId().getName()))
            .body("[1].project-id.office-id", equalTo(CONTRACT.getWaterUser().getProjectId().getOfficeId()))
            .body("[1].water-right", equalTo(CONTRACT.getWaterUser().getWaterRight()))
        ;

        // delete WaterUser
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.JSONV1)
            .accept(Formats.JSONV1)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(LOCATION_ID, CONTRACT.getWaterUser().getProjectId().getName())
            .queryParam(DELETE_MODE, "DELETE ALL")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("/projects/" + OFFICE_ID + "/" + CONTRACT.getWaterUser().getProjectId().getName()
                    + "/water-user/" + CONTRACT.getWaterUser().getEntityName())
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NO_CONTENT))
        ;
    }
}
