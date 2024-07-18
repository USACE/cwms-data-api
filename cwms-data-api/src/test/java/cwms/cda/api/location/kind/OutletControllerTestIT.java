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

import cwms.cda.api.Controllers;
import cwms.cda.data.dao.DeleteRule;
import cwms.cda.data.dao.LocationsDaoImpl;
import cwms.cda.data.dao.location.kind.OutletDao;
import cwms.cda.data.dao.location.kind.ProjectStructureIT;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.Location;
import cwms.cda.data.dto.location.kind.Outlet;
import cwms.cda.formatters.Formats;
import fixtures.CwmsDataApiSetupCallback;
import fixtures.TestAccounts;
import io.restassured.filter.log.LogDetail;
import java.io.IOException;
import javax.servlet.http.HttpServletResponse;
import mil.army.usace.hec.test.database.CwmsDatabaseContainer;
import org.jooq.DSLContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import static cwms.cda.api.Controllers.FAIL_IF_EXISTS;
import static cwms.cda.data.dao.DaoTest.getDslContext;
import static cwms.cda.security.KeyAccessManager.AUTH_HEADER;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.*;

class OutletControllerTestIT extends ProjectStructureIT {
    private static final TestAccounts.KeyUser USER = TestAccounts.KeyUser.SPK_NORMAL;
    private static final String CONDUIT_GATE_RATING_GROUP = "Rating-" + PROJECT_1_ID.getName() + "-ConduitGate";
    private static final String OUTLET_KIND = "OUTLET";
    private static final Location CONDUIT_GATE_1_LOC = buildProjectStructureLocation(PROJECT_1_ID.getName() + "-CG10",
                                                                                     OUTLET_KIND);
    private static final Outlet CONDUIT_GATE_1_OUTLET = buildTestOutlet(CONDUIT_GATE_1_LOC, PROJECT_LOC,
                                                                        CONDUIT_GATE_RATING_GROUP);

    private static final Location EXISTING_CONDUIT_GATE = buildProjectStructureLocation(PROJECT_1_ID.getName() + "-CG20",
                                                                                        OUTLET_KIND);
    private static final Outlet EXISTING_CONDUIT_GATE_OUTLET = buildTestOutlet(EXISTING_CONDUIT_GATE, PROJECT_LOC,
                                                                               CONDUIT_GATE_RATING_GROUP);

    @BeforeAll
    public static void setup() throws Exception {
        setupProject();
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, OFFICE_ID);
            LocationsDaoImpl locationsDao = new LocationsDaoImpl(context);
            OutletDao outletDao = new OutletDao(context);
            try {
                locationsDao.storeLocation(EXISTING_CONDUIT_GATE);
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
            LocationsDaoImpl locationsDao = new LocationsDaoImpl(context);
            OutletDao outletDao = new OutletDao(context);

            outletDao.deleteOutlet(EXISTING_CONDUIT_GATE.getOfficeId(), EXISTING_CONDUIT_GATE.getName(),
                                   DeleteRule.DELETE_ALL);
            locationsDao.deleteLocation(EXISTING_CONDUIT_GATE.getName(), EXISTING_CONDUIT_GATE.getOfficeId(), true);
        }, CwmsDataApiSetupCallback.getWebUser());
        tearDownProject();
    }

    @Test
    void test_outlet_get_all() {
        //Get the newly created outlet
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV1)
            .queryParam(Controllers.OFFICE, OFFICE_ID)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("projects/" + OFFICE_ID + "/" + PROJECT_1_ID.getName() + "/outlets")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("location", not(nullValue()))
            .body("project-id.name", equalTo(CONDUIT_GATE_1_OUTLET.getProjectId().getName()))
            .body("project-id.office-id", equalTo(CONDUIT_GATE_1_OUTLET.getProjectId().getOfficeId()));
    }

    @Test
    void test_outlet_crud() {
        // Structure of test:
        // 1)Create the Outlet - TG3 does not exist in the db.
        // 2)Retrieve the Outlet and assert that it exists
        // 3)Delete the Outlet
        // 4)Retrieve the Outlet and assert that it does not exist
        String json = Formats.format(Formats.parseHeader(Formats.JSONV1, Outlet.class), CONDUIT_GATE_1_OUTLET);

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
            .get("projects/" + OFFICE_ID + "/outlets/" + CONDUIT_GATE_1_OUTLET.getLocation().getName())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("location", not(nullValue()))
            .body("project-id.name", equalTo(CONDUIT_GATE_1_OUTLET.getProjectId().getName()))
            .body("project-id.office-id", equalTo(CONDUIT_GATE_1_OUTLET.getProjectId().getOfficeId()));

        //Delete the Outlet
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .queryParam(Controllers.OFFICE, OFFICE_ID)
            .header(AUTH_HEADER, USER.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("projects/" + OFFICE_ID + "/outlets/" + CONDUIT_GATE_1_OUTLET.getLocation().getName())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_NO_CONTENT));

        //Re-retrieve, and ensure it doesn't exist anymore.
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV1)
            .queryParam(Controllers.OFFICE, OFFICE_ID)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("projects/" + OFFICE_ID + "/outlets/" + CONDUIT_GATE_1_OUTLET.getLocation().getName())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_NOT_FOUND));
    }

    private static Outlet buildTestOutlet(Location outletLoc, Location projectLoc, String ratingId) {
        return new Outlet.Builder().withProjectId(new CwmsId.Builder().withName(projectLoc.getName())
                                                                      .withOfficeId(projectLoc.getOfficeId())
                                                                      .build())
                                   .withLocation(outletLoc)
                                   .withRatingGroupId(ratingId)
                                   .build();
    }
}