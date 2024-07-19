/*
 * MIT License
 *
 * Copyright (c) 2024 Hydrologic Engineering Center
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
import cwms.cda.data.dao.DeleteRule;
import cwms.cda.data.dao.LocationsDaoImpl;
import cwms.cda.data.dao.location.kind.LocationUtil;
import cwms.cda.data.dto.Location;
import cwms.cda.data.dto.location.kind.Embankment;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import fixtures.CwmsDataApiSetupCallback;
import fixtures.TestAccounts;
import io.restassured.filter.log.LogDetail;
import mil.army.usace.hec.test.database.CwmsDatabaseContainer;
import org.apache.commons.io.IOUtils;
import org.jooq.DSLContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import usace.cwms.db.jooq.codegen.packages.CWMS_PROJECT_PACKAGE;
import usace.cwms.db.jooq.codegen.udt.records.PROJECT_OBJ_T;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Instant;

import static cwms.cda.api.Controllers.FAIL_IF_EXISTS;
import static cwms.cda.data.dao.DaoTest.getDslContext;
import static cwms.cda.security.KeyAccessManager.AUTH_HEADER;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.*;

final class EmbankmentControllerIT extends DataApiTestIT {
    
    private static final Location PROJECT_LOC;
    private static final Location EMBANKMENT_LOC;
    private static final Embankment EMBANKMENT;
    static {
        try(InputStream projectStream = EmbankmentControllerIT.class.getResourceAsStream("/cwms/cda/api/project_location.json");
            InputStream embankmentStream = EmbankmentControllerIT.class.getResourceAsStream("/cwms/cda/api/embankment.json")) {
            String projectLocJson = IOUtils.toString(projectStream, StandardCharsets.UTF_8);
            PROJECT_LOC = Formats.parseContent(new ContentType(Formats.JSONV1), projectLocJson, Location.class);
            String embankmentJson = IOUtils.toString(embankmentStream, StandardCharsets.UTF_8);
            EMBANKMENT = Formats.parseContent(new ContentType(Formats.JSONV1), embankmentJson, Embankment.class);
            EMBANKMENT_LOC = EMBANKMENT.getLocation();
        } catch(Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @BeforeAll
    public static void setup() throws Exception {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            try {
                DSLContext context = getDslContext(c, databaseLink.getOfficeId());
                LocationsDaoImpl locationsDao = new LocationsDaoImpl(context);
                PROJECT_OBJ_T projectObjT = buildProject();
                CWMS_PROJECT_PACKAGE.call_STORE_PROJECT(context.configuration(), projectObjT, "T");
                locationsDao.storeLocation(EMBANKMENT_LOC);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @AfterAll
    public static void tearDown() throws Exception {

        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, databaseLink.getOfficeId());
            LocationsDaoImpl locationsDao = new LocationsDaoImpl(context);
            try {
                locationsDao.deleteLocation(EMBANKMENT_LOC.getName(), databaseLink.getOfficeId(), true);

            } catch (NotFoundException ex) {
                /* only an error within the tests below. */
            }
            try {
                CWMS_PROJECT_PACKAGE.call_DELETE_PROJECT(context.configuration(), PROJECT_LOC.getName(),
                    DeleteRule.DELETE_ALL.getRule(), databaseLink.getOfficeId());

            } catch (NotFoundException ex) {
                /* only an error within the tests below. */
            }
            try {
                locationsDao.deleteLocation(PROJECT_LOC.getName(), databaseLink.getOfficeId(), true);
            } catch (NotFoundException ex) {
                /* only an error within the tests below. */
            }
        });
    }

    @Test
    void test_get_create_delete() {

        // Structure of test:
        // 1)Create the Embankment
        // 2)Retrieve the Embankment and assert that it exists
        // 3)Delete the Embankment
        // 4)Retrieve the Embankment and assert that it does not exist
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SWT_NORMAL;
        String json = Formats.format(Formats.parseHeader(Formats.JSONV1, Embankment.class), EMBANKMENT);
        //Create the Embankment
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.JSONV1)
            .body(json)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(FAIL_IF_EXISTS, "false")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/projects/embankments/")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED))
        ;
        String office = EMBANKMENT.getLocation().getOfficeId();
        // Retrieve the Embankment and assert that it exists
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV1)
            .queryParam(Controllers.OFFICE, office)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/projects/embankments/" + EMBANKMENT.getLocation().getName())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("upstream-side-slope", equalTo(EMBANKMENT.getUpstreamSideSlope().floatValue()))
            .body("downstream-side-slope", equalTo(EMBANKMENT.getDownstreamSideSlope().floatValue()))
            .body("structure-length", equalTo(EMBANKMENT.getStructureLength().floatValue()))
            .body("max-height", equalTo(EMBANKMENT.getMaxHeight().floatValue()))
            .body("top-width", equalTo(EMBANKMENT.getTopWidth().floatValue()))
            .body("length-units", equalTo(EMBANKMENT.getLengthUnits()))
            .body("downstream-protection-type", not(nullValue()))
            .body("upstream-protection-type", not(nullValue()))
            .body("structure-type", not(nullValue()))
            .body("location", not(nullValue()))
            .body("project-id.name", equalTo(EMBANKMENT.getProjectId().getName()))
            .body("project-id.office-id", equalTo(EMBANKMENT.getProjectId().getOfficeId()))
        ;

        // Delete a Embankment
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .queryParam(Controllers.OFFICE, office)
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("/projects/embankments/" + EMBANKMENT.getLocation().getName())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NO_CONTENT))
        ;

        // Retrieve a Embankment and assert that it does not exist
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV1)
            .queryParam(Controllers.OFFICE, office)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/projects/embankments/" + EMBANKMENT.getLocation().getName())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NOT_FOUND))
        ;
    }

    @Test
    void test_update_does_not_exist() {
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SWT_NORMAL;
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .queryParam(Controllers.OFFICE, user.getOperatingOffice())
            .queryParam(Controllers.NAME, "NewBogus")
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .patch("/projects/embankments/bogus")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NOT_FOUND))
        ;
    }

    @Test
    void test_delete_does_not_exist() {
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SWT_NORMAL;
        // Delete a Embankment
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .queryParam(Controllers.OFFICE, user.getOperatingOffice())
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("/projects/embankments/" + Instant.now().toEpochMilli())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NOT_FOUND))
        ;
    }

    @Test
    void test_get_all() {

        // Structure of test:
        // 1)Create the Embankment
        // 2)Retrieve the Embankment with getAll and assert that it exists
        // 3)Delete the Embankment
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SWT_NORMAL;
        String json = Formats.format(Formats.parseHeader(Formats.JSONV1, Embankment.class), EMBANKMENT);
        //Create the Embankment
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.JSONV1)
            .body(json)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(FAIL_IF_EXISTS, "false")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/projects/embankments/")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED))
        ;
        String office = EMBANKMENT.getLocation().getOfficeId();
        // Retrieve the Embankment and assert that it exists
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV1)
            .queryParam(Controllers.OFFICE, office)
            .queryParam(Controllers.PROJECT_ID, EMBANKMENT.getProjectId().getName())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/projects/embankments/")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("[0].upstream-side-slope", equalTo(EMBANKMENT.getUpstreamSideSlope().floatValue()))
            .body("[0].downstream-side-slope", equalTo(EMBANKMENT.getDownstreamSideSlope().floatValue()))
            .body("[0].structure-length", equalTo(EMBANKMENT.getStructureLength().floatValue()))
            .body("[0].max-height", equalTo(EMBANKMENT.getMaxHeight().floatValue()))
            .body("[0].top-width", equalTo(EMBANKMENT.getTopWidth().floatValue()))
            .body("[0].length-units", equalTo(EMBANKMENT.getLengthUnits()))
            .body("[0].downstream-protection-type", not(nullValue()))
            .body("[0].upstream-protection-type", not(nullValue()))
            .body("[0].structure-type", not(nullValue()))
            .body("[0].location", not(nullValue()))
            .body("[0].project-id.name", equalTo(EMBANKMENT.getProjectId().getName()))
            .body("[0].project-id.office-id", equalTo(EMBANKMENT.getProjectId().getOfficeId()))
        ;

        // Delete a Embankment
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .queryParam(Controllers.OFFICE, office)
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("/projects/embankments/" + EMBANKMENT.getLocation().getName())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NO_CONTENT))
        ;
    }

    private static PROJECT_OBJ_T buildProject() {
        PROJECT_OBJ_T retval = new PROJECT_OBJ_T();
        retval.setPROJECT_LOCATION(LocationUtil.getLocation(PROJECT_LOC));
        retval.setPUMP_BACK_LOCATION(null);
        retval.setNEAR_GAGE_LOCATION(null);
        retval.setAUTHORIZING_LAW(null);
        retval.setCOST_YEAR(Timestamp.from(Instant.now()));
        retval.setFEDERAL_COST(BigDecimal.ONE);
        retval.setNONFEDERAL_COST(BigDecimal.TEN);
        retval.setFEDERAL_OM_COST(BigDecimal.ZERO);
        retval.setNONFEDERAL_OM_COST(BigDecimal.valueOf(15.0));
        retval.setCOST_UNITS_ID("$");
        retval.setREMARKS("TEST RESERVOIR PROJECT");
        retval.setPROJECT_OWNER("CDA");
        retval.setHYDROPOWER_DESCRIPTION("HYDRO DESCRIPTION");
        retval.setSEDIMENTATION_DESCRIPTION("SEDIMENTATION DESCRIPTION");
        retval.setDOWNSTREAM_URBAN_DESCRIPTION("DOWNSTREAM URBAN DESCRIPTION");
        retval.setBANK_FULL_CAPACITY_DESCRIPTION("BANK FULL CAPACITY DESCRIPTION");
        retval.setYIELD_TIME_FRAME_START(Timestamp.from(Instant.now()));
        retval.setYIELD_TIME_FRAME_END(Timestamp.from(Instant.now()));
        return retval;
    }
}