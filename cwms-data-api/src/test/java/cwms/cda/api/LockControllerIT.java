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

import static cwms.cda.api.Controllers.FAIL_IF_EXISTS;
import static cwms.cda.data.dao.DaoTest.getDslContext;
import static cwms.cda.security.KeyAccessManager.AUTH_HEADER;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

import cwms.cda.api.errors.NotFoundException;
import cwms.cda.data.dao.DeleteRule;
import cwms.cda.data.dao.LocationsDaoImpl;
import cwms.cda.data.dao.location.kind.LocationUtil;
import cwms.cda.data.dto.Location;
import cwms.cda.data.dto.location.kind.Lock;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import fixtures.CwmsDataApiSetupCallback;
import fixtures.TestAccounts;
import io.restassured.filter.log.LogDetail;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Instant;
import javax.servlet.http.HttpServletResponse;
import mil.army.usace.hec.test.database.CwmsDatabaseContainer;
import org.apache.commons.io.IOUtils;
import org.jooq.DSLContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import usace.cwms.db.jooq.codegen.packages.CWMS_PROJECT_PACKAGE;
import usace.cwms.db.jooq.codegen.udt.records.PROJECT_OBJ_T;

final class LockControllerIT extends DataApiTestIT {

    private static final Location PROJECT_LOC;
    private static final Location LOCK_LOC;
    private static final Lock LOCK;

    static {
        try (InputStream projectStream = LockControllerIT.class.getResourceAsStream(
            "/cwms/cda/api/project_location_lock.json");
             InputStream lockStream = LockControllerIT.class.getResourceAsStream("/cwms/cda/api/lock.json")) {
            String projectLocJson = IOUtils.toString(projectStream, StandardCharsets.UTF_8);
            PROJECT_LOC = Formats.parseContent(new ContentType(Formats.JSONV1), projectLocJson, Location.class);
            String lockJson = IOUtils.toString(lockStream, StandardCharsets.UTF_8);
            LOCK = Formats.parseContent(new ContentType(Formats.JSONV1), lockJson, Lock.class);
            LOCK_LOC = LOCK.getLocation();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @BeforeAll
    public static void setup() throws Exception {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            try {
                DSLContext context = getDslContext(c, LOCK_LOC.getOfficeId());
                LocationsDaoImpl locationsDao = new LocationsDaoImpl(context);
                PROJECT_OBJ_T projectObjT = buildProject();
                CWMS_PROJECT_PACKAGE.call_STORE_PROJECT(context.configuration(), projectObjT, "T");
                locationsDao.storeLocation(LOCK_LOC);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    @AfterAll
    public static void tearDown() throws Exception {

        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, LOCK_LOC.getOfficeId());
            LocationsDaoImpl locationsDao = new LocationsDaoImpl(context);
            try {
                locationsDao.deleteLocation(LOCK_LOC.getName(), LOCK_LOC.getOfficeId(), true);

            } catch (NotFoundException ex) {
                /* only an error within the tests below. */
            }
            try {
                CWMS_PROJECT_PACKAGE.call_DELETE_PROJECT(context.configuration(), PROJECT_LOC.getName(),
                    DeleteRule.DELETE_ALL.getRule(), PROJECT_LOC.getOfficeId());

            } catch (NotFoundException ex) {
                /* only an error within the tests below. */
            }
            try {
                locationsDao.deleteLocation(PROJECT_LOC.getName(), PROJECT_LOC.getOfficeId(), true);
            } catch (NotFoundException ex) {
                /* only an error within the tests below. */
            }
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    @Test
    void test_get_create_delete() {

        // Structure of test:
        // 1)Create the Lock
        // 2)Retrieve the Lock and assert that it exists
        // 3)Delete the Lock
        // 4)Retrieve the Lock and assert that it does not exist
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SWT_NORMAL;
        String json = Formats.format(Formats.parseHeader(Formats.JSONV1, Lock.class), LOCK);
        //Create the Lock
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.JSONV1)
            .body(json)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(FAIL_IF_EXISTS, "false")
            .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/projects/locks/")
            .then()
            .log().ifValidationFails(LogDetail.ALL, true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED))
        ;
        String office = LOCK.getLocation().getOfficeId();
        // Retrieve the Lock and assert that it exists
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV1)
            .queryParam(Controllers.OFFICE, office)
            .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/projects/locks/" + LOCK.getLocation().getName())
            .then()
            .log().ifValidationFails(LogDetail.ALL, true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("location", not(nullValue()))
            .body("project-id.name", equalTo(LOCK.getProjectId().getName()))
            .body("project-id.office-id", equalTo(LOCK.getProjectId().getOfficeId()))
        ;

        // Delete a Lock
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .queryParam(Controllers.OFFICE, office)
            .header(AUTH_HEADER, user.toHeaderValue())
            .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("/projects/locks/" + LOCK.getLocation().getName())
            .then()
            .log().ifValidationFails(LogDetail.ALL, true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_NO_CONTENT))
        ;

        // Retrieve a Lock and assert that it does not exist
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV1)
            .queryParam(Controllers.OFFICE, office)
            .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/projects/locks/" + LOCK.getLocation().getName())
            .then()
            .log().ifValidationFails(LogDetail.ALL, true)
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
            .patch("/projects/locks/bogus")
            .then()
            .log().ifValidationFails(LogDetail.ALL, true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_NOT_FOUND))
        ;
    }

    @Test
    void test_delete_does_not_exist() {
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SWT_NORMAL;
        // Delete a Lock
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .queryParam(Controllers.OFFICE, user.getOperatingOffice())
            .header(AUTH_HEADER, user.toHeaderValue())
            .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("/projects/locks/" + Instant.now().toEpochMilli())
            .then()
            .log().ifValidationFails(LogDetail.ALL, true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_NOT_FOUND))
        ;
    }

    @Test
    void test_get_all() {

        // Structure of test:
        // 1)Create the Lock
        // 2)Retrieve the Lock with getAll and assert that it exists
        // 3)Delete the Lock
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SWT_NORMAL;
        String json = Formats.format(Formats.parseHeader(Formats.JSONV1, Lock.class), LOCK);
        //Create the Lock
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.JSONV1)
            .body(json)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(FAIL_IF_EXISTS, "false")
            .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/projects/locks/")
            .then()
            .log().ifValidationFails(LogDetail.ALL, true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED))
        ;
        String office = LOCK.getLocation().getOfficeId();
        // Retrieve the Lock and assert that it exists
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV1)
            .queryParam(Controllers.OFFICE, office)
            .queryParam(Controllers.PROJECT_ID, LOCK.getProjectId().getName())
            .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/projects/locks/")
            .then()
            .log().ifValidationFails(LogDetail.ALL, true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("[0].name", equalTo(LOCK.getLocation().getName()))
            .body("[0].office-id", equalTo(LOCK.getLocation().getOfficeId()))
        ;

        // Delete a Lock
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .queryParam(Controllers.OFFICE, office)
            .header(AUTH_HEADER, user.toHeaderValue())
            .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("/projects/locks/" + LOCK.getLocation().getName())
            .then()
            .log().ifValidationFails(LogDetail.ALL, true)
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