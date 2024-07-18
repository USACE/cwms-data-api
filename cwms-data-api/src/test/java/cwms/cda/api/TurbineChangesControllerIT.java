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
import static cwms.cda.api.Controllers.PROJECT_ID;
import static cwms.cda.data.dao.DaoTest.getDslContext;
import static cwms.cda.security.KeyAccessManager.AUTH_HEADER;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

import cwms.cda.api.errors.NotFoundException;
import cwms.cda.data.dao.DeleteRule;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dao.LocationsDaoImpl;
import cwms.cda.data.dao.location.kind.LocationUtil;
import cwms.cda.data.dao.location.kind.TurbineDao;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.Location;
import cwms.cda.data.dto.location.kind.Turbine;
import cwms.cda.data.dto.location.kind.TurbineChange;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import fixtures.CwmsDataApiSetupCallback;
import fixtures.TestAccounts;
import io.restassured.filter.log.LogDetail;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import javax.servlet.http.HttpServletResponse;
import mil.army.usace.hec.test.database.CwmsDatabaseContainer;
import org.apache.commons.io.IOUtils;
import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import usace.cwms.db.jooq.codegen.packages.CWMS_PROJECT_PACKAGE;
import usace.cwms.db.jooq.codegen.udt.records.PROJECT_OBJ_T;

//* NOTE requires at least 24.05.24-RC03 to fix nation ID issue with store_location_f */
final class TurbineChangesControllerIT extends DataApiTestIT {
    private static final String OFFICE = TestAccounts.KeyUser.SWT_NORMAL.getOperatingOffice();
    private static final Location PROJECT_LOC;
    private static final Location TURBINE_LOC;
    private static final Turbine TURBINE;
    private static final List<TurbineChange> TURBINE_CHANGES;

    static {
        Class<TurbineChangesControllerIT> c = TurbineChangesControllerIT.class;
        Charset utf8 = StandardCharsets.UTF_8;
        ContentType contentType = new ContentType(Formats.JSONV1);
        try(InputStream projectStream = c.getResourceAsStream("/cwms/cda/api/project_location_turb.json");
            InputStream turbineStream = c.getResourceAsStream("/cwms/cda/api/turbine.json");
            InputStream turbineChangesStream = c.getResourceAsStream("/cwms/cda/api/turbine-changes.json")) {
            String json = IOUtils.toString(Objects.requireNonNull(projectStream), utf8);
            PROJECT_LOC = Formats.parseContent(contentType, json, Location.class);
            json = IOUtils.toString(Objects.requireNonNull(turbineStream), utf8);
            TURBINE = Formats.parseContent(contentType, json, Turbine.class);
            TURBINE_LOC = TURBINE.getLocation();
            json = IOUtils.toString(Objects.requireNonNull(turbineChangesStream), utf8);
            TURBINE_CHANGES = Formats.parseContentList(contentType, json, TurbineChange.class);
        } catch(Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @BeforeAll
    public static void setup() throws Exception {
        tearDown();
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            try {
                DSLContext context = getDslContext(c, OFFICE);
                LocationsDaoImpl locationsDao = new LocationsDaoImpl(context);
                PROJECT_OBJ_T projectObjT = buildProject();
                CWMS_PROJECT_PACKAGE.call_STORE_PROJECT(context.configuration(), projectObjT, "T");
                locationsDao.storeLocation(TURBINE_LOC);
                Turbine turbine = new Turbine.Builder()
                    .withProjectId(new CwmsId.Builder()
                        .withOfficeId(PROJECT_LOC.getOfficeId())
                        .withName(PROJECT_LOC.getName())
                        .build())
                    .withLocation(TURBINE_LOC)
                    .build();
                new TurbineDao(context).storeTurbine(turbine, false);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        },
        CwmsDataApiSetupCallback.getWebUser());
    }

    @AfterAll
    public static void tearDown() throws Exception {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, OFFICE);
            cleanTurbine(context, TURBINE_LOC);
            cleanProject(context, PROJECT_LOC);
        },
        CwmsDataApiSetupCallback.getWebUser());
    }

    @Test
    void test_get_create_delete() {

        // Structure of test:
        // 1)Create the Turbine Changes
        // 2)Retrieve the Turbine and assert that it exists
        // 3)Delete the Turbine Changes
        // 4)Retrieve the Turbine Changes and assert that they do not exist
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SWT_NORMAL;
        String json = Formats.format(Formats.parseHeader(Formats.JSONV1, TurbineChange.class), TURBINE_CHANGES,
            TurbineChange.class);
        //Create the Turbine Changes
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.JSONV1)
            .body(json)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(FAIL_IF_EXISTS, "false")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("projects/" + PROJECT_LOC.getOfficeId() + "/" + PROJECT_LOC.getName() + "/turbine-changes/")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED))
        ;
        String office = TURBINE.getLocation().getOfficeId();

        // Retrieve the Turbine Changes and assert that they exists
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV1)
            .queryParam(Controllers.OFFICE, office)
            .queryParam(Controllers.BEGIN, "2024-03-04T08:00:00Z")
            .queryParam(Controllers.END, "2024-03-04T10:00:00Z")
            .queryParam(Controllers.START_TIME_INCLUSIVE, "true")
            .queryParam(Controllers.END_TIME_INCLUSIVE, "true")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("projects/" + PROJECT_LOC.getOfficeId() + "/" + PROJECT_LOC.getName() + "/turbine-changes/")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("[0].project-id", not(nullValue()))
            .body("[0].change-date", equalTo(1709539200000L))
            .body("[2].settings[0].type", equalTo("turbine-setting"))
            .body("[2].settings[0].location-id", not(nullValue()))
            .body("[2].settings[0].generation-units", equalTo("MW"))
        ;

        // Delete the Turbine Changes
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .queryParam(Controllers.OFFICE, office)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(Controllers.BEGIN, "2024-03-04T08:00:00Z")
            .queryParam(Controllers.END, "2024-03-04T10:00:00Z")
            .queryParam(Controllers.OVERRIDE_PROTECTION, "true")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("projects/" + PROJECT_LOC.getOfficeId() + "/" + PROJECT_LOC.getName() + "/turbine-changes/")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NO_CONTENT))
        ;


        // Retrieve the Turbine Changes and assert that they no longer exist
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV1)
            .queryParam(Controllers.OFFICE, office)
            .queryParam(Controllers.BEGIN, "2024-03-04T08:00:00Z")
            .queryParam(Controllers.END, "2024-03-04T10:00:00Z")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("projects/" + PROJECT_LOC.getOfficeId() + "/" + PROJECT_LOC.getName() + "/turbine-changes/")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("isEmpty()", is(true))
        ;
    }

    @Test
    void test_delete_does_not_exist() {
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SWT_NORMAL;
        // Delete a Turbine
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .queryParam(Controllers.OFFICE, user.getOperatingOffice())
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("projects/" + Instant.now().toEpochMilli() + "turbine-changes/")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NOT_FOUND))
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

    private static void cleanTurbine(DSLContext context, Location turbine) {
        try {
            new TurbineDao(context).deleteTurbine(turbine.getName(), OFFICE, DeleteRule.DELETE_ALL);
        } catch (NotFoundException ex) {
            /* this is only an error within the tests themselves */
        }

        try {
            new LocationsDaoImpl(context).deleteLocation(turbine.getName(), OFFICE, true);
        } catch (NotFoundException ex) {
            /* this is only an error within the tests themselves */
        }
    }

    private static void cleanProject(DSLContext context, Location project) {
        try {
            CWMS_PROJECT_PACKAGE.call_DELETE_PROJECT(context.configuration(), project.getName(),
                DeleteRule.DELETE_ALL.getRule(), OFFICE);
        } catch (DataAccessException ex) {
            if (!JooqDao.isNotFound(ex)) {
                throw ex;
            }
        } catch (NotFoundException ex) {
            /* this is only an error within the tests themselves */
        }

        try {
            new LocationsDaoImpl(context).deleteLocation(project.getName(), OFFICE, true);
        } catch (NotFoundException ex) {
            /* this is only an error within the tests themselves */
        }
    }
}