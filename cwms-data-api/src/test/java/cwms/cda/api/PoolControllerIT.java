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

import static cwms.cda.api.Controllers.*;
import static cwms.cda.data.dao.DaoTest.getDslContext;
import static cwms.cda.security.ApiKeyIdentityProvider.AUTH_HEADER;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import cwms.cda.api.errors.NotFoundException;
import cwms.cda.data.dao.DeleteRule;
import cwms.cda.data.dao.LocationLevelsDaoImpl;
import cwms.cda.data.dao.LocationsDaoImpl;
import cwms.cda.data.dao.location.kind.LocationUtil;
import cwms.cda.data.dto.Location;
import cwms.cda.data.dto.Pool;
import cwms.cda.data.dto.locationlevel.ConstantLocationLevel;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import fixtures.CwmsDataApiSetupCallback;
import fixtures.TestAccounts;
import io.restassured.filter.log.LogDetail;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import javax.servlet.http.HttpServletResponse;
import mil.army.usace.hec.test.database.CwmsDatabaseContainer;
import org.apache.commons.io.IOUtils;
import org.jooq.DSLContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import usace.cwms.db.jooq.codegen.packages.CWMS_PROJECT_PACKAGE;
import usace.cwms.db.jooq.codegen.udt.records.PROJECT_OBJ_T;

final class PoolControllerIT extends DataApiTestIT {

    private static final Location PROJECT_LOC;
    private static final Pool POOL;
    private static final String OFFICE_ID = "office-id";
    private static final String MESSAGE = "message";
    private static final String IDENTIFIER = "identifier";

    static {
        try (InputStream projectStream = PoolControllerIT.class.getResourceAsStream(
              "/cwms/cda/api/project_pool.json");
             InputStream poolStream = PoolControllerIT.class.getResourceAsStream("/cwms/cda/api/pool.json")) {
            String projectLocJson = IOUtils.toString(projectStream, StandardCharsets.UTF_8);
            PROJECT_LOC = Formats.parseContent(new ContentType(Formats.JSONV2), projectLocJson, Location.class);
            String poolJson = IOUtils.toString(poolStream, StandardCharsets.UTF_8);
            POOL = Formats.parseContent(new ContentType(Formats.JSONV2), poolJson, Pool.class);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @BeforeAll
    static void setup() throws Exception {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, PROJECT_LOC.getOfficeId());
            PROJECT_OBJ_T projectObjT = buildProject();
            CWMS_PROJECT_PACKAGE.call_STORE_PROJECT(context.configuration(), projectObjT, "T");
            LocationLevelsDaoImpl dao = new LocationLevelsDaoImpl(context);
            String locationId = PROJECT_LOC.getName();
            dao.storeLocationLevel(new ConstantLocationLevel.Builder(locationId + "." + POOL.getBottomLevelId(),
                  Instant.now().truncatedTo(ChronoUnit.HOURS)).withConstantValue(100.0)
                        .build());
            dao.storeLocationLevel(new ConstantLocationLevel.Builder(locationId + "." + POOL.getTopLevelId(),
                  Instant.now().truncatedTo(ChronoUnit.HOURS)).withConstantValue(300.0)
                  .build());
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    @AfterAll
    static void tearDown() throws Exception {

        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext context = getDslContext(c, PROJECT_LOC.getOfficeId());
            LocationsDaoImpl locationsDao = new LocationsDaoImpl(context);
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

    @ParameterizedTest
    @ValueSource(strings = {Formats.JSONV2, Formats.DEFAULT})
    void test_get_create_delete(String format) {

        // Structure of test:
        // 1)Create the Pool
        // 2)Retrieve the Pool and assert that it exists
        // 3)Delete the Pool
        // 4)Retrieve the Pool and assert that it does not exist
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SWT_NORMAL;
        String json = Formats.format(Formats.parseHeader(Formats.JSONV2, Pool.class), POOL);
        //Create the Pool
        given()
              .log().ifValidationFails(LogDetail.ALL, true)
              .contentType(Formats.JSONV2)
              .body(json)
              .header(AUTH_HEADER, user.toHeaderValue())
              .queryParam(FAIL_IF_EXISTS, "false")
        .when()
              .redirects().follow(true)
              .redirects().max(3)
              .post("/pools/")
        .then()
              .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
              .statusCode(is(HttpServletResponse.SC_CREATED))
              .body(OFFICE_ID, equalTo(POOL.getPoolName().getOfficeId()))
              .body(MESSAGE, notNullValue())
              .body(IDENTIFIER, equalTo(POOL.getPoolName().getPoolName()))
        ;
        String office = POOL.getPoolName().getOfficeId();
        // Retrieve the Pool and assert that it exists
        given()
              .log().ifValidationFails(LogDetail.ALL, true)
              .accept(format)
              .queryParam(PROJECT_ID, POOL.getProjectId())
              .queryParam(OFFICE, POOL.getPoolName().getOfficeId())
        .when()
              .redirects().follow(true)
              .redirects().max(3)
              .get("/pools/" + POOL.getPoolName().getPoolName())
        .then()
              .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
              .statusCode(is(HttpServletResponse.SC_OK))
              .body("attribute", equalTo(POOL.getAttribute()))
              .body("bottom-level-id", equalTo(POOL.getBottomLevelId()))
              .body("top-level-id", equalTo(POOL.getTopLevelId()))
              .body("clob-text", equalTo(POOL.getClobText()))
              .body("description", equalTo(POOL.getDescription()))
              .body("project-id", equalTo(POOL.getProjectId()))
              .body("pool-name.pool-name", equalTo(POOL.getPoolName().getPoolName()))
              .body("pool-name.office-id", equalTo(POOL.getPoolName().getOfficeId()))
        ;

        // Delete a Pool
        given()
              .log().ifValidationFails(LogDetail.ALL, true)
              .queryParam(OFFICE, office)
              .queryParam(PROJECT_ID, POOL.getProjectId())
              .header(AUTH_HEADER, user.toHeaderValue())
        .when()
              .redirects().follow(true)
              .redirects().max(3)
              .delete("/pools/" + POOL.getPoolName().getPoolName())
        .then()
              .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
              .statusCode(is(HttpServletResponse.SC_NO_CONTENT))
        ;

        // Retrieve a Pool and assert that it does not exist
        given()
              .log().ifValidationFails(LogDetail.ALL, true)
              .accept(format)
              .queryParam(PROJECT_ID, POOL.getProjectId())
              .queryParam(OFFICE, POOL.getPoolName().getOfficeId())
        .when()
              .redirects().follow(true)
              .redirects().max(3)
              .get("/pools/" + POOL.getPoolName().getPoolName())
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
              .queryParam(OFFICE, user.getOperatingOffice())
              .queryParam(PROJECT_ID, PROJECT_LOC.getName())
              .queryParam(NAME, "NewBogus")
              .header(AUTH_HEADER, user.toHeaderValue())
        .when()
              .redirects().follow(true)
              .redirects().max(3)
              .patch("/pools/bogus")
        .then()
              .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
              .statusCode(is(HttpServletResponse.SC_NOT_FOUND))
        ;
    }

    @Test
    void test_delete_does_not_exist() {
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SWT_NORMAL;
        // Delete a Pool
        given()
              .log().ifValidationFails(LogDetail.ALL, true)
              .queryParam(OFFICE, user.getOperatingOffice())
              .queryParam(PROJECT_ID, PROJECT_LOC.getName())
              .header(AUTH_HEADER, user.toHeaderValue())
        .when()
              .redirects().follow(true)
              .redirects().max(3)
              .delete("/pools/" + Instant.now().toEpochMilli())
        .then()
              .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
              .statusCode(is(HttpServletResponse.SC_NOT_FOUND))
        ;
    }

    @ParameterizedTest
    @ValueSource(strings = {Formats.JSONV2, Formats.DEFAULT})
    void test_get_all(String format) {

        // Structure of test:
        // 1)Create the Pool
        // 2)Retrieve the Pool with getAll and assert that it exists
        // 3)Delete the Pool
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SWT_NORMAL;
        String json = Formats.format(Formats.parseHeader(Formats.JSONV2, Pool.class), POOL);
        //Create the Pool
        given()
              .log().ifValidationFails(LogDetail.ALL, true)
              .contentType(Formats.JSONV2)
              .body(json)
              .header(AUTH_HEADER, user.toHeaderValue())
              .queryParam(FAIL_IF_EXISTS, "false")
        .when()
              .redirects().follow(true)
              .redirects().max(3)
              .post("/pools/")
        .then()
              .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
              .statusCode(is(HttpServletResponse.SC_CREATED))
              .body(OFFICE_ID, equalTo(POOL.getPoolName().getOfficeId()))
              .body(MESSAGE, notNullValue())
              .body(IDENTIFIER, equalTo(POOL.getPoolName().getPoolName()))
        ;
        String office = POOL.getPoolName().getOfficeId();
        // Retrieve the Pool and assert that it exists
        given()
              .log().ifValidationFails(LogDetail.ALL, true)
              .accept(format)
              .queryParam(INCLUDE_EXPLICIT, "true")
              .queryParam(INCLUDE_IMPLICIT, "true")
              .queryParam(ID_MASK, POOL.getProjectId())
              .queryParam(OFFICE, POOL.getPoolName().getOfficeId())
        .when()
              .redirects().follow(true)
              .redirects().max(3)
              .get("/pools/")
        .then()
              .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
              .statusCode(is(HttpServletResponse.SC_OK))
              .body("pools[0].attribute", equalTo(POOL.getAttribute()))
              .body("pools[0].bottom-level-id", equalTo(POOL.getBottomLevelId()))
              .body("pools[0].top-level-id", equalTo(POOL.getTopLevelId()))
              .body("pools[0].clob-text", equalTo(POOL.getClobText()))
              .body("pools[0].description", equalTo(POOL.getDescription()))
              .body("pools[0].project-id", equalTo(POOL.getProjectId()))
              .body("pools[0].pool-name.pool-name", equalTo(POOL.getPoolName().getPoolName()))
              .body("pools[0].pool-name.office-id", equalTo(POOL.getPoolName().getOfficeId()))
        ;

        // Delete a Pool
        given()
              .log().ifValidationFails(LogDetail.ALL, true)
              .queryParam(OFFICE, office)
              .queryParam(PROJECT_ID, POOL.getProjectId())
              .header(AUTH_HEADER, user.toHeaderValue())
        .when()
              .redirects().follow(true)
              .redirects().max(3)
              .delete("/pools/" + POOL.getPoolName().getPoolName())
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