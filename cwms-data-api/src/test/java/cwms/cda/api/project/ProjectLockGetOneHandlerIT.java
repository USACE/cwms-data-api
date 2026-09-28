/*
 * MIT License
 *
 * Copyright (c) 2026 Hydrologic Engineering Center
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

package cwms.cda.api.project;


import static cwms.cda.api.Controllers.APPLICATION_ID;
import static cwms.cda.api.project.ProjectLockHandlerUtil.buildTestProject;
import static cwms.cda.api.project.ProjectLockHandlerUtil.deleteProject;
import static cwms.cda.api.project.ProjectLockHandlerUtil.revokeLock;
import static cwms.cda.data.dao.DaoTest.getDslContext;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import cwms.cda.api.Controllers;
import cwms.cda.api.DataApiTestIT;
import cwms.cda.api.project.ProjectLockHandlerUtil.Fixture;
import cwms.cda.data.dao.project.ProjectDao;
import cwms.cda.data.dao.project.ProjectLockDao;
import cwms.cda.data.dto.project.Project;
import cwms.cda.data.dto.project.ProjectLock;
import fixtures.CwmsDataApiSetupCallback;
import fixtures.TestAccounts;
import io.restassured.filter.log.LogDetail;
import io.restassured.specification.RequestSpecification;
import java.sql.SQLException;
import javax.servlet.http.HttpServletResponse;
import mil.army.usace.hec.test.database.CwmsDatabaseContainer;
import org.jooq.DSLContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

@Tag("integration")
public class ProjectLockGetOneHandlerIT extends DataApiTestIT {

    public static final String OFFICE = "SPK";
    String projId = "stProj";
    String appId = "isLocked_test";

    int revokeTimeout = 10;
    boolean revokeExisting = true;
    String lockId = null;

    @BeforeEach
    void setUp() throws SQLException {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext dsl = getDslContext(c, OFFICE);
            ProjectDao prjDao = new ProjectDao(dsl);

            Project testProject = buildTestProject(OFFICE, projId);
            prjDao.create(testProject, true);
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    @AfterEach
    void tearDown() throws SQLException {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext dsl = getDslContext(c, OFFICE);

            revokeLock(dsl, OFFICE, projId, appId);
            deleteProject(dsl, projId, OFFICE, appId);
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    @ParameterizedTest
    @MethodSource(ProjectLockHandlerUtil.METHOD_SOURCE)
    <T extends ProjectLock> void test_lock_status(Fixture<T> fixture, String format) throws SQLException {

        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext dsl = getDslContext(c, OFFICE);
            ProjectLockDao<T> lockDao = fixture.newDao(dsl);

            TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;
            lockDao.allowLockRevokerRights(OFFICE, projId, appId, user.getName());
            T req1 = fixture.minimal(OFFICE, projId, appId);
            lockId = lockDao.requestLock(req1, revokeExisting, revokeTimeout);

            assertNotNull(lockId);
            assertTrue(lockId.length() > 8);  // FYI its 32 hex chars guid
        }, CwmsDataApiSetupCallback.getWebUser());

        // This is a little different.  Returns a 200 if locked, 404 if not.
        baseRequest(fixture, format)
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get(fixture.getOnePath(OFFICE, projId))
            .then()
                .log().ifValidationFails(LogDetail.ALL, true)
            .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
        ;

        databaseLink.connection(c -> {
            DSLContext dsl = getDslContext(c, OFFICE);
            fixture.newDao(dsl).releaseLock(OFFICE, lockId);
        }, CwmsDataApiSetupCallback.getWebUser());

        baseRequest(fixture, format)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
                .get(fixture.getOnePath(OFFICE, projId))
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NOT_FOUND))
        ;

    }

    private <T extends ProjectLock> RequestSpecification baseRequest(Fixture<T> fixture, String format) {
        RequestSpecification spec = given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .header("Authorization", TestAccounts.KeyUser.SPK_NORMAL.toHeaderValue())
                .accept(format)
                .queryParam(APPLICATION_ID, appId);
        if (fixture.officeAsQueryParam()) {
            spec = spec.queryParam(Controllers.OFFICE, OFFICE);
        }
        return spec;
    }

}
