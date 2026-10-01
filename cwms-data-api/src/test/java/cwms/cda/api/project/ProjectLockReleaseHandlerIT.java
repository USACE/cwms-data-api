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

import static cwms.cda.api.Controllers.LOCK_ID;
import static cwms.cda.api.project.ProjectLockHandlerUtil.buildTestProject;
import static cwms.cda.api.project.ProjectLockHandlerUtil.deleteProject;
import static cwms.cda.api.project.ProjectLockHandlerUtil.releaseLock;
import static cwms.cda.api.project.ProjectLockHandlerUtil.revokeLock;
import static cwms.cda.data.dao.DaoTest.getDslContext;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.flogger.FluentLogger;
import cwms.cda.api.Controllers;
import cwms.cda.api.DataApiTestIT;
import cwms.cda.api.project.ProjectLockHandlerUtil.Fixture;
import cwms.cda.data.dao.project.ProjectDao;
import cwms.cda.data.dao.project.ProjectLockDao;
import cwms.cda.data.dao.project.ProjectLockDaoV1;
import cwms.cda.data.dto.project.Project;
import cwms.cda.data.dto.project.ProjectLock;
import cwms.cda.formatters.Formats;
import fixtures.TestAccounts;
import io.restassured.filter.log.LogDetail;
import io.restassured.specification.RequestSpecification;
import java.sql.SQLException;
import javax.servlet.http.HttpServletResponse;
import org.jooq.DSLContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

@Tag("integration")
public class ProjectLockReleaseHandlerIT extends DataApiTestIT {
    private static final FluentLogger logger = FluentLogger.forEnclosingClass();

    public static final String OFFICE = "SPK";
    String projId = "lockRelease";
    String appId = "test_release";

    String lockId;

    String userName = TestAccounts.KeyUser.SPK_NORMAL.getName();

    @BeforeEach
    void setUp() throws SQLException {
        connectionAsWebUser(c -> {
            DSLContext dsl = getDslContext(c, OFFICE);
            ProjectDao prjDao = new ProjectDao(dsl);

            Project testProject = buildTestProject(OFFICE, projId);
            prjDao.create(testProject, true);
        });
    }

    @AfterEach
    void tearDown() throws SQLException {
        connectionAsWebUser(c -> {
            DSLContext dsl = getDslContext(c, OFFICE);
            ProjectLockDaoV1 lockDao = new ProjectLockDaoV1(dsl);

            releaseLock(dsl, OFFICE, lockId);
            revokeLock(dsl, OFFICE, projId, appId);

            lockDao.removeAllLockRevokerRights(OFFICE, appId, userName);
            deleteProject(dsl, projId, OFFICE, appId);
        });
    }

    @ParameterizedTest
    @MethodSource(ProjectLockHandlerUtil.METHOD_SOURCE)
    <T extends ProjectLock> void test_release(Fixture<T> fixture, String format) throws SQLException {

        connectionAsWebUser(c -> {
            DSLContext dsl = getDslContext(c, OFFICE);
            ProjectLockDao<T> lockDao = fixture.newDao(dsl);

            lockDao.removeAllLockRevokerRights(OFFICE, appId, userName); // start fresh
            lockDao.allowLockRevokerRights(OFFICE, projId, appId, userName);

            T req1 = fixture.minimal(OFFICE, projId, appId);
            lockId = lockDao.requestLock(req1, true, 10);
            assertNotNull(lockId);
            assertTrue(lockId.length() > 8);  // FYI its 32 hex chars

            boolean locked = lockDao.isLocked(OFFICE, projId, appId);
            assertTrue(locked);
        });

        RequestSpecification spec = given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(format)
            .header("Authorization", TestAccounts.KeyUser.SPK_NORMAL.toHeaderValue())
            .queryParam(LOCK_ID, lockId);
        if (fixture.officeAsQueryParam()) {
            spec = spec.queryParam(Controllers.OFFICE, OFFICE);
        }

        spec
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post(fixture.releasePath(OFFICE))
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK));

        connectionAsWebUser(c -> {
            DSLContext dsl = getDslContext(c, OFFICE);
            boolean locked = fixture.newDao(dsl).isLocked(OFFICE, projId, appId);
            assertFalse(locked);
        });

    }

}
