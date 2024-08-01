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
import cwms.cda.data.dao.project.ProjectDao;
import cwms.cda.data.dao.project.ProjectLockDao;
import cwms.cda.data.dto.project.Project;
import cwms.cda.data.dto.project.ProjectLock;
import cwms.cda.formatters.Formats;
import fixtures.TestAccounts;
import io.restassured.filter.log.LogDetail;
import java.sql.SQLException;
import javax.servlet.http.HttpServletResponse;
import org.jooq.DSLContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

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
            ProjectLockDao lockDao = new ProjectLockDao(dsl);
            ProjectDao prjDao = new ProjectDao(dsl);

            Project testProject = buildTestProject(OFFICE, projId);
            prjDao.create(testProject);

            lockDao.removeAllLockRevokerRights(OFFICE, appId, userName); // start fresh
            lockDao.allowLockRevokerRights(OFFICE, projId, appId, userName);

            ProjectLock req1 = new ProjectLock.Builder(OFFICE, projId, appId).build();
            lockId = lockDao.requestLock(req1, true, 10);
            assertNotNull(lockId);
            assertTrue(lockId.length() > 8);  // FYI its 32 hex chars

            boolean locked = lockDao.isLocked(OFFICE, projId, appId);
            assertTrue(locked);

        });
    }

    @AfterEach
    void tearDown() throws SQLException {
        connectionAsWebUser(c -> {
            DSLContext dsl = getDslContext(c, OFFICE);
            ProjectLockDao lockDao = new ProjectLockDao(dsl);

            releaseLock(dsl, OFFICE, lockId);
            revokeLock(dsl, OFFICE, projId, appId);

            lockDao.removeAllLockRevokerRights(OFFICE, appId, userName);
            deleteProject(dsl, projId, OFFICE, appId);
        });
    }

    @Test
    void test_release() throws SQLException {

        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSON)
            .header("Authorization", TestAccounts.KeyUser.SPK_NORMAL.toHeaderValue())
            .queryParam(LOCK_ID, lockId)
            .queryParam(Controllers.OFFICE, OFFICE)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/project-locks/release")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK));

        connectionAsWebUser(c -> {
            DSLContext dsl = getDslContext(c, OFFICE);
            ProjectLockDao lockDao = new ProjectLockDao(dsl);
            boolean locked = lockDao.isLocked(OFFICE, projId, appId);
            assertFalse(locked);
        });

    }

}
