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
import static org.junit.jupiter.api.Assertions.assertNull;

import cwms.cda.api.DataApiTestIT;
import cwms.cda.data.dao.project.ProjectDao;
import cwms.cda.data.dao.project.ProjectLockDao;
import cwms.cda.data.dto.project.Project;
import cwms.cda.data.dto.project.ProjectLock;
import cwms.cda.formatters.Formats;
import fixtures.TestAccounts;
import io.restassured.filter.log.LogDetail;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;
import javax.servlet.http.HttpServletResponse;
import org.jooq.DSLContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;


@Tag("integration")
public class ProjectLockRevokeDenyHandlerIT extends DataApiTestIT {

    // other lock tests at : https://bitbucket.hecdev.net/projects/CWMS/repos/hec-cwms-data-access/
    // browse/hec-db-integration-testing/src/test/java/wcds/dbi/oracle/project/
    // TestProjectLocks.java#475,492,537

    public static final String OFFICE = "SPK";
    final String[] lockId = {null};
    final String[] lockId2 = {null};

    String projId = "deny_Proj";
    String appId = "deny_test";

    TestAccounts.KeyUser user1 = TestAccounts.KeyUser.SPK_NORMAL;
    TestAccounts.KeyUser user2 = TestAccounts.KeyUser.SPK_NORMAL2;
    int revokeTimeout = 10;

    @BeforeEach
    void setUp() throws SQLException {
        connectionAsWebUser(c -> {
            DSLContext dsl = getDslContext(c, OFFICE);
            ProjectDao prjDao = new ProjectDao(dsl);
            ProjectLockDao lockDao = new ProjectLockDao(dsl);

            Project testProject = buildTestProject(OFFICE, projId);
            prjDao.create(testProject, true);

            lockDao.allowLockRevokerRights(OFFICE, projId, appId, user1.getName());
            lockDao.allowLockRevokerRights(OFFICE, projId, appId, user2.getName());

            boolean dontRevoke = false;

            ProjectLock req1 = new ProjectLock.Builder(OFFICE, projId, appId).build();
            lockId[0] = lockDao.requestLock(req1, dontRevoke, revokeTimeout);
            assertNotNull(lockId[0]);

            assertFalse(lockId[0].isEmpty());
            // now user1 has a lock.
        });
    }

    @AfterEach
    void tearDown() throws SQLException {
        connectionAsWebUser(c -> {
            DSLContext dsl = getDslContext(c, OFFICE);

            releaseLock(dsl, OFFICE, lockId2);
            releaseLock(dsl, OFFICE, lockId);
            revokeLock(dsl, OFFICE, projId, appId);
            deleteProject(dsl, projId, OFFICE, appId);
        });
    }

    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    @Test
    void test_can_deny_revoke() {

        requestOnThread();

        // while the above is waiting on thread for revokeTimeout we deny the revoke.
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSON)
            .header("Authorization", user1.toHeaderValue())
            .queryParam(LOCK_ID, lockId[0])
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/project-locks/deny")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
            .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK));
    }

    private void requestOnThread() {
        // try to get the lock on another thread.
        Thread thread = new Thread(() -> {
            try {
                long before = System.currentTimeMillis();
                lockId2[0]  = requestLock();
                assertNull(lockId2[0]);

                long after = System.currentTimeMillis();
                long elapsed = after - before;
                assertFalse(Math.abs((1000L * revokeTimeout) - elapsed) < 1000);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
        thread.setDaemon(true);
        thread.start();
    }

    private String requestLock() throws SQLException {
        final String[] retval = {null};
        connectionAsWebUser(c -> {
            DSLContext dsl = getDslContext(c, OFFICE);
            ProjectLockDao lockDao = new ProjectLockDao(dsl);

            // this should block until revokeTimeout if we don't do anything
            // but we are going to deny the revoke so it should return before 10s and
            // user2 should not get the lock
            ProjectLock req1 = new ProjectLock.Builder(OFFICE, projId, appId).build();
            retval[0] = lockDao.requestLock(req1, true, revokeTimeout);
        });
        return retval[0];
    }

}
