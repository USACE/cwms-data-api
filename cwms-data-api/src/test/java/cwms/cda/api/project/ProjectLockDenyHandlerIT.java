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
import fixtures.CwmsDataApiSetupCallback;
import fixtures.TestAccounts;
import io.restassured.filter.log.LogDetail;
import java.sql.SQLException;
import javax.servlet.http.HttpServletResponse;
import mil.army.usace.hec.test.database.CwmsDatabaseContainer;
import org.jooq.DSLContext;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;


@Tag("integration")
public class ProjectLockDenyHandlerIT extends DataApiTestIT {

    public static final String OFFICE = "SPK";

    // other lock tests at : https://bitbucket.hecdev.net/projects/CWMS/repos/hec-cwms-data-access/
    // browse/hec-db-integration-testing/src/test/java/wcds/dbi/oracle/project/
    // TestProjectLocks.java#475,492,537


    @Test
    void test_can_deny_revoke() throws SQLException {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        final String[] lockId = {null};
        final String[] lockId2 = {null};

        String projId = "deny_Proj";
        String appId = "deny_test";
        String officeMask = OFFICE;
        TestAccounts.KeyUser user1 = TestAccounts.KeyUser.SPK_NORMAL;
        TestAccounts.KeyUser user2 = TestAccounts.KeyUser.SPK_NORMAL2;
        int revokeTimeout = 10;

        try {
            // setup
            databaseLink.connection(c -> {
                DSLContext dsl = getDslContext(c, OFFICE);
                ProjectDao prjDao = new ProjectDao(dsl);
                ProjectLockDao lockDao = new ProjectLockDao(dsl);

                Project testProject = buildTestProject(OFFICE, projId);
                prjDao.create(testProject);

                lockDao.allowLockRevokerRights(OFFICE, officeMask, projId, appId, user1.getName());
                lockDao.allowLockRevokerRights(OFFICE, officeMask, projId, appId, user2.getName());

                boolean dontRevoke = false;

                ProjectLock req1 = new ProjectLock.Builder(OFFICE, projId, appId).build();
                lockId[0] = lockDao.requestLock(req1, dontRevoke, revokeTimeout);
                assertNotNull(lockId[0]);

                assertFalse(lockId[0].isEmpty());
                // now user1 has a lock.
            }, CwmsDataApiSetupCallback.getWebUser());


            // have user2 try to get the lock on another thread.
            Thread thread = new Thread(() -> {
                try {
                    databaseLink.connection(c -> {
                        DSLContext dsl = getDslContext(c, OFFICE);
                        ProjectLockDao lockDao = new ProjectLockDao(dsl);

                        long before = System.currentTimeMillis();

                        // this should block until revokeTimeout if we don't do anything
                        // but we are going to deny the revoke so it should return before 10s and
                        // user2 should not get the lock
                        ProjectLock req1 = new ProjectLock.Builder(OFFICE, projId, appId).build();
                        lockId2[0] = lockDao.requestLock(req1, true, revokeTimeout);
                        assertNull(lockId2[0]);

                        long after = System.currentTimeMillis();
                        long elapsed = after - before;
                        assertFalse(Math.abs((1000 * revokeTimeout) - elapsed) < 1000);

                    }, CwmsDataApiSetupCallback.getWebUser());
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            });
            thread.setDaemon(true);
            thread.start();

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

        } finally {
            // clean up
            databaseLink.connection(c -> {
                DSLContext dsl = getDslContext(c, OFFICE);
                ProjectDao prjDao = new ProjectDao(dsl);
                ProjectLockDao lockDao = new ProjectLockDao(dsl);
                releaseLock(lockDao, OFFICE, lockId2);
                releaseLock(lockDao, OFFICE, lockId);
                revokeLock(lockDao,OFFICE, projId, appId);

                deleteProject(prjDao, projId, lockDao, OFFICE, appId);

            }, CwmsDataApiSetupCallback.getWebUser());
        }

    }

}
