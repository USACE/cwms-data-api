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

import static cwms.cda.api.Controllers.APPLICATION_ID;
import static cwms.cda.api.Controllers.OFFICE_MASK;
import static cwms.cda.api.Controllers.PROJECT_ID;
import static cwms.cda.api.Controllers.TIME_ZONE_ID;
import static cwms.cda.api.project.ProjectLockHandlerUtil.buildTestProject;
import static cwms.cda.api.project.ProjectLockHandlerUtil.deleteProject;
import static cwms.cda.data.dao.DaoTest.getDslContext;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalToIgnoringCase;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.flogger.FluentLogger;
import cwms.cda.api.DataApiTestIT;
import cwms.cda.data.dao.project.ProjectDao;
import cwms.cda.data.dao.project.ProjectLockDao;
import cwms.cda.data.dto.project.Project;
import cwms.cda.formatters.Formats;
import fixtures.CwmsDataApiSetupCallback;
import fixtures.TestAccounts;
import io.restassured.filter.log.LogDetail;
import java.sql.SQLException;
import java.util.logging.Level;
import javax.servlet.http.HttpServletResponse;
import mil.army.usace.hec.test.database.CwmsDatabaseContainer;
import org.jooq.DSLContext;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("integration")
public class ProjectLockCatalogHandlerIT extends DataApiTestIT {
    private static final FluentLogger logger = FluentLogger.forEnclosingClass();

    public static final String OFFICE = "SPK";


    @Test
    void test_cat_locks() throws SQLException {

        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext dsl = getDslContext(c, OFFICE);
            ProjectDao prjDao = new ProjectDao(dsl);
            ProjectLockDao lockDao = new ProjectLockDao(dsl);

            String projId = "catLocks";
            String appId = "catLocks_test";
            TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;
            String userName = user.getName();
            String officeMask = OFFICE;
            Project testProject = buildTestProject(OFFICE, projId);
            prjDao.create(testProject);
            try {
                int revokeTimeout = 10;

                lockDao.updateLockRevokerRights(OFFICE, userName, projId, appId + "_1", officeMask, true);
                lockDao.updateLockRevokerRights(OFFICE, userName, projId, appId + "_2", officeMask, true);
                String lock1 = lockDao.requestLock(OFFICE, projId, appId + "_1", true, revokeTimeout);
                assertTrue(lock1.length() > 8);
                String lock2 = lockDao.requestLock(OFFICE, projId, appId + "_2", false, revokeTimeout);
                assertTrue(lock2.length() > 8);
                assertNotEquals(lock1, lock2);

                given()
                    .log().ifValidationFails(LogDetail.ALL, true)
                    .accept(Formats.JSON)
                    .queryParam(PROJECT_ID, projId)
                    .queryParam(APPLICATION_ID, appId)
                    .queryParam(TIME_ZONE_ID, "UTC")
                    .queryParam(OFFICE_MASK, officeMask)
                .when()
                    .redirects().follow(true)
                    .redirects().max(3)
                .get("/project-locks/")
                .then()
                    .log().ifValidationFails(LogDetail.ALL, true)
                .assertThat()
                    .statusCode(is(HttpServletResponse.SC_OK))
                    .body("size()", is(2))
                    .body("[0].office-id", equalToIgnoringCase(OFFICE))
                    .body("[0].project-id", equalToIgnoringCase(projId))
                    .body("[0].application-id", equalToIgnoringCase(appId+"_1"))
                    .body("[0].session-user", equalToIgnoringCase(userName))
//                                .body("[0].session-program", equalToIgnoringCase("JDBC Thin Client"))
                    .body("[1].office-id", equalToIgnoringCase(OFFICE))
                    .body("[1].project-id", equalToIgnoringCase(projId))
                    .body("[1].application-id", equalToIgnoringCase(appId+"_2"))
                    .body("[1].session-user", equalToIgnoringCase(userName))
                ;

                lockDao.releaseLock(lock1);
                lockDao.releaseLock(lock2);

                lockDao.updateLockRevokerRights(OFFICE, userName, projId, appId + "_1", officeMask, true);
                lockDao.updateLockRevokerRights(OFFICE, userName, projId, appId + "_2", officeMask, true);

            } finally {
                try {
                    lockDao.revokeLock(OFFICE, projId, appId + "_1", 0);
                } catch (Exception e) {
                    logger.at(Level.WARNING).withCause(e).log("Failed to revoke lock: %s", appId+"_1");
                }
                try {
                    lockDao.revokeLock(OFFICE, projId, appId + "_2", 0);
                } catch (Exception e) {
                    logger.at(Level.WARNING).withCause(e).log("Failed to revoke lock: %s", appId+"_2");
                }

                deleteProject(prjDao, projId, lockDao, OFFICE, appId);
            }
        });
    }

}
