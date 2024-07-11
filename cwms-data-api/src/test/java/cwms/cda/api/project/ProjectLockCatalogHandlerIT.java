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

import static cwms.cda.api.Controllers.APPLICATION_MASK;
import static cwms.cda.api.Controllers.PROJECT_MASK;
import static cwms.cda.api.project.ProjectLockHandlerUtil.buildTestProject;
import static cwms.cda.api.project.ProjectLockHandlerUtil.deleteProject;
import static cwms.cda.api.project.ProjectLockHandlerUtil.releaseLock;
import static cwms.cda.api.project.ProjectLockHandlerUtil.revokeLock;
import static cwms.cda.data.dao.DaoTest.getDslContext;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalToIgnoringCase;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
public class ProjectLockCatalogHandlerIT extends DataApiTestIT {

    public static final String OFFICE = "SPK";

    String projId1 = "catLocks1";
    String projId2 = "catLocks2";
    String appId = "catLocks_test";
    int revokeTimeout = 10;
    String lock1;
    String lock2;

    @BeforeEach
    void setUp() throws SQLException {
        connectionAsWebUser(c -> {
            DSLContext dsl = getDslContext(c, OFFICE);
            ProjectDao prjDao = new ProjectDao(dsl);

            TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;
            String userName = user.getName();
            String officeMask = OFFICE;
            Project testProject1 = buildTestProject(OFFICE, projId1);
            prjDao.create(testProject1);
            Project testProject2 = buildTestProject(OFFICE, projId2);
            prjDao.create(testProject2);

            ProjectLockDao lockDao = new ProjectLockDao(dsl);
            lockDao.updateLockRevokerRights(OFFICE, officeMask, projId1, appId, userName, true);
            lockDao.updateLockRevokerRights(OFFICE, officeMask, projId2, appId, userName, true);

            ProjectLock req1 = new ProjectLock.Builder(OFFICE, projId1, appId).build();
            String lock1 = lockDao.requestLock(req1, true, revokeTimeout);
            assertTrue(lock1.length() > 8);

            ProjectLock req2 = new ProjectLock.Builder(OFFICE, projId2, appId).build();
            String lock2 = lockDao.requestLock(req2, false, revokeTimeout);
            assertTrue(lock2.length() > 8);
            assertNotEquals(lock1, lock2);
        });
    }

    @AfterEach
    void tearDown() throws SQLException {
        connectionAsWebUser(c -> {
            DSLContext dsl = getDslContext(c, OFFICE);

            releaseLock(dsl, OFFICE, lock1);
            releaseLock(dsl, OFFICE, lock2);

            ProjectLockDao lockDao = new ProjectLockDao(dsl);
            lockDao.updateLockRevokerRights(OFFICE, null, projId1, appId, TestAccounts.KeyUser.SPK_NORMAL.getName(), true);
            lockDao.updateLockRevokerRights(OFFICE, null, projId2, appId, TestAccounts.KeyUser.SPK_NORMAL.getName(), true);

            revokeLock(dsl, OFFICE, projId1, appId);
            revokeLock(dsl, OFFICE, projId2, appId);

            deleteProject(dsl, projId1, OFFICE, appId);
            deleteProject(dsl, projId2, OFFICE, appId);
        });
    }

    @Test
    void test_cat_locks() {

        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSON)
            .queryParam(PROJECT_MASK, "catLocks*")
            .queryParam(APPLICATION_MASK, appId)
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
            .body("[0].project-id", equalToIgnoringCase(projId1))
            .body("[0].application-id", equalToIgnoringCase(appId))
            .body("[0].session-user", equalToIgnoringCase(TestAccounts.KeyUser.SPK_NORMAL.getName()))
            .body("[1].office-id", equalToIgnoringCase(OFFICE))
            .body("[1].project-id", equalToIgnoringCase(projId2))
            .body("[1].application-id", equalToIgnoringCase(appId))
            .body("[1].session-user", equalToIgnoringCase(TestAccounts.KeyUser.SPK_NORMAL.getName()))
        ;
    }

}
