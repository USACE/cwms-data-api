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
import static cwms.cda.api.Controllers.USER_ID;
import static cwms.cda.api.project.ProjectLockHandlerUtil.buildTestProject;
import static cwms.cda.api.project.ProjectLockHandlerUtil.deleteProject;
import static cwms.cda.data.dao.DaoTest.getDslContext;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import cwms.cda.api.Controllers;
import cwms.cda.api.DataApiTestIT;
import cwms.cda.data.dao.project.ProjectDao;
import cwms.cda.data.dao.project.ProjectLockDao;
import cwms.cda.data.dto.project.LockRevokerRights;
import cwms.cda.data.dto.project.Project;
import cwms.cda.formatters.Formats;
import fixtures.TestAccounts;
import io.restassured.filter.log.LogDetail;
import java.sql.SQLException;
import java.util.List;
import javax.servlet.http.HttpServletResponse;
import org.jooq.DSLContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("integration")
public class RemoveAllLockRevokerRightsHandlerIT extends DataApiTestIT {

    public static final String OFFICE = "SPK";
    String projId = "remAllIT";
    String appId = "test_remAll";

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
            ProjectLockDao lockDao = new ProjectLockDao(dsl);
            lockDao.removeAllLockRevokerRights(OFFICE, appId, TestAccounts.KeyUser.SPK_NORMAL.getName());
            deleteProject(dsl, projId, OFFICE, appId);
        });
    }

    @Test
    void test_removeAll_rights() throws SQLException {
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSON)
            .header("Authorization", TestAccounts.KeyUser.SPK_NORMAL.toHeaderValue())
            .queryParam(Controllers.OFFICE, OFFICE)
            .queryParam(USER_ID, TestAccounts.KeyUser.SPK_NORMAL.getName())
            .queryParam(APPLICATION_ID, appId)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/project-lock-rights/remove-all")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
        ;

        connectionAsWebUser(c -> {
            DSLContext dsl = getDslContext(c, OFFICE);
            ProjectLockDao lockDao = new ProjectLockDao(dsl);

            // Add an allow
            lockDao.updateLockRevokerRights(OFFICE, projId, appId, TestAccounts.KeyUser.SPK_NORMAL.getName(), true);

            // make sure its there.
            List<LockRevokerRights> lockRevokerRights = lockDao.catLockRevokerRights(OFFICE, projId, appId);
            assertNotNull(lockRevokerRights);
            assertFalse(lockRevokerRights.isEmpty());
        });

        // Now remove all again
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSON)
            .header("Authorization", TestAccounts.KeyUser.SPK_NORMAL.toHeaderValue())
            .queryParam(Controllers.OFFICE, OFFICE)
            .queryParam(USER_ID, TestAccounts.KeyUser.SPK_NORMAL.getName())
            .queryParam(APPLICATION_ID, appId)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/project-lock-rights/remove-all")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
        ;

        connectionAsWebUser(c -> {
            DSLContext dsl = getDslContext(c, OFFICE);
            ProjectLockDao lockDao = new ProjectLockDao(dsl);

            // make sure its gone.
            List<LockRevokerRights> lockRevokerRights = lockDao.catLockRevokerRights(OFFICE, projId, appId);
            assertNotNull(lockRevokerRights);
            assertTrue(lockRevokerRights.isEmpty());
        });

    }

}
