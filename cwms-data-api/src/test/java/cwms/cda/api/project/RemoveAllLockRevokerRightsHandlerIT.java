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
import static cwms.cda.api.Controllers.OFFICE_MASK;
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
import fixtures.CwmsDataApiSetupCallback;
import fixtures.TestAccounts;
import io.restassured.filter.log.LogDetail;
import java.sql.SQLException;
import java.util.List;
import javax.servlet.http.HttpServletResponse;
import mil.army.usace.hec.test.database.CwmsDatabaseContainer;
import org.jooq.DSLContext;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("integration")
public class RemoveAllLockRevokerRightsHandlerIT extends DataApiTestIT {

    public static final String OFFICE = "SPK";

    @Test
    void test_removeAll_rights() throws SQLException {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext dsl = getDslContext(c, OFFICE);
            ProjectLockDao lockDao = new ProjectLockDao(dsl);
            ProjectDao prjDao = new ProjectDao(dsl);

            String projId = "remAllIT";
            String appId = "test_remAll";
            String officeMask = OFFICE;

            Project testProject = buildTestProject(OFFICE, projId);
            prjDao.create(testProject);

            TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;
            String userName = user.getName();

            try {

                // first removeALL
                given()
                        .log().ifValidationFails(LogDetail.ALL, true)
                        .accept(Formats.JSON)
                        .header("Authorization", user.toHeaderValue())
                        .queryParam(OFFICE_MASK, officeMask)
                        .queryParam(Controllers.OFFICE, OFFICE)
                        .queryParam(USER_ID, userName)
                        .queryParam(APPLICATION_MASK, appId)
                    .when()
                        .redirects().follow(true)
                        .redirects().max(3)
                        .post("/project-lock-rights/remove-all")
                    .then()
                        .log().ifValidationFails(LogDetail.ALL, true)
                    .assertThat()
                        .statusCode(is(HttpServletResponse.SC_OK))
                ;

                // Add an allow
                lockDao.updateLockRevokerRights(OFFICE, officeMask, projId, appId, userName, true);

                // make sure its there.
                List<LockRevokerRights> lockRevokerRights = lockDao.catLockRevokerRights(OFFICE, projId, appId);
                assertNotNull(lockRevokerRights);
                assertFalse(lockRevokerRights.isEmpty());

                // Now remove all again
                given()
                        .log().ifValidationFails(LogDetail.ALL, true)
                        .accept(Formats.JSON)
                        .header("Authorization", user.toHeaderValue())
                        .queryParam(Controllers.OFFICE, OFFICE)
                        .queryParam(OFFICE_MASK, officeMask)
                        .queryParam(USER_ID, userName)
                        .queryParam(APPLICATION_MASK, appId)
                    .when()
                        .redirects().follow(true)
                        .redirects().max(3)
                        .post("/project-lock-rights/remove-all")
                    .then()
                        .log().ifValidationFails(LogDetail.ALL, true)
                    .assertThat()
                        .statusCode(is(HttpServletResponse.SC_OK))
                ;

                // make sure its gone.
                lockRevokerRights = lockDao.catLockRevokerRights(OFFICE, projId, appId);
                assertNotNull(lockRevokerRights);
                assertTrue(lockRevokerRights.isEmpty());


            } finally {
                lockDao.removeAllLockRevokerRights(OFFICE, officeMask, appId, userName);
                deleteProject(prjDao, projId, lockDao, OFFICE, appId);
            }
        });

    }

}
