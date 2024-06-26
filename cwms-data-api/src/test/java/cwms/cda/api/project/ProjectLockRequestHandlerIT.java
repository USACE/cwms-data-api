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

import static cwms.cda.api.Controllers.REVOKE_EXISTING;
import static cwms.cda.api.Controllers.REVOKE_TIMEOUT;
import static cwms.cda.api.project.ProjectLockHandlerUtil.buildTestProject;
import static cwms.cda.api.project.ProjectLockHandlerUtil.deleteProject;
import static cwms.cda.data.dao.DaoTest.getDslContext;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.flogger.FluentLogger;
import cwms.cda.api.DataApiTestIT;
import cwms.cda.data.dao.project.ProjectDao;
import cwms.cda.data.dao.project.ProjectLockDao;
import cwms.cda.data.dto.project.Project;
import cwms.cda.data.dto.project.ProjectLock;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.json.JsonV2;
import fixtures.CwmsDataApiSetupCallback;
import fixtures.TestAccounts;
import io.restassured.filter.log.LogDetail;
import io.restassured.response.ExtractableResponse;
import io.restassured.response.Response;
import java.sql.SQLException;
import javax.servlet.http.HttpServletResponse;
import mil.army.usace.hec.test.database.CwmsDatabaseContainer;
import org.jooq.DSLContext;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("integration")
public class ProjectLockRequestHandlerIT extends DataApiTestIT {
    private static final FluentLogger logger = FluentLogger.forEnclosingClass();

    public static final String OFFICE = "SPK";


    @Test
    void test_request() throws SQLException {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext dsl = getDslContext(c, OFFICE);
            ProjectLockDao lockDao = new ProjectLockDao(dsl);
            ProjectDao prjDao = new ProjectDao(dsl);

            String projId = "lockReq";
            String appId = "test_req";
            String officeMask = OFFICE;

            TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;
            String userName = user.getName();

            Project testProject = buildTestProject(OFFICE, projId);
            prjDao.create(testProject);
            try {
                lockDao.removeAllLockRevokerRights(OFFICE, userName, appId, officeMask); // start fresh
                lockDao.updateLockRevokerRights(OFFICE, userName, projId, appId, officeMask, true);

                ProjectLock toRequest = new ProjectLock.Builder()
                        .withOfficeId(OFFICE)
                        .withProjectId(projId)
                        .withApplicationId(appId)
                        .withOsUser("fake_osuser")
                        .withSessionProgram("fake_program")
                        .withSessionMachine("fake_machine")
                        .withSessionUser(userName)
                        .build();

                ObjectMapper om = JsonV2.buildObjectMapper();
                String serializedProject = om.writeValueAsString(toRequest);

                ExtractableResponse<Response> response =
                    given()
                        .log().ifValidationFails(LogDetail.ALL, true)
                        .accept(Formats.JSON)
                        .header("Authorization", user.toHeaderValue())
                        .queryParam(REVOKE_EXISTING, false)
                        .queryParam(REVOKE_TIMEOUT, 10)
                        .contentType(Formats.JSON)
                        .body(serializedProject)
                    .when()
                        .redirects().follow(true)
                        .redirects().max(3)
                        .post("/project-locks/")
                    .then()
                        .log().ifValidationFails(LogDetail.ALL, true)
                    .assertThat()
                        .statusCode(is(HttpServletResponse.SC_CREATED))
                        .extract();
                String lockId = response.path("id");

                try {
                    assertNotNull(lockId);
                    assertFalse(lockId.isEmpty());
                } finally {
                    lockDao.releaseLock(OFFICE, lockId);
                }

            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            } finally {
                lockDao.removeAllLockRevokerRights(OFFICE, userName, appId, officeMask);
                deleteProject(prjDao, projId, lockDao, OFFICE, appId);
            }
        });

    }

}
