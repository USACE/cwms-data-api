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
import static cwms.cda.api.Controllers.PROJECT_MASK;
import static cwms.cda.api.project.ProjectLockHandlerUtil.buildTestProject;
import static cwms.cda.api.project.ProjectLockHandlerUtil.deleteProject;
import static cwms.cda.api.project.ProjectLockHandlerUtil.releaseLock;
import static cwms.cda.api.project.ProjectLockHandlerUtil.revokeLock;
import static cwms.cda.data.dao.DaoTest.getDslContext;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import cwms.cda.api.DataApiTestIT;
import cwms.cda.data.dao.project.ProjectDao;
import cwms.cda.data.dao.project.ProjectLockDao;
import cwms.cda.data.dto.project.Project;
import cwms.cda.data.dto.project.ProjectLock;
import cwms.cda.formatters.Formats;
import fixtures.CwmsDataApiSetupCallback;
import io.restassured.filter.log.LogDetail;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.servlet.http.HttpServletResponse;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.jooq.DSLContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("integration")
public class ProjectLockCatalogHandlerIT extends DataApiTestIT {

    public static final String OFFICE = "SPK";

    String projId1 = "catLocks1";
    String projId2 = "catLocks2";
    String appId = "catlocks_test";
    int revokeTimeout = 10;
    String lock1;
    String lock2;

    @BeforeEach
    void setUp() throws SQLException {
        connectionAsWebUser(c -> {
            DSLContext dsl = getDslContext(c, OFFICE);
            ProjectDao prjDao = new ProjectDao(dsl);

            Project testProject1 = buildTestProject(OFFICE, projId1);
            prjDao.create(testProject1);
            Project testProject2 = buildTestProject(OFFICE, projId2);
            prjDao.create(testProject2);

            ProjectLockDao lockDao = new ProjectLockDao(dsl);
            String webUser = CwmsDataApiSetupCallback.getWebUser();  // l2webtest
            //  removeAllLockRevokerRights seems to hang..
            lockDao.updateLockRevokerRights(OFFICE,  projId1, appId, webUser, true);
            lockDao.updateLockRevokerRights(OFFICE,  projId2, appId, webUser, true);

            ProjectLock req1 = new ProjectLock.Builder(OFFICE, projId1, appId).build();

            lock1 = lockDao.requestLock(req1, true, revokeTimeout);
            Assertions.assertNotNull(lock1);
            assertTrue(lock1.length() > 8);

            ProjectLock req2 = new ProjectLock.Builder(OFFICE, projId2, appId).build();
            lock2 = lockDao.requestLock(req2, false, revokeTimeout);
            Assertions.assertNotNull(lock2);
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
            String webUser = CwmsDataApiSetupCallback.getWebUser();  // l2webtest

            lockDao.updateLockRevokerRights(OFFICE,  projId1, appId, webUser, true);
            lockDao.updateLockRevokerRights(OFFICE,  projId2, appId, webUser, true);

            revokeLock(dsl, OFFICE, projId1, appId);  // happens as L2WEBTEST
            revokeLock(dsl, OFFICE, projId2, appId);

            deleteProject(dsl, projId1, OFFICE, appId);
            deleteProject(dsl, projId2, OFFICE, appId);
        });
    }

    @Test
    void test_cat_locks() {

        String webUser = CwmsDataApiSetupCallback.getWebUser();

        Map<String,String> lock1 = new LinkedHashMap<>();
        lock1.put("project-id", projId1);
        lock1.put("application-id", appId);
        lock1.put("office-id", OFFICE);
        lock1.put("session-user", webUser);

        Map<String,String> lock2 = new LinkedHashMap<>();
        lock1.put("project-id", projId2);
        lock1.put("application-id", appId);
        lock1.put("office-id", OFFICE);
        lock1.put("session-user", webUser);

        Matcher<String> locksMatcher = getMatcher(lock1, lock2);

        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSON)
            .queryParam(OFFICE_MASK, OFFICE)
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
            .body("[0]", locksMatcher)
            .body("[1]", locksMatcher)
        ;
    }

    private Matcher<String> getMatcher(Map<String, String> lock1, Map<String, String> lock2) {

        return new BaseMatcher<String>() {

            @Override
            public void describeTo(Description description) {
                description.appendText("the expected Project Lock items");
            }

            @Override
            public boolean matches(Object o) {
                // o is LinkedHashMap<String, String>.  keys are office-id, project-id, application-id...
                // values are "SPK", "catLocks2"

                if (o instanceof Map) {
                    Map<String, String> jsonLock = (Map<String, String>) o;
                    return matches(lock1, jsonLock) || matches(lock2, jsonLock);
                }

                return false;
            }

            private boolean matches(Map<String,String> expected, Map<String, String> provided) {

                if (provided.keySet().containsAll(expected.keySet())) {
                        return expected.entrySet().stream()
                                .allMatch(entry -> entry.getValue().equalsIgnoreCase(provided.get(entry.getKey())));
                }
                return false;
            }
        };

    }


}
