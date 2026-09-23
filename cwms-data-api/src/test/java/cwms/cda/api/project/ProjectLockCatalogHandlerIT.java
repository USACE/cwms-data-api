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
import cwms.cda.api.project.ProjectLockHandlerUtil.Fixture;
import cwms.cda.data.dao.project.ProjectDao;
import cwms.cda.data.dao.project.ProjectLockDao;
import cwms.cda.data.dao.project.ProjectLockDaoV1;
import cwms.cda.data.dto.project.Project;
import cwms.cda.data.dto.project.ProjectLock;
import cwms.cda.formatters.Formats;
import fixtures.CwmsDataApiSetupCallback;
import fixtures.TestAccounts;
import io.restassured.filter.log.LogDetail;
import io.restassured.specification.RequestSpecification;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

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
            prjDao.create(testProject1, true);
            Project testProject2 = buildTestProject(OFFICE, projId2);
            prjDao.create(testProject2, true);
        });
    }

    @AfterEach
    void tearDown() throws SQLException {
        connectionAsWebUser(c -> {
            DSLContext dsl = getDslContext(c, OFFICE);

            releaseLock(dsl, OFFICE, lock1);
            releaseLock(dsl, OFFICE, lock2);

            ProjectLockDaoV1 lockDao = new ProjectLockDaoV1(dsl);
            String webUser = CwmsDataApiSetupCallback.getWebUser();  // l2webtest

            lockDao.updateLockRevokerRights(OFFICE, projId1, appId, webUser, true);
            lockDao.updateLockRevokerRights(OFFICE, projId2, appId, webUser, true);

            revokeLock(dsl, OFFICE, projId1, appId);  // happens as L2WEBTEST
            revokeLock(dsl, OFFICE, projId2, appId);

            deleteProject(dsl, projId1, OFFICE, appId);
            deleteProject(dsl, projId2, OFFICE, appId);
        });
    }

    @ParameterizedTest
    @MethodSource(ProjectLockHandlerUtil.METHOD_SOURCE)
    <T extends ProjectLock> void test_cat_locks(Fixture<T> fixture, String format) throws SQLException {

        String webUser = CwmsDataApiSetupCallback.getWebUser();  // l2webtest

        connectionAsWebUser(c -> {
            DSLContext dsl = getDslContext(c, OFFICE);
            ProjectLockDao<T> lockDao = fixture.newDao(dsl);

            //  removeAllLockRevokerRights seems to hang..
            lockDao.updateLockRevokerRights(OFFICE, projId1, appId, webUser, true);
            lockDao.updateLockRevokerRights(OFFICE, projId2, appId, webUser, true);

            T req1 = fixture.minimal(OFFICE, projId1, appId);
            lock1 = lockDao.requestLock(req1, true, revokeTimeout);
            Assertions.assertNotNull(lock1);
            assertTrue(lock1.length() > 8);

            T req2 = fixture.minimal(OFFICE, projId2, appId);
            lock2 = lockDao.requestLock(req2, false, revokeTimeout);
            Assertions.assertNotNull(lock2);
            assertTrue(lock2.length() > 8);
            assertNotEquals(lock1, lock2);
        });

        Map<String,String> expectedLock1 = new LinkedHashMap<>();
        expectedLock1.put("name", projId1);
        expectedLock1.put("application-id", appId);
        expectedLock1.put("office-id", OFFICE);
        expectedLock1.put("session-user", webUser);

        Map<String,String> expectedLock2 = new LinkedHashMap<>();
        expectedLock2.put("name", projId2);
        expectedLock2.put("application-id", appId);
        expectedLock2.put("office-id", OFFICE);
        expectedLock2.put("session-user", webUser);

        Matcher<String> locksMatcher = getMatcher(fixture, expectedLock1, expectedLock2);

        RequestSpecification spec =
            given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(format)
                .header("Authorization", TestAccounts.KeyUser.SPK_NORMAL.toHeaderValue()) // catalog call needs auth b/c it returns PII
                .queryParam(PROJECT_MASK, "catLocks*")
                .queryParam(APPLICATION_MASK, appId);
        if (fixture.officeAsQueryParam()) {
            spec = spec.queryParam(OFFICE_MASK, OFFICE);
        }

        spec
        .when()
            .redirects().follow(true)
            .redirects().max(3)
        .get(fixture.catalogPath(OFFICE))
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("size()", is(2))
            .body("[0]", locksMatcher)
            .body("[1]", locksMatcher)
        ;
    }

    private <T extends ProjectLock> Matcher<String> getMatcher(Fixture<T> fixture, Map<String, String> lock1,
            Map<String, String> lock2) {

        return new BaseMatcher<String>() {

            @Override
            public void describeTo(Description description) {
                description.appendText("the expected Project Lock items");
            }

            @Override
            public boolean matches(Object o) {
                // o is a LinkedHashMap<String, Object>. For V1 the keys are flat
                // (office-id, project-id, application-id...); for V2 office/name are
                // nested under an "id" object, so the Fixture knows how to pull them out.
                if (o instanceof Map) {
                    Map<String, Object> jsonLock = (Map<String, Object>) o;
                    return matches(lock1, jsonLock) || matches(lock2, jsonLock);
                }

                return false;
            }

            private boolean matches(Map<String, String> expected, Map<String, Object> provided) {
                String office = fixture.officeOf(provided);
                String name = fixture.nameOf(provided);
                Object applicationId = provided.get("application-id");
                Object sessionUser = provided.get("session-user");

                return expected.get("office-id").equalsIgnoreCase(office)
                        && expected.get("name").equalsIgnoreCase(name)
                        && expected.get("application-id").equalsIgnoreCase(String.valueOf(applicationId))
                        && expected.get("session-user").equalsIgnoreCase(String.valueOf(sessionUser));
            }
        };

    }


}
