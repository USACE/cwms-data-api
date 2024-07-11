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


import static cwms.cda.api.project.ProjectLockHandlerUtil.buildTestProject;
import static cwms.cda.data.dao.DaoTest.getDslContext;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;

import com.google.common.flogger.FluentLogger;
import cwms.cda.api.Controllers;
import cwms.cda.api.DataApiTestIT;
import cwms.cda.data.dao.DeleteRule;
import cwms.cda.data.dao.project.ProjectDao;
import cwms.cda.data.dto.project.Project;
import cwms.cda.formatters.Formats;
import fixtures.TestAccounts;
import io.restassured.filter.log.LogDetail;
import java.sql.SQLException;
import java.time.Instant;
import java.util.logging.Level;
import javax.servlet.http.HttpServletResponse;
import org.jooq.DSLContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;


@Tag("integration")
public class ProjectPublishStatusUpdateHandlerIT extends DataApiTestIT {
    private static final FluentLogger logger = FluentLogger.forEnclosingClass();
    public static final String OFFICE = "SPK";
    String projId = "pub_status";
    String appId = "pub_test";
    TestAccounts.KeyUser user1 = TestAccounts.KeyUser.SPK_NORMAL;

    @BeforeEach
    void setUp() throws SQLException {
        connectionAsWebUser(c -> {
            DSLContext dsl = getDslContext(c, OFFICE);
            ProjectDao prjDao = new ProjectDao(dsl);

            Project testProject = buildTestProject(OFFICE, projId);
            prjDao.create(testProject);
        });
    }

    @AfterEach
    void tearDown() throws SQLException {
        connectionAsWebUser(c -> {
            DSLContext dsl = getDslContext(c, OFFICE);
            ProjectDao prjDao = new ProjectDao(dsl);

            try {
                prjDao.delete(OFFICE, projId, DeleteRule.DELETE_ALL);
            } catch (Exception e) {
                logger.at(Level.WARNING).withCause(e).log("Failed to delete project: %s", projId);
            }
        });
    }

    @Test
    void test_can_call_publish() {

        String src = "srcId";
        // skip tsId
        Instant start = Instant.now();
        String startStr = start.toString();
        Instant end = Instant.now();
        String endStr = end.toString();

        long before = Instant.now().toEpochMilli();

        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSON)
            .header("Authorization", user1.toHeaderValue())
            .queryParam(Controllers.OFFICE, OFFICE)
            .queryParam(Controllers.APPLICATION_ID, appId)
            .queryParam(Controllers.SOURCE_ID, src)
            .queryParam(Controllers.BEGIN, startStr)
            .queryParam(Controllers.END, endStr)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/projects/status-update/" + projId)
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .body("value", is(greaterThan(before)))
            .body("value", is(lessThan(Instant.now().toEpochMilli())))
            .statusCode(is(HttpServletResponse.SC_OK));

    }

}
