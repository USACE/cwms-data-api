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

package cwms.cda.api;

import static cwms.cda.security.KeyAccessManager.AUTH_HEADER;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.fasterxml.jackson.databind.ObjectMapper;
import cwms.cda.data.dto.Project;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.json.JsonV2;
import fixtures.TestAccounts;
import io.restassured.filter.log.LogDetail;
import io.restassured.response.ExtractableResponse;
import io.restassured.response.Response;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("integration")
final class ProjectControllerIT extends DataApiTestIT {


    @Test
    void test_get_create_delete() throws IOException {
        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/api/project.json");
        assertNotNull(resource);
        String json = IOUtils.toString(resource, StandardCharsets.UTF_8);
        assertNotNull(json);
        Project project = Formats.parseContent(new ContentType(Formats.JSON), json, Project.class);

        // Structure of test:
        // 1)Create the Project
        // 2)Retrieve the Project and assert that it exists
        // 3)Delete the Project
        // 4)Retrieve the Project and assert that it does not exist
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

        //Create the project
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSON)
            .contentType(Formats.JSON)
            .body(json)
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/projects/")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED))
        ;

        String office = project.getOfficeId();
        // Retrieve the project and assert that it exists
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSON)
            .queryParam(Controllers.OFFICE, office)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("projects/" + project.getName())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("office-id", equalTo(office))
            .body("name", equalTo(project.getName()))
        ;

        // Delete a Project
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSON)
            .queryParam(Controllers.OFFICE, office)
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("projects/" + project.getName())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NO_CONTENT))
        ;

        // Retrieve a Project and assert that it does not exist
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSON)
            .queryParam(Controllers.OFFICE, office)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("projects/" + project.getName())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NOT_FOUND))
        ;
    }

    @Test
    void test_update_does_not_exist() throws IOException {
        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/api/project_new.json");
        assertNotNull(resource);
        String json = IOUtils.toString(resource, StandardCharsets.UTF_8);
        assertNotNull(json);
        Project project = Formats.parseContent(new ContentType(Formats.JSON), json, Project.class);
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

        //Try to update the project - should fail b/c it does not exist
        given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(Formats.JSON)
                .contentType(Formats.JSON)
                .body(json)
                .header(AUTH_HEADER, user.toHeaderValue())
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .patch("/projects/" + project.getName())
            .then()
                .log().ifValidationFails(LogDetail.ALL, true)
            .assertThat()
                .statusCode(is(HttpServletResponse.SC_NOT_FOUND))
        ;

    }

    @Test
    void test_delete_does_not_exist() {
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;
        // Delete a Project
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSON)
            .queryParam(Controllers.OFFICE, user.getOperatingOffice())
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("projects/" + "blah" + Math.random())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NOT_FOUND))
        ;
    }

    @Test
    void test_get_all() throws IOException {
        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/api/project.json");
        assertNotNull(resource);
        String json = IOUtils.toString(resource, StandardCharsets.UTF_8);
        assertNotNull(json);
        Project project = Formats.parseContent(new ContentType(Formats.JSON), json, Project.class);

        // Structure of test:
        // 1)Create the Project
        // 2)Retrieve the Project with getAll and assert that it exists
        // 3)Delete the Project
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

        //Create the project
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSON)
            .contentType(Formats.JSON)
            .body(json)
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
        .post("/projects/")
            .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED))
        ;

        String office = project.getOfficeId();

        long expectedCostYear = 1717282800000L;

        // Retrieve the project and assert that it exists
        long expectedStart = 1717282800000L;
        long expectedEnd = 1717308000000L;

        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSON)
            .queryParam(Controllers.OFFICE, office)
            .queryParam(Controllers.ID_MASK, "^" + project.getName() + "$")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("projects/")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("projects[0].office-id", equalTo(office))
            .body("projects[0].name", equalTo(project.getName()))
            .body("projects[0].federal-cost", equalTo(100.0f))
            .body("projects[0].non-federal-cost", equalTo(50.0f))
            .body("projects[0].federal-o-and-m-cost", equalTo(10.0f))
            .body("projects[0].non-federal-o-and-m-cost", equalTo(5.0f))
            .body("projects[0].authorizing-law", equalTo("Authorizing Law"))
            .body("projects[0].project-owner", equalTo("Project Owner"))
            .body("projects[0].hydropower-desc", equalTo("Hydropower Description"))
            .body("projects[0].sedimentation-desc", equalTo("Sedimentation Description"))
            .body("projects[0].downstream-urban-desc", equalTo("Downstream Urban Description"))
            .body("projects[0].bank-full-capacity-desc", equalTo("Bank Full Capacity Description"))
            .body("projects[0].project-remarks", equalTo("Remarks"))
            .body("projects[0].yield-time-frame-start", equalTo(expectedStart))
            .body("projects[0].yield-time-frame-end", equalTo(expectedEnd))
            .body("projects[0].cost-year", equalTo(expectedCostYear))
// TODO:           .body("projects[0].pump-back-location-id", equalTo("Pumpback Location Id"))
// TODO:           .body("projects[0].pump-back-office-id", equalTo("SPK"))
// TODO:           .body("projects[0].near-gage-location-id", equalTo("Near Gage Location Id"))
// TODO:           .body("projects[0].near-gage-office-id", equalTo("SPK"))
// TODO:           .body("projects[0].cost-unit", equalTo("$"))

        ;

        // Delete a Project
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSON)
            .queryParam(Controllers.OFFICE, office)
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("projects/" + project.getName())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NO_CONTENT))
        ;
    }

    @Test
    void test_get_all_paged() throws IOException {
        InputStream resource = this.getClass().getResourceAsStream("/cwms/cda/api/project.json");
        assertNotNull(resource);
        String json = IOUtils.toString(resource, StandardCharsets.UTF_8);
        assertNotNull(json);
        Project project = Formats.parseContent(new ContentType(Formats.JSON), json, Project.class);

        // Structure of test:
        // 1)Create Projects
        // 2)Retrieve the Project with getAll and assert that it exists
        // 3)Delete the Projects
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

        ObjectMapper om = JsonV2.buildObjectMapper();

        for (int i = 0; i < 15; i++) {
            Project.Builder builder = new Project.Builder();
            builder.from(project)
                    .withName(String.format("PageTest%2d", i));
            Project build = builder.build();
            String projJson = om.writeValueAsString(build);

            //Create the project
            given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(Formats.JSON)
                .contentType(Formats.JSON)
                .body(projJson)
                .header(AUTH_HEADER, user.toHeaderValue())
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .post("/projects/")
            .then()
                .log().ifValidationFails(LogDetail.ALL, true)
            .assertThat()
                .statusCode(is(HttpServletResponse.SC_CREATED));
        }

        String office = project.getOfficeId();
        try {
            ExtractableResponse<Response> extractableResponse = given()
                    .log().ifValidationFails(LogDetail.ALL, true)
                    .accept(Formats.JSON)
                    .queryParam(Controllers.OFFICE, office)
                    .queryParam(Controllers.PAGE_SIZE, 5)
                    .queryParam(Controllers.ID_MASK, "^PageTest.*$")
                .when()
                    .redirects().follow(true)
                    .redirects().max(3)
                    .get("projects/")
                .then()
                    .log().ifValidationFails(LogDetail.ALL, true)
                .assertThat()
                    .statusCode(is(HttpServletResponse.SC_OK))
                    .body("projects[0].office-id", equalTo(office))
                    .body("projects[0].name", equalTo("PageTest 0"))
                    .body("projects[1].name", equalTo("PageTest 1"))
                    .body("projects[2].name", equalTo("PageTest 2"))
                    .body("projects[3].name", equalTo("PageTest 3"))
                    .body("projects[4].name", equalTo("PageTest 4"))
                    .extract();

            String next = extractableResponse.path("next-page");
            assertNotNull(next);
            assertFalse(next.isEmpty());

            given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(Formats.JSON)
                .queryParam(Controllers.OFFICE, office)
                .queryParam(Controllers.PAGE, next)
                .queryParam(Controllers.PAGE_SIZE, 5)
                .queryParam(Controllers.ID_MASK, "^PageTest.*$")
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("projects/")
            .then()
                .log().ifValidationFails(LogDetail.ALL, true)
            .assertThat()
                .statusCode(is(HttpServletResponse.SC_OK))
                .body("projects[0].office-id", equalTo(office))
                .body("projects[0].name", equalTo("PageTest 5"))
                .body("projects[1].name", equalTo("PageTest 6"))
                .body("projects[2].name", equalTo("PageTest 7"))
                .body("projects[3].name", equalTo("PageTest 8"))
                .body("projects[4].name", equalTo("PageTest 9"))
                ;
        } finally {
            for (int i = 0; i < 15; i++) {
                // Delete the Projects
                given()
                    .log().ifValidationFails(LogDetail.ALL, true)
                    .accept(Formats.JSON)
                    .queryParam(Controllers.OFFICE, office)
                    .header(AUTH_HEADER, user.toHeaderValue())
                .when()
                    .redirects().follow(true)
                    .redirects().max(3)
                    .delete(String.format("projects/PageTest%2d", i))
                .then()
                    .log().ifValidationFails(LogDetail.ALL, true)
                ;

            }
        }
    }

}
