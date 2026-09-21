/*
 * MIT License
 *
 * Copyright (c) 2023 Hydrologic Engineering Center
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

import static cwms.cda.api.Controllers.CASCADE_DELETE;
import static cwms.cda.api.Controllers.CWMS_OFFICE;
import static cwms.cda.api.Controllers.IGNORE_NULLS;
import static cwms.cda.api.Controllers.OFFICE;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import com.google.common.flogger.FluentLogger;
import cwms.cda.ApiServlet;
import cwms.cda.api.errors.NotFoundException;
import cwms.cda.data.dao.TimeSeriesCategoryDao;
import cwms.cda.data.dao.TimeSeriesGroupDao;
import cwms.cda.data.dto.TimeSeriesCategory;
import cwms.cda.data.dto.timeseriesgroup.TimeSeriesGroup;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import fixtures.CwmsDataApiSetupCallback;
import fixtures.FunctionalSchemas;
import fixtures.TestAccounts;
import io.restassured.filter.log.LogDetail;
import java.util.ArrayList;
import java.util.List;
import javax.servlet.http.HttpServletResponse;
import mil.army.usace.hec.test.database.CwmsDatabaseContainer;
import org.jooq.Configuration;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@Tag("integration")
class TimeSeriesCategoryControllerTestIT extends DataApiTestIT
{
    private static final FluentLogger logger = FluentLogger.forEnclosingClass();
    private final List<TimeSeriesCategory> categoriesToCleanup = new ArrayList<>();
    private final List<TimeSeriesGroup> groupsToCleanup = new ArrayList<>();

    TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;
    TestAccounts.KeyUser user2 = TestAccounts.KeyUser.SWT_NORMAL;

    @AfterEach
    void clear_data() throws Exception {
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection(c -> {
            Configuration configuration = DSL.using(c).configuration();
            TimeSeriesGroupDao groupDao = new TimeSeriesGroupDao(configuration.dsl());
            TimeSeriesCategoryDao categoryDao = new TimeSeriesCategoryDao(configuration.dsl());

            for (TimeSeriesGroup group : groupsToCleanup) {
                try {
                    groupDao.unassignAllTs(group, group.getOfficeId());
                    if (!group.getOfficeId().equalsIgnoreCase(CWMS_OFFICE)) {
                        groupDao.delete(group.getTimeSeriesCategory().getId(), group.getId(), group.getOfficeId(), false);
                    }
                } catch (NotFoundException e) {
                    logger.atConfig().withCause(e).log("Group not found");
                }
            }
            for (TimeSeriesCategory category : categoriesToCleanup) {
                try {
                    categoryDao.delete(category.getId(), true, category.getOfficeId());
                } catch (NotFoundException e) {
                    logger.atConfig().withCause(e).log("Category not found");
                }
            }

            groupsToCleanup.clear();
            categoriesToCleanup.clear();

        }, CwmsDataApiSetupCallback.getWebUser());
    }


    @ParameterizedTest
    @ValueSource(strings = {Formats.JSON, Formats.DEFAULT})
    void test_create_update_delete(String format) {
        String officeId = user.getOperatingOffice();
        String originalId = "test_create_update_delete";
        String updatedId = "test_updated_id";
        TimeSeriesCategory cat = new TimeSeriesCategory(officeId, originalId, "IntegrationTesting");
        ContentType contentType = Formats.parseHeader(Formats.JSON, TimeSeriesCategory.class);
        String json = Formats.format(contentType, cat);

        // Delete Category to start so its not there.
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(format)
            .contentType(Formats.JSON)
            .header("Authorization", user.toHeaderValue())
            .queryParam(OFFICE, officeId)
            .queryParam(CASCADE_DELETE, "true")
        .when()
            .delete("/timeseries/category/" + originalId)
                ;  // don't care if this fails or not.

        // Verify its not there
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(format)
            .contentType(Formats.JSON)
            .queryParam(OFFICE, officeId)
        .when()
            .get("/timeseries/category/" + originalId)
        .then()
            .statusCode(is(HttpServletResponse.SC_NOT_FOUND));

        categoriesToCleanup.add(cat);
        // Create Category
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(format)
            .contentType(Formats.JSON)
            .body(json)
            .header("Authorization", user.toHeaderValue())
        .when()
            .post("/timeseries/category")
        .then()
            .statusCode(is(HttpServletResponse.SC_CREATED));

        // Verify it is there
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(format)
            .contentType(Formats.JSON)
            .queryParam(OFFICE, officeId)
        .when()
            .get("/timeseries/category/" + originalId)
        .then()
            .statusCode(is(HttpServletResponse.SC_OK));

        // Update Category (Rename and change description)
        TimeSeriesCategory updatedCat = new TimeSeriesCategory(officeId, updatedId, "UpdatedDescription");
        categoriesToCleanup.add(updatedCat);
        String updatedJson = Formats.format(contentType, updatedCat);
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(format)
            .contentType(Formats.JSON)
            .body(updatedJson)
            .header("Authorization", user.toHeaderValue())
        .when()
            .patch("/timeseries/category/" + originalId)
        .then()
            .statusCode(is(HttpServletResponse.SC_OK));

        // Read and verify update
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(format)
            .contentType(Formats.JSON)
            .queryParam(OFFICE, officeId)
        .when()
            .get("/timeseries/category/" + updatedId)
        .then()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("office-id", equalTo(updatedCat.getOfficeId()))
            .body("id", equalTo(updatedCat.getId()))
            .body("description", equalTo(updatedCat.getDescription()));

        // Verify old ID is gone
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(format)
            .contentType(Formats.JSON)
            .queryParam(OFFICE, officeId)
        .when()
            .get("/timeseries/category/" + originalId)
        .then()
            .statusCode(is(HttpServletResponse.SC_NOT_FOUND));

        // Delete Updated Category
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(format)
            .contentType(Formats.JSON)
            .header("Authorization", user.toHeaderValue())
            .queryParam(OFFICE, officeId)
            .queryParam(CASCADE_DELETE, "true")
        .when()
            .delete("/timeseries/category/" + updatedId)
        .then()
            .statusCode(is(HttpServletResponse.SC_NO_CONTENT));

        // Verify new ID is gone
        given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(format)
                .contentType(Formats.JSON)
                .queryParam(OFFICE, officeId)
        .when()
                .get("/timeseries/category/" + updatedId)
        .then()
                .statusCode(is(HttpServletResponse.SC_NOT_FOUND));
    }

    @ParameterizedTest
    @ValueSource(strings = {Formats.JSON, Formats.DEFAULT})
    void test_create_read_delete(String format) {
        String officeId = user.getOperatingOffice();
        TimeSeriesCategory cat = new TimeSeriesCategory(officeId, "test_create_read_delete", "IntegrationTesting");
        ContentType contentType = Formats.parseHeader(Formats.JSON, TimeSeriesCategory.class);
        String xml = Formats.format(contentType, cat);
        categoriesToCleanup.add(cat);
        //Create Category
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(format)
            .contentType(Formats.JSON)
            .body(xml)
            .header("Authorization", user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/timeseries/category")
        .then()
            .assertThat()
            .log().ifValidationFails(LogDetail.ALL,true)
            .statusCode(is(HttpServletResponse.SC_CREATED));
        //Read
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(format)
            .contentType(Formats.JSON)
            .queryParam(OFFICE, officeId)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/category/" + cat.getId())
        .then()
            .assertThat()
            .log().ifValidationFails(LogDetail.ALL,true)
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("office-id", equalTo(cat.getOfficeId()))
            .body("id", equalTo(cat.getId()))
            .body("description", equalTo(cat.getDescription()));
        //Delete
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(format)
            .contentType(Formats.JSON)
            .header("Authorization", user.toHeaderValue())
            .queryParam(OFFICE, officeId)
            .queryParam(CASCADE_DELETE, "true")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("/timeseries/category/" + cat.getId())
        .then()
            .assertThat()
            .log().ifValidationFails(LogDetail.ALL,true)
            .statusCode(is(HttpServletResponse.SC_NO_CONTENT));

        //Read Empty
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(format)
            .contentType(Formats.JSON)
            .queryParam("office", officeId)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/category/" + cat.getId())
        .then()
            .assertThat()
            .log().ifValidationFails(LogDetail.ALL,true)
            .statusCode(is(HttpServletResponse.SC_NOT_FOUND));
    }

    @ParameterizedTest
    @ValueSource(strings = {Formats.JSON, Formats.DEFAULT})
    void test_create_read_delete_new_LRTS_identifier(String format) {
        String officeId = user.getOperatingOffice();
        TimeSeriesCategory cat = new TimeSeriesCategory(officeId, "test_lrts_id", "IntegrationTesting");
        ContentType contentType = Formats.parseHeader(Formats.JSON, TimeSeriesCategory.class);
        String xml = Formats.format(contentType, cat);

        categoriesToCleanup.add(cat);
        //Create Category
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(format)
            .contentType(Formats.JSON)
            .body(xml)
            .header("Authorization", user.toHeaderValue())
            .header(ApiServlet.IS_NEW_LRTS, true)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/timeseries/category")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED));
        //Read
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(format)
            .contentType(Formats.JSON)
            .queryParam(OFFICE, officeId)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/category/" + cat.getId())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("office-id", equalTo(cat.getOfficeId()))
            .body("id", equalTo(cat.getId()))
            .body("description", equalTo(cat.getDescription()));
        //Delete
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(format)
            .contentType(Formats.JSON)
            .header("Authorization", user.toHeaderValue())
            .queryParam(OFFICE, officeId)
            .queryParam(CASCADE_DELETE, "true")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("/timeseries/category/" + cat.getId())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NO_CONTENT));

        //Read Empty
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(format)
            .contentType(Formats.JSON)
            .queryParam("office", officeId)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/category/" + cat.getId())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NOT_FOUND));
    }

    @ParameterizedTest
    @ValueSource(strings = {Formats.JSON, Formats.DEFAULT})
    void test_create_already_existing_CWMS_category(String format) {
        String officeId = user.getOperatingOffice();
        TimeSeriesCategory cat = new TimeSeriesCategory(officeId, "Default", "Default");
        ContentType contentType = Formats.parseHeader(Formats.JSON, TimeSeriesCategory.class);
        String xml = Formats.format(contentType, cat);

        //Attempt to Create Category, should fail
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(format)
            .contentType(Formats.JSON)
            .body(xml)
            .header("Authorization", user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/timeseries/category")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CONFLICT));
        //Read Empty
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(format)
            .contentType(Formats.JSON)
            .queryParam("office", officeId)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/category/" + cat.getId())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NOT_FOUND));
    }

    @ParameterizedTest
    @ValueSource(strings = {Formats.JSON, Formats.DEFAULT})
    @FunctionalSchemas(values = {"99.99.99.9-CDA_STAGING"})
    void test_create_read_delete_same_category_different_office(String format) throws Exception {
        String officeId = user.getOperatingOffice();
        String officeId2 = user2.getOperatingOffice();
        String timeSeriesId = "Alder Springs.Precip-Cumulative.Inst.15Minutes.0.raw-cda";
        createLocation(timeSeriesId.split("\\.")[0],true, officeId);
        createTimeseries(officeId,timeSeriesId);
        TimeSeriesCategory cat = new TimeSeriesCategory(officeId, "test_create_read_delete1", "IntegrationTesting");
        TimeSeriesCategory cat2 = new TimeSeriesCategory(officeId2, "test_create_read_delete1", "IntegrationTesting");
        categoriesToCleanup.add(cat);
        categoriesToCleanup.add(cat2);
        ContentType contentType = Formats.parseHeader(Formats.JSON, TimeSeriesCategory.class);
        String xml = Formats.format(contentType, cat);
        //Create Category
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(format)
            .contentType(Formats.JSON)
            .body(xml)
            .header("Authorization", user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/timeseries/category")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED));
        xml = Formats.format(contentType, cat2);
        //Create Category 2
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(format)
            .contentType(Formats.JSON)
            .body(xml)
            .header("Authorization", user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/timeseries/category")
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED));
        //Read
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(format)
            .contentType(Formats.JSON)
            .queryParam(OFFICE, officeId)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/category/" + cat.getId())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("office-id", equalTo(cat.getOfficeId()))
            .body("id", equalTo(cat.getId()))
            .body("description", equalTo(cat.getDescription()));
        // Read second category
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(format)
            .contentType(Formats.JSON)
            .queryParam(OFFICE, officeId2)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/category/" + cat2.getId())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("office-id", equalTo(cat2.getOfficeId()))
            .body("id", equalTo(cat2.getId()))
            .body("description", equalTo(cat2.getDescription()));
        //Delete
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(format)
            .contentType(Formats.JSON)
            .header("Authorization", user.toHeaderValue())
            .queryParam(OFFICE, officeId)
            .queryParam(CASCADE_DELETE, "true")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("/timeseries/category/" + cat.getId())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NO_CONTENT));
        //Delete second category
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(format)
            .contentType(Formats.JSON)
            .header("Authorization", user2.toHeaderValue())
            .queryParam(OFFICE, officeId2)
            .queryParam(CASCADE_DELETE, "true")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("/timeseries/category/" + cat2.getId())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NO_CONTENT));
        //Read Empty
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(format)
            .contentType(Formats.JSON)
            .queryParam("office", officeId)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/category/" + cat.getId())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NOT_FOUND));
        //Read second Empty
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(format)
            .contentType(Formats.JSON)
            .queryParam("office", officeId2)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/timeseries/category/" + cat2.getId())
        .then()
            .log().ifValidationFails(LogDetail.ALL,true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NOT_FOUND));
    }

    @ParameterizedTest
    @ValueSource(strings = {Formats.JSON, Formats.DEFAULT})
    void test_create_update_ignore_nulls(String format) {
        String officeId = user.getOperatingOffice();
        String catId = "test_ignore_nulls";
        TimeSeriesCategory cat = new TimeSeriesCategory(officeId, catId, "InitialDescription");
        ContentType contentType = Formats.parseHeader(Formats.JSON, TimeSeriesCategory.class);
        String json = Formats.format(contentType, cat);

        categoriesToCleanup.add(cat);
        // Create Category with ignore-nulls=false
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(format)
            .contentType(Formats.JSON)
            .queryParam(IGNORE_NULLS, "false")
            .body(json)
            .header("Authorization", user.toHeaderValue())
        .when()
            .post("/timeseries/category")
        .then()
            .statusCode(is(HttpServletResponse.SC_CREATED));

        // Update Category with ignore-nulls=true (default) and partial data
        // We use a JSON string to send nulls directly

        String partialUpdateJson = "{\"office-id\":\"" + officeId + "\", \"id\":\"" + catId + "\", \"description\":null}";
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(format)
            .contentType(Formats.JSON)
            .queryParam(IGNORE_NULLS, "true")
            .body(partialUpdateJson)
            .header("Authorization", user.toHeaderValue())
        .when()
            .patch("/timeseries/category/" + catId)
        .then()
            .statusCode(is(HttpServletResponse.SC_OK));

        // Verify description was NOT updated to null because ignore-nulls=true
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(format)
            .contentType(Formats.JSON)
            .queryParam(OFFICE, officeId)
        .when()
            .get("/timeseries/category/" + catId)
        .then()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("description", equalTo("InitialDescription"));

        // Update Category with ignore-nulls=false and null description
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(format)
            .contentType(Formats.JSON)
            .queryParam(IGNORE_NULLS, "false")
            .body(partialUpdateJson)
            .header("Authorization", user.toHeaderValue())
        .when()
            .patch("/timeseries/category/" + catId)
        .then()
            .statusCode(is(HttpServletResponse.SC_OK));

        // Verify description WAS updated to null
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(format)
            .contentType(Formats.JSON)
            .queryParam(OFFICE, officeId)
        .when()
            .get("/timeseries/category/" + catId)
        .then()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("description", is(org.hamcrest.Matchers.anyOf(equalTo(""), org.hamcrest.Matchers.nullValue())));

        // Delete Category
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(format)
            .contentType(Formats.JSON)
            .header("Authorization", user.toHeaderValue())
            .queryParam(OFFICE, officeId)
            .queryParam(CASCADE_DELETE, "true")
        .when()
            .delete("/timeseries/category/" + catId)
        .then()
            .statusCode(is(HttpServletResponse.SC_NO_CONTENT));
    }
}
