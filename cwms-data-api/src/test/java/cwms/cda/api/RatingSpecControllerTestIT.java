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

import fixtures.TestAccounts;
import hec.data.cwmsRating.io.RatingSpecContainer;
import io.restassured.filter.log.LogDetail;
import io.restassured.path.json.JsonPath;
import io.restassured.response.Response;
import mil.army.usace.hec.cwms.rating.io.xml.RatingSpecXmlFactory;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dto.rating.RatingSpec;
import cwms.cda.formatters.Formats;

import javax.servlet.http.HttpServletResponse;
import java.util.stream.IntStream;

import static cwms.cda.api.Controllers.METHOD;
import static cwms.cda.api.Controllers.OFFICE;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("integration")
class RatingSpecControllerTestIT extends DataApiTestIT {

    @Test
    void test_empty_rating_spec() throws Exception {
        String locationId = "RatingSpecTestEmpty";
        String officeId = "SPK";
        createLocation(locationId, true, officeId);
        String ratingXml = readResourceFile("cwms/cda/api/empty_rating_spec.xml");
        RatingSpecContainer specContainer = RatingSpecXmlFactory.ratingSpecContainer(ratingXml);
        specContainer.officeId = officeId;
        specContainer.specOfficeId = officeId;
        specContainer.locationId = locationId;
        specContainer.specId = specContainer.specId.replace("TEST", locationId);
        String specXml = RatingSpecXmlFactory.toXml(specContainer, "", 0, true);
        String templateXml = RatingSpecXmlFactory.toXml(specContainer, "", 0);
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;
        //Create Template
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV2)
            .contentType(Formats.XMLV2)
            .body(templateXml)
            .header("Authorization", user.toHeaderValue())
            .queryParam(OFFICE, officeId)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/ratings/template")
        .then()
            .assertThat()
            .log().ifValidationFails(LogDetail.ALL,true)
            .statusCode(is(HttpServletResponse.SC_CREATED));
        //Create Spec
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV2)
            .contentType(Formats.XMLV2)
            .body(specXml)
            .header("Authorization", user.toHeaderValue())
            .queryParam(OFFICE, officeId)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/ratings/spec")
        .then()
            .assertThat()
            .log().ifValidationFails(LogDetail.ALL,true)
            .statusCode(is(HttpServletResponse.SC_CREATED));

        //Read
        Response response =
            given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(Formats.JSONV2)
                .contentType(Formats.JSONV2)
                .header("Authorization", user.toHeaderValue())
                .queryParam("office", officeId)
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/ratings/metadata");
            // then follows
        RatingSpec ratingSpec = new RatingSpec(new RatingSpec.Builder().fromRatingSpec(new hec.data.cwmsRating.RatingSpec(specContainer)));
        JsonPath path = new JsonPath(response.asString());
        //get values of JSON array after getting array size
        boolean foundMatching = IntStream.range(0, path.getInt("rating-metadata.size()"))
                .mapToObj(i -> path.getObject("rating-metadata[" + i + "].rating-spec", RatingSpec.class))
                .anyMatch(s -> s.hashCode() == ratingSpec.hashCode());
        assertTrue(foundMatching);
        //Delete
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .header("Authorization", user.toHeaderValue())
            .queryParam(OFFICE, officeId)
            .queryParam(METHOD, JooqDao.DeleteMethod.DELETE_ALL)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("/ratings/spec/" + specContainer.specId)
        .then()
            .assertThat()
            .log().ifValidationFails(LogDetail.ALL,true)
            .statusCode(is(HttpServletResponse.SC_NO_CONTENT));
    }

    @Test
    void test_create_read_delete() throws Exception {
        String locationId = "RatingSpecTest";
        String officeId = "SPK"; // TODO: sort out office id usage
        createLocation(locationId, true, officeId);
        String ratingXml = readResourceFile("cwms/cda/api/Zanesville_Stage_Flow_COE_Production.xml");
        RatingSpecContainer specContainer = RatingSpecXmlFactory.ratingSpecContainer(ratingXml);
        specContainer.officeId = officeId;
        specContainer.specOfficeId = officeId;
        specContainer.locationId = locationId;
        specContainer.specId = specContainer.specId.replace("Zanesville", locationId);
        String specXml = RatingSpecXmlFactory.toXml(specContainer, "", 0, true);
        String templateXml = RatingSpecXmlFactory.toXml(specContainer, "", 0);
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;
        //Create Template
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV2)
            .contentType(Formats.XMLV2)
            .body(templateXml)
            .header("Authorization", user.toHeaderValue())
            .queryParam(OFFICE, officeId)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/ratings/template")
        .then()
            .assertThat()
            .log().ifValidationFails(LogDetail.ALL,true)
            .statusCode(is(HttpServletResponse.SC_CREATED));
        //Create Spec
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV2)
            .contentType(Formats.XMLV2)
            .body(specXml)
            .header("Authorization", user.toHeaderValue())
            .queryParam(OFFICE, officeId)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/ratings/spec")
        .then()
            .assertThat()
            .log().ifValidationFails(LogDetail.ALL,true)
            .statusCode(is(HttpServletResponse.SC_CREATED));

        //Read
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .header("Authorization", user.toHeaderValue())
            .queryParam("office", officeId)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/ratings/spec/" + specContainer.specId)
        .then()
            .assertThat()
            .log().ifValidationFails(LogDetail.ALL,true)
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("rating-id", equalTo(specContainer.specId))
            .body("office-id", equalTo(specContainer.officeId))
            .body("template-id", equalTo(specContainer.templateId))
            .body("in-range-method", equalTo(specContainer.inRangeMethod))
            .body("out-range-low-method", equalTo(specContainer.outRangeLowMethod))
            .body("out-range-high-method", equalTo(specContainer.outRangeHighMethod));
        //Delete
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .header("Authorization", user.toHeaderValue())
            .queryParam(OFFICE, officeId)
            .queryParam(METHOD, JooqDao.DeleteMethod.DELETE_ALL)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("/ratings/spec/" + specContainer.specId)
        .then()
            .assertThat()
            .log().ifValidationFails(LogDetail.ALL,true)
            .statusCode(is(HttpServletResponse.SC_NO_CONTENT));

        //Read Empty
        given()
            .log().ifValidationFails(LogDetail.ALL,true)
            .accept(Formats.JSONV2)
            .contentType(Formats.JSONV2)
            .header("Authorization", user.toHeaderValue())
            .queryParam("office", officeId)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/ratings/spec/" + specContainer.specId)
        .then()
            .assertThat()
            .log().ifValidationFails(LogDetail.ALL,true)
            .statusCode(is(HttpServletResponse.SC_NOT_FOUND));
    }
}
