/*
 * MIT License
 *
 * Copyright (c) 2025 Hydrologic Engineering Center
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
package cwms.cda.api.rating;

import static cwms.cda.api.Controllers.AT;
import static cwms.cda.api.Controllers.END;
import static cwms.cda.api.Controllers.OFFICE;
import static cwms.cda.api.Controllers.REPLACE_BASE_CURVE;
import static cwms.cda.api.Controllers.STORE_TEMPLATE;
import cwms.cda.api.DataApiTestIT;
import static cwms.cda.api.rating.RatingsControllerTestIT.EXISTING_LOC;
import cwms.cda.api.rating.RatingsControllerTestIT.GetAllTest;
import static cwms.cda.api.rating.RatingsControllerTestIT.SPK;
import static cwms.cda.api.rating.RatingsControllerTestIT.cleanUp;
import static cwms.cda.api.rating.RatingsControllerTestIT.store;
import cwms.cda.formatters.Formats;
import fixtures.TestAccounts;
import hec.data.cwmsRating.io.RatingSetContainer;
import hec.data.cwmsRating.io.RatingSpecContainer;
import static io.restassured.RestAssured.given;
import io.restassured.filter.log.LogDetail;
import io.restassured.path.json.JsonPath;
import io.restassured.path.xml.XmlPath;
import io.restassured.response.Response;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.servlet.http.HttpServletResponse;
import mil.army.usace.hec.cwms.rating.io.xml.RatingContainerXmlFactory;
import mil.army.usace.hec.cwms.rating.io.xml.RatingSetContainerXmlFactory;
import mil.army.usace.hec.cwms.rating.io.xml.RatingSpecXmlFactory;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isEmptyString;
import org.junit.jupiter.api.AfterEach;

import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

@Tag("integration")
class RatingsControllerIT extends DataApiTestIT{
    private static final String OFFICE_ID = "office-id";
    private static final String MESSAGE = "message";
    private static final String IDENTIFIER = "identifier";

    @AfterEach
    void tearDown() {
        cleanUp();
    }

    static void storeOneSet(boolean storeTemplate) throws Exception {
        storeOneSet("cwms/cda/api/Zanesville_Stage_Flow_COE_Production.xml", "Zanesville", storeTemplate, false);
    }

    static void storeOneSet(String file, String locationString, boolean storeTemplate, boolean replaceBaseCurve) throws Exception {
        createLocation(EXISTING_LOC, true, SPK);
        String ratingXml = readResourceFile(file);
        ratingXml = ratingXml.replaceAll(locationString, EXISTING_LOC);
        String ratingXml2 = ratingXml;
        RatingSetContainer container = RatingSetContainerXmlFactory.ratingSetContainerFromXml(ratingXml2);
        RatingSpecContainer specContainer = container.ratingSpecContainer;
        specContainer.officeId = SPK;
        specContainer.specOfficeId = SPK;
        specContainer.locationId = EXISTING_LOC;
        String specXml = RatingSpecXmlFactory.toXml(specContainer, "", 0, true);
        String templateXml = RatingSpecXmlFactory.toXml(specContainer, "", 0);
        String setXml = RatingContainerXmlFactory.toXml(container, "", 0, true, false);
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

        //Create Template
        given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .contentType(Formats.XMLV2)
                .body(templateXml)
                .header("Authorization", user.toHeaderValue())
                .queryParam(OFFICE, SPK)
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
                .contentType(Formats.XMLV2)
                .body(specXml)
                .header("Authorization", user.toHeaderValue())
                .queryParam(OFFICE, SPK)
        .when()
                .redirects().follow(true)
                .redirects().max(3)
                .post("/ratings/spec")
        .then()
        .assertThat()
                .log().ifValidationFails(LogDetail.ALL,true)
                .statusCode(is(HttpServletResponse.SC_CREATED));

        //Create the set
        given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .contentType(Formats.XMLV2)
                .body(setXml)
                .header("Authorization", user.toHeaderValue())
                .queryParam(OFFICE, SPK)
                .queryParam(STORE_TEMPLATE, storeTemplate)
                .queryParam(REPLACE_BASE_CURVE, replaceBaseCurve)
        .when()
                .redirects().follow(true)
                .redirects().max(3)
                .post("/ratings")
        .then()
        .assertThat()
                .log().ifValidationFails(LogDetail.ALL,true)
                .statusCode(is(HttpServletResponse.SC_CREATED))
                .body(OFFICE_ID, equalTo(SPK))
                .body(MESSAGE, equalTo("Rating Set successfully stored to CWMS."))
                .body(IDENTIFIER, isEmptyString());
    }

    static void updateOneSet(String file, boolean replaceBaseCurve) throws Exception {
        createLocation(EXISTING_LOC, true, SPK);
        String ratingXml = readResourceFile(file);
        ratingXml = ratingXml.replaceAll("STJ-St_Joseph-Missouri", EXISTING_LOC);
        String ratingXml2 = ratingXml;
        RatingSetContainer container = RatingSetContainerXmlFactory.ratingSetContainerFromXml(ratingXml2);
        RatingSpecContainer specContainer = container.ratingSpecContainer;
        specContainer.officeId = SPK;
        specContainer.specOfficeId = SPK;
        specContainer.locationId = EXISTING_LOC;
        String specXml = RatingSpecXmlFactory.toXml(specContainer, "", 0, true);
        String templateXml = RatingSpecXmlFactory.toXml(specContainer, "", 0);
        String setXml = RatingContainerXmlFactory.toXml(container, "", 0, true, false);
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

        //Create Template
        given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .contentType(Formats.XMLV2)
                .body(templateXml)
                .header("Authorization", user.toHeaderValue())
                .queryParam(OFFICE, SPK)
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
                .contentType(Formats.XMLV2)
                .body(specXml)
                .header("Authorization", user.toHeaderValue())
                .queryParam(OFFICE, SPK)
        .when()
                .redirects().follow(true)
                .redirects().max(3)
                .post("/ratings/spec")
        .then()
        .assertThat()
                .log().ifValidationFails(LogDetail.ALL,true)
                .statusCode(is(HttpServletResponse.SC_CREATED));

        //Create the set
        given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .contentType(Formats.XMLV2)
                .body(setXml)
                .header("Authorization", user.toHeaderValue())
                .queryParam(OFFICE, SPK)
                .queryParam(STORE_TEMPLATE, false)
                .queryParam(REPLACE_BASE_CURVE, replaceBaseCurve)
        .when()
                .redirects().follow(true)
                .redirects().max(3)
                .patch("/ratings/STJ-St_Joseph-Missouri.Stage;Flow.USGS-BASE.Production")
        .then()
        .assertThat()
                .log().ifValidationFails(LogDetail.ALL,true)
                .statusCode(is(HttpServletResponse.SC_OK))
                .body(OFFICE_ID, equalTo(SPK))
                .body(MESSAGE, equalTo("Updated RatingSet"))
                .body(IDENTIFIER, isEmptyString());

    }

    @ParameterizedTest
    @EnumSource(GetAllTest.class)
    void test_getAll_storeTemplate(GetAllTest test) throws Exception {
        store(true);
        given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(test.accept)
        .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/ratings")
        .then()
        .assertThat()
                .log().ifValidationFails(LogDetail.ALL,true)
                .statusCode(is(HttpServletResponse.SC_OK))
                .contentType(is(test.expectedContentType));
    }

    @Test
    void test_fail_not_monotonic() throws Exception {
        DataApiTestIT.createLocation(EXISTING_LOC, true, SPK);
        String ratingXml = readResourceFile("cwms/cda/api/STJ-St_Joseph-Missouri_Stage_Flow_USGS-BASE_Production.xml");
        ratingXml = ratingXml.replaceAll("STJ-St_Joseph-Missouri", EXISTING_LOC);
        String ratingXml2 = ratingXml;
        RatingSetContainer container = RatingSetContainerXmlFactory.ratingSetContainerFromXml(ratingXml2);
        RatingSpecContainer specContainer = container.ratingSpecContainer;
        specContainer.officeId = SPK;
        specContainer.specOfficeId = SPK;
        specContainer.locationId = EXISTING_LOC;
        String specXml = RatingSpecXmlFactory.toXml(specContainer, "", 0, true);
        String templateXml = RatingSpecXmlFactory.toXml(specContainer, "", 0);
        String setXml = RatingContainerXmlFactory.toXml(container, "", 0, true, false);
        setXml = setXml.replace("0.4544681", "0.5544681"); //this will make things not increase monotonically
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

        //Create Template
        given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .contentType(Formats.XMLV2)
                .body(templateXml)
                .header("Authorization", user.toHeaderValue())
                .queryParam(OFFICE, SPK)
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
                .contentType(Formats.XMLV2)
                .body(specXml)
                .header("Authorization", user.toHeaderValue())
                .queryParam(OFFICE, SPK)
        .when()
                .redirects().follow(true)
                .redirects().max(3)
                .post("/ratings/spec")
        .then()
        .assertThat()
                .log().ifValidationFails(LogDetail.ALL,true)
                .statusCode(is(HttpServletResponse.SC_CREATED));

        //Create the set should fail since not monotonically increasing
        given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .contentType(Formats.XMLV2)
                .body(setXml)
                .header("Authorization", user.toHeaderValue())
                .queryParam(OFFICE, SPK)
                .queryParam(STORE_TEMPLATE, true)
                .queryParam(REPLACE_BASE_CURVE, true)
        .when()
                .redirects().follow(true)
                .redirects().max(3)
                .post("/ratings")
        .then()
        .assertThat()
                .statusCode(is(HttpServletResponse.SC_INTERNAL_SERVER_ERROR));
    }

    @ParameterizedTest
    @EnumSource(GetAllTest.class)
    @SuppressWarnings("unchecked")
    void test_multiple_store_with_store_template(GetAllTest test) throws Exception {
        store(true);
        storeOneSet(true);
        Response response = given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(test.accept)
                .queryParam(AT, "2001-04-09T13:53:01Z")
                .queryParam(END, "2086-06-06T00:00:00Z")
        .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/ratings")
        .then()
        .assertThat()
                .log().ifValidationFails(LogDetail.ALL,true)
                .statusCode(is(HttpServletResponse.SC_OK))
                .contentType(is(test.expectedContentType))
                .extract()
                .response();
        List<String> dates = new ArrayList<>();

        if (response.contentType().contains("json")) {
            Map<String, Object> root = response.jsonPath().get("ratings");
            List<Map<String, Object>>  ratings = ((List<Map<String, Object>>)root.get("ratings"));
            for(Map<String, Object> rating : ratings) {
                Map<String, Object> simpleRating = (Map<String, Object>) rating.get("simple-rating");
                if (simpleRating != null && simpleRating.get("effective-date") != null) {
                    dates.add((String) simpleRating.get("effective-date"));
                }
            }
        } else if (response.contentType().contains("xml")) {
            dates = response.xmlPath().getList("ratings.simple-rating.effective-date");
        }

        //verifying calling multiple stores does not overwrite existing set, so checking original effective-dates are still present
        assertEquals(1, Collections.frequency(dates, "2016-06-06T00:00:00Z"));
        assertEquals(1, Collections.frequency(dates, "2085-06-06T00:00:00Z"));
    }

    @ParameterizedTest
    @EnumSource(GetAllTest.class)
    @SuppressWarnings("unchecked")
    void test_multiple_store(GetAllTest test) throws Exception {
        store(true);
        storeOneSet(false);
        Response response = given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(test.accept)
                .queryParam(AT, "2001-04-09T13:53:01Z")
                .queryParam(END, "2086-06-06T00:00:00Z")
        .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/ratings")
        .then()
        .assertThat()
                .log().ifValidationFails(LogDetail.ALL,true)
                .statusCode(is(HttpServletResponse.SC_OK))
                .contentType(is(test.expectedContentType))
                .extract()
                .response();
        List<String> dates = new ArrayList<>();

        if (response.contentType().contains("json")) {
            dates = ((List<?>) ((Map<String,Object>)response.jsonPath().get("ratings")).get("ratings")).stream()
                    .map(rating -> (Map<String, Object>) ((Map<?, ?>) rating).get("simple-rating"))
                    .filter(Objects::nonNull)
                    .map(sr -> (String) sr.get("effective-date"))
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
        } else if (response.contentType().contains("xml")) {
            dates = response.xmlPath().getList("ratings.simple-rating.effective-date");
        }

        //verifying calling multiple stores does not overwrite existing set, so checking original effective-dates are still present
        assertEquals(1, Collections.frequency(dates, "2016-06-06T00:00:00Z"));
        assertEquals(1, Collections.frequency(dates, "2085-06-06T00:00:00Z"));
    }

    @ParameterizedTest
    @EnumSource(GetAllTest.class)
    void test_multiple_store_replace_base(GetAllTest test) throws Exception {
        storeOneSet("cwms/cda/api/STJ-St_Joseph-Missouri_Stage_Flow_USGS-BASE_Production.xml", "STJ-St_Joseph-Missouri", true, true);
        Response response = given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(test.accept)
                .queryParam(AT, "2001-04-09T13:53:01Z")
                .queryParam(END, "2086-06-06T00:00:00Z")
        .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/ratings")
        .then()
        .assertThat()
                .log().ifValidationFails(LogDetail.ALL,true)
                .statusCode(is(HttpServletResponse.SC_OK))
                .contentType(is(test.expectedContentType))
                .extract()
                .response();
        List<String> dates = new ArrayList<>();
        List<String> shifts = new ArrayList<>();
        List<String> ratingPoints = new ArrayList<>();
        Object description = null;

        if (response.contentType().contains("json")) {
            JsonPath json = response.jsonPath();
            dates = extractJsonDates(json);
            shifts = extractJsonShifts(json);
            ratingPoints = extractJsonRatingPoints(json);
            description = json.getList("ratings.ratings.usgs-stream-rating.description").get(0);
        } else if (response.contentType().contains("xml")) {
            XmlPath xml = response.xmlPath();
            dates = xml.getList("ratings.usgs-stream-rating.effective-date");
            description = xml.get("ratings.usgs-stream-rating.description");
            shifts = xml.getList("ratings.usgs-stream-rating.shifts");
            ratingPoints = xml.getList("ratings.usgs-stream-rating.rating-points");
        }

        assertEquals(1, Collections.frequency(dates, "2016-06-11T05:00:00Z"));
        assertEquals("11.0", description);
        String firstShift = shifts.get(0).trim();
        double shiftVal1 = Double.parseDouble(firstShift.split(" ")[0]);
        double shiftVal2 = Double.parseDouble(firstShift.split(" ")[1]);
        assertEquals(0.05, shiftVal1);
        assertEquals(0.0, shiftVal2);
        String firstRatingPoint = ratingPoints.get(0).split("\n")[0];
        double ratingVal1 = Double.parseDouble(firstRatingPoint.split(" ")[0]);
        double ratingVal2 = Double.parseDouble(firstRatingPoint.split(" ")[1]);
        assertEquals(0.4544681, ratingVal1, 0.00001);
        assertEquals(14700, ratingVal2, 0.00001);

        //Update with replace-base set to false... this should update the shifts but not the base curve
        updateOneSet("cwms/cda/api/STJ-St_Joseph-Missouri_Stage_Flow_USGS-BASE_Production_Changed.xml", false);
        response = given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(test.accept)
                .queryParam(AT, "2001-04-09T13:53:01Z")
                .queryParam(END, "2086-06-06T00:00:00Z")
        .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/ratings")
        .then()
        .assertThat()
                .log().ifValidationFails(LogDetail.ALL,true)
                .statusCode(is(HttpServletResponse.SC_OK))
                .contentType(is(test.expectedContentType))
                .extract()
                .response();
        dates = new ArrayList<>();
        shifts = new ArrayList<>();
        ratingPoints = new ArrayList<>();
        if (response.contentType().contains("json")) {
            JsonPath json = response.jsonPath();
            dates = extractJsonDates(json);
            shifts = extractJsonShifts(json);
            ratingPoints = extractJsonRatingPoints(json);
            description = json.getList("ratings.ratings.usgs-stream-rating.description").get(0);
        } else if (response.contentType().contains("xml")) {
            XmlPath xml = response.xmlPath();
            dates = xml.getList("ratings.usgs-stream-rating.effective-date");
            description = xml.get("ratings.usgs-stream-rating.description");
            shifts = xml.getList("ratings.usgs-stream-rating.shifts");
            ratingPoints = xml.getList("ratings.usgs-stream-rating.rating-points");
        }

        assertEquals(1, Collections.frequency(dates, "2016-06-11T05:00:00Z"));
        assertEquals("11.0", description);
        firstShift = shifts.get(0).trim();
        shiftVal1 = Double.parseDouble(firstShift.split(" ")[0]);
        shiftVal2 = Double.parseDouble(firstShift.split(" ")[1]);
        assertEquals(1, shiftVal1);
        assertEquals(2, shiftVal2);
        firstRatingPoint = ratingPoints.get(0).split("\n")[0];
        ratingVal1 = Double.parseDouble(firstRatingPoint.split(" ")[0]);
        ratingVal2 = Double.parseDouble(firstRatingPoint.split(" ")[1]);
        assertEquals(0.4544681, ratingVal1, 0.00001);
        assertEquals(14700, ratingVal2, 0.00001);

        //update with replace-base set to true - this will update the base curve, so verify the ratings point is changed
        updateOneSet("cwms/cda/api/STJ-St_Joseph-Missouri_Stage_Flow_USGS-BASE_Production_Changed.xml", true);
        response = given()
                .log().ifValidationFails(LogDetail.ALL,true)
                .accept(test.accept)
                .queryParam(AT, "2001-04-09T13:53:01Z")
                .queryParam(END, "2086-06-06T00:00:00Z")
        .when()
                .redirects().follow(true)
                .redirects().max(3)
                .get("/ratings")
        .then()
        .assertThat()
                .log().ifValidationFails(LogDetail.ALL,true)
                .statusCode(is(HttpServletResponse.SC_OK))
                .contentType(is(test.expectedContentType))
                .extract()
                .response();
        dates = new ArrayList<>();
        shifts = new ArrayList<>();
        if (response.contentType().contains("json")) {
            JsonPath json = response.jsonPath();
            dates = extractJsonDates(json);
            shifts = extractJsonShifts(json);
            ratingPoints = extractJsonRatingPoints(json);
            description = json.getList("ratings.ratings.usgs-stream-rating.description").get(0);
        } else if (response.contentType().contains("xml")) {
            XmlPath xml = response.xmlPath();
            dates = xml.getList("ratings.usgs-stream-rating.effective-date");
            description = xml.get("ratings.usgs-stream-rating.description");
            shifts = xml.getList("ratings.usgs-stream-rating.shifts");
            ratingPoints = xml.getList("ratings.usgs-stream-rating.rating-points");
        }

        assertEquals(1, Collections.frequency(dates, "2016-06-11T05:00:00Z"));
        assertEquals("12.0", description);
        firstShift = shifts.get(0).trim();
        shiftVal1 = Double.parseDouble(firstShift.split(" ")[0]);
        shiftVal2 = Double.parseDouble(firstShift.split(" ")[1]);
        assertEquals(1, shiftVal1);
        assertEquals(2, shiftVal2);
        //verify that the base curve HAS changed with replace base flag set to true
        firstRatingPoint = ratingPoints.get(0).split("\n")[0];
        ratingVal1 = Double.parseDouble(firstRatingPoint.split(" ")[0]);
        ratingVal2 = Double.parseDouble(firstRatingPoint.split(" ")[1]);
        assertEquals(1.4544681, ratingVal1, 0.00001);
        assertEquals(14800, ratingVal2, 0.00001);

        //resets description and shifts to original values since this test is iterating over different formats
        updateOneSet("cwms/cda/api/STJ-St_Joseph-Missouri_Stage_Flow_USGS-BASE_Production.xml", true);
    }

    @SuppressWarnings("unchecked")
    private static List<String> extractJsonRatingPoints(JsonPath json) {
        return ((List<?>) ((Map<String, Object>) json.get("ratings")).get("ratings")).stream()
                .map(rating -> (Map<String, Object>) ((Map<?, ?>) rating).get("usgs-stream-rating"))
                .filter(Objects::nonNull)
                .map(sr -> (List<List<Number>>) sr.get("values"))
                .filter(Objects::nonNull)
                .flatMap(List::stream) // Stream<List<Number>>
                .map(pair -> pair.stream()
                        .map(String::valueOf)
                        .collect(Collectors.joining(" ")))
                .collect(Collectors.toList());
    }

    @SuppressWarnings("unchecked")
    private List<String> extractJsonShifts(JsonPath json) {
        return ((List<?>) ((Map<String, Object>) json.get("ratings")).get("ratings")).stream()
                .map(rating -> (Map<String, Object>) ((Map<?, ?>) rating).get("usgs-stream-rating"))
                .filter(Objects::nonNull)
                .flatMap(sr -> {
                    List<Map<String, Object>> shifts2 = (List<Map<String, Object>>) sr.get("shifts");
                    return shifts2 != null ? shifts2.stream() : Stream.empty();
                })
                .map(shift -> (List<List<Number>>) shift.get("values"))
                .filter(Objects::nonNull)
                .flatMap(List::stream)
                .map(pair -> pair.stream()
                        .map(String::valueOf)
                        .collect(Collectors.joining(" ")))
                .collect(Collectors.toList());
    }

    @SuppressWarnings("unchecked")
    private static List<String> extractJsonDates(JsonPath json) {
        return ((List<?>) ((Map<String, Object>) json.get("ratings")).get("ratings")).stream()
                .map(rating -> (Map<String, Object>) ((Map<?, ?>) rating).get("usgs-stream-rating"))
                .filter(Objects::nonNull)
                .map(sr -> (String) sr.get("effective-date"))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

}
