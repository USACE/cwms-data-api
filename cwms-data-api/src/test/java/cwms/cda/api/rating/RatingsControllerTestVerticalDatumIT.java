/*
 * MIT License
 * Copyright (c) 2025 Hydrologic Engineering Center
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package cwms.cda.api.rating;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import cwms.cda.api.DataApiTestIT;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dao.VerticalDatum;
import cwms.cda.data.dto.VerticalDatumInfo;
import cwms.cda.formatters.Formats;
import fixtures.TestAccounts;
import hec.data.cwmsRating.AbstractRating;
import hec.data.cwmsRating.RatingSet;
import hec.data.cwmsRating.io.RatingSetContainer;
import hec.data.cwmsRating.io.RatingSpecContainer;
import io.restassured.filter.log.LogDetail;
import io.restassured.response.ExtractableResponse;
import io.restassured.response.Response;
import java.io.IOException;
import java.util.stream.Stream;
import javax.servlet.http.HttpServletResponse;
import mil.army.usace.hec.cwms.rating.io.xml.RatingSetContainerXmlFactory;
import mil.army.usace.hec.cwms.rating.io.xml.RatingSpecXmlFactory;
import mil.army.usace.hec.cwms.rating.io.xml.RatingXmlFactory;
import mil.army.usace.hec.metadata.VerticalDatumContainer;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import static cwms.cda.api.Controllers.*;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("integration")
class RatingsControllerTestVerticalDatumIT extends DataApiTestIT {
    static final String BASE_LOCATION = "RatingDatumTest";
    static final String LOC_WITH_NAVD88 = BASE_LOCATION + "-NAVD88";
    static final String LOC_WITH_NGVD29 = BASE_LOCATION + "-NGVD29";
    static final String TEMPLATE = "Elev;Area.Standard";
    static final String SPK = "SPK";

    @BeforeAll
    static void beforeAll() throws Exception {
        //Make sure we always have something.
        createLocation(BASE_LOCATION, true, SPK);
        createLocationWithVerticalDatum(LOC_WITH_NAVD88, true, SPK, VerticalDatum.NAVD88);
        createLocationWithVerticalDatum(LOC_WITH_NGVD29, true, SPK, VerticalDatum.NGVD29);

        String xml = readVerticalDatumRatingXml(BASE_LOCATION);
        RatingSetContainer container = RatingSetContainerXmlFactory.ratingSetContainerFromXml(xml);
        RatingSpecContainer specContainer = container.ratingSpecContainer;
        String templateXml = RatingSpecXmlFactory.toXml(specContainer, "", 0);
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;
        String specXml = RatingSpecXmlFactory.toXml(specContainer, "", 0, true);

        createTemplate(templateXml, user);

        createSpec(specXml, user);
    }

    @Test
    void test_create_with_datum_param_differs_from_location_native_datum() throws Exception {
        // Verify RatingsController create (POST /ratings) accepts the datum query parameter
        // when the input rating XML does not include vertical-datum-info.

        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

        // Build rating XML for BASE_LOCATION and strip datum info
        String xmlWithDatum = readVerticalDatumRatingXml(LOC_WITH_NGVD29);
        String xml = stripVerticalDatumInfo(xmlWithDatum);

        RatingSet ratingSet = RatingXmlFactory.ratingSet(xmlWithDatum);
        String ratingId = ratingSet.getRatingSpec().getRatingSpecId();
        XmlMapper xmlMapper = new XmlMapper();
        JsonNode root = xmlMapper.readTree(xmlWithDatum);
        JsonNode firstIndNode = root
                .path("simple-rating")
                .path("rating-points")
                .path("point")
                .get(0)
                .path("ind");
        double firstElev = firstIndNode.asDouble();
        JsonNode vdiNode = root
                .path("simple-rating")
                .path("vertical-datum-info");
        VerticalDatumInfo vdi = xmlMapper.treeToValue(vdiNode, VerticalDatumInfo.class);
        VerticalDatumInfo.Offset offset = vdi.getOffsetForDatum(VerticalDatum.NAVD88);

        // First create with a datum that doesn't match native datum for location
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.XMLV2)
            .body(xml) //using xml with no datum info so param is used
            .header("Authorization", user.toHeaderValue())
            .queryParam(OFFICE, SPK)
            .queryParam(DATUM, VerticalDatum.NAVD88)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/ratings")
        .then()
        .assertThat()
            .log().ifValidationFails(LogDetail.ALL, true)
            .statusCode(is(HttpServletResponse.SC_CREATED));

        // 2) verify elevation is as expected
        Response response = given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.XMLV2)
            .queryParam(OFFICE, SPK)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/ratings/" + ratingId);
        response.then()
        .assertThat()
            .log().ifValidationFails(LogDetail.ALL, true)
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("ratings.simple-rating.rating-points.point[0].ind.toDouble()", closeTo(firstElev + offset.getValue(), 0.001));

        deleteRatingEffectiveDates(user, ratingId);
    }

    @Test
    void test_update_datum_not_in_body_uses_param() throws Exception {
        // Verify RatingsController update/store (PATCH /ratings/{id}) accepts the datum query parameter
        // when the input rating XML does not include vertical-datum-info.

        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

        // Build rating XML for BASE_LOCATION and strip datum info
        String xmlWithDatum = readVerticalDatumRatingXml(LOC_WITH_NGVD29);
        String xml = stripVerticalDatumInfo(xmlWithDatum);

        RatingSet ratingSet = RatingXmlFactory.ratingSet(xmlWithDatum);
        String ratingId = ratingSet.getRatingSpec().getRatingSpecId();
        XmlMapper xmlMapper = new XmlMapper();
        JsonNode root = xmlMapper.readTree(xmlWithDatum);
        JsonNode firstIndNode = root
                .path("simple-rating")
                .path("rating-points")
                .path("point")
                .get(0)
                .path("ind");
        double firstElev = firstIndNode.asDouble();
        JsonNode vdiNode = root
                .path("simple-rating")
                .path("vertical-datum-info");
        VerticalDatumInfo vdi = xmlMapper.treeToValue(vdiNode, VerticalDatumInfo.class);
        VerticalDatumInfo.Offset offset = vdi.getOffsetForDatum(VerticalDatum.NAVD88);

        // First POST to ensure the rating exists
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.XMLV2)
            .body(xmlWithDatum)
            .header("Authorization", user.toHeaderValue())
            .queryParam(OFFICE, SPK)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/ratings")
        .then()
        .assertThat()
            .log().ifValidationFails(LogDetail.ALL, true)
            .statusCode(is(HttpServletResponse.SC_CREATED));

        // 2) verify elevation is as expected
        Response response = given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.XMLV2)
            .queryParam(OFFICE, SPK)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/ratings/" + ratingId);
        response.then()
        .assertThat()
            .log().ifValidationFails(LogDetail.ALL, true)
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("ratings.simple-rating.rating-points.point[0].ind.toDouble()", closeTo(firstElev, 0.001));


        // 3) PATCH with datum = NAVD88 and no datum info in body means we apply offset, so value changes
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.XMLV2)
            .body(xml)
            .header("Authorization", user.toHeaderValue())
            .queryParam(OFFICE, SPK)
            .queryParam(DATUM, VerticalDatum.NAVD88)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .patch("/ratings/" + ratingId)
        .then()
        .assertThat()
            .log().ifValidationFails(LogDetail.ALL, true)
            .statusCode(is(HttpServletResponse.SC_OK));

        // 4) retrieve rating and verify value is changed as expected
        response = given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.XMLV2)
            .queryParam(OFFICE, SPK)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/ratings/" + ratingId);
        response.then()
        .assertThat()
            .log().ifValidationFails(LogDetail.ALL, true)
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("ratings.simple-rating.rating-points.point[0].ind.toDouble()", closeTo(firstElev + offset.getValue(), 0.001));

        deleteRatingEffectiveDates(user, ratingId);
    }

    @Test
    void test_update_with_datum_in_body_ignores_param() throws Exception {
        // Verify RatingsController update/store (PATCH /ratings/{id}) ignores the datum query parameter
        // when the input rating XML does include vertical-datum-info.

        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

        // Build rating XML for BASE_LOCATION and strip datum info
        String xmlWithDatum = readVerticalDatumRatingXml(LOC_WITH_NGVD29);

        RatingSet ratingSet = RatingXmlFactory.ratingSet(xmlWithDatum);
        String ratingId = ratingSet.getRatingSpec().getRatingSpecId();
        XmlMapper xmlMapper = new XmlMapper();
        JsonNode root = xmlMapper.readTree(xmlWithDatum);
        JsonNode firstIndNode = root
                .path("simple-rating")
                .path("rating-points")
                .path("point")
                .get(0)
                .path("ind");
        double firstElev = firstIndNode.asDouble();

        // First POST to ensure the rating exists
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.XMLV2)
            .body(xmlWithDatum)
            .header("Authorization", user.toHeaderValue())
            .queryParam(OFFICE, SPK)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/ratings")
        .then()
        .assertThat()
            .log().ifValidationFails(LogDetail.ALL, true)
            .statusCode(is(HttpServletResponse.SC_CREATED));

        // 2) verify elevation is as expected
        Response response = given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.XMLV2)
            .queryParam(OFFICE, SPK)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/ratings/" + ratingId);
        response.then()
        .assertThat()
            .log().ifValidationFails(LogDetail.ALL, true)
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("ratings.simple-rating.rating-points.point[0].ind.toDouble()", closeTo(firstElev, 0.001));


        // 3) PATCH with datum = NAVD88 but it should be ignored since body contains datum info
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.XMLV2)
            .body(xmlWithDatum)
            .header("Authorization", user.toHeaderValue())
            .queryParam(OFFICE, SPK)
            .queryParam(DATUM, VerticalDatum.NAVD88)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .patch("/ratings/" + ratingId)
        .then()
        .assertThat()
            .log().ifValidationFails(LogDetail.ALL, true)
            .statusCode(is(HttpServletResponse.SC_OK));

        // 4) retrieve rating and verify elevation is still as expected
        response = given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.XMLV2)
            .queryParam(OFFICE, SPK)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/ratings/" + ratingId);
        response.then()
        .assertThat()
            .log().ifValidationFails(LogDetail.ALL, true)
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("ratings.simple-rating.rating-points.point[0].ind.toDouble()", closeTo(firstElev, 0.001));

        deleteRatingEffectiveDates(user, ratingId);
    }

    private static void createSpec(String specXml, TestAccounts.KeyUser user) {
        given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .contentType(Formats.XMLV2)
                .body(specXml)
                .header("Authorization", user.toHeaderValue())
                .queryParam(OFFICE, SPK)
            .when()
                .redirects()
                .follow(true)
                .redirects()
                .max(3)
                .post("/ratings/spec")
            .then()
                .assertThat()
                .log().ifValidationFails(LogDetail.ALL, true)
                .statusCode(is(HttpServletResponse.SC_CREATED));
    }

    private static void createTemplate(String templateXml, TestAccounts.KeyUser user) {
        given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .contentType(Formats.XMLV2)
                .body(templateXml)
                .header("Authorization", user.toHeaderValue())
                .queryParam(OFFICE, SPK)
            .when()
                .redirects()
                .follow(true)
                .redirects()
                .max(3)
                .post("/ratings/template")
            .then()
                .assertThat()
                .log().ifValidationFails(LogDetail.ALL, true)
                .statusCode(is(HttpServletResponse.SC_CREATED));
    }

    @AfterAll
    static void cleanUp() {
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

        // Delete Template
        given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .contentType(Formats.XMLV2)
                .header("Authorization", user.toHeaderValue())
                .queryParam(OFFICE, SPK)
                .queryParam(METHOD, JooqDao.DeleteMethod.DELETE_ALL)
            .when()
                .redirects()
                .follow(true)
                .redirects()
                .max(3)
                .delete("/ratings/template/" + TEMPLATE)
            .then()
                .log().ifValidationFails(LogDetail.ALL, true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_NO_CONTENT));
    }

    @MethodSource(value = "provideDatumCombinations")
    @ParameterizedTest
    void test_vertical_datum_get_all(TestLocationIds locId, TestLocationVerticalDatumData testData) throws Exception {
        String xml = readVerticalDatumRatingXml(locId._locationId);
        XmlMapper xmlMapper = new XmlMapper();
        JsonNode root = xmlMapper.readTree(xml);
        JsonNode vdiNode = root
                .path("simple-rating")
                .path("vertical-datum-info");
        VerticalDatumInfo vdi = xmlMapper.treeToValue(vdiNode, VerticalDatumInfo.class);
        vdi = vdi.convertedTo(vdi.getOffsetForDatum(locId._nativeDatum));
        String newVdiXml = xmlMapper.writeValueAsString(vdi);
        xml = xml.replaceAll("<vertical-datum-info[\\s\\S]*?</vertical-datum-info>", newVdiXml);
        RatingSet originalRatingSet = RatingXmlFactory.ratingSet(xml);
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;
        String ratingId = originalRatingSet.getRatingSpec().getRatingSpecId();
        AbstractRating originalRating = originalRatingSet.getRatings()[0];
        originalRating.setVerticalDatumContainer(null);

        storeRatingFromXml(xml, user);

        //Request the one rating id we stored, using the getAll endpoint with a query param filter
        String requestedVerticalDatum = testData._requestedVerticalDatum == null ? "" : testData._requestedVerticalDatum.toString();
        ExtractableResponse<Response> response = given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .contentType(Formats.XMLV2)
                .queryParam(OFFICE, SPK)
                .queryParam(DATUM, requestedVerticalDatum)
                .queryParam(NAME, ratingId)
            .when()
                .redirects()
                .follow(true)
                .redirects()
                .max(3)
                .get("/ratings")
            .then()
                .assertThat()
                .log().ifValidationFails(LogDetail.ALL, true)
                .statusCode(is(HttpServletResponse.SC_OK))
                .contentType(is(Formats.XMLV2))
                .extract();

        deleteRatingEffectiveDates(user, ratingId);
    }

    @MethodSource(value = "provideDatumCombinations")
    @ParameterizedTest
    void test_vertical_datum_get_one(TestLocationIds locId, TestLocationVerticalDatumData testData) throws Exception {
        //This tests getting a rating with various combinations of native location datum and requested datum
        //Storing a rating without any vertical datum info, then requesting it back with various datum requests
        String xml = readVerticalDatumRatingXml(locId._locationId);
        XmlMapper xmlMapper = new XmlMapper();
        JsonNode root = xmlMapper.readTree(xml);
        JsonNode vdiNode = root
                .path("simple-rating")
                .path("vertical-datum-info");
        VerticalDatumInfo vdi = xmlMapper.treeToValue(vdiNode, VerticalDatumInfo.class);
        vdi = vdi.convertedTo(vdi.getOffsetForDatum(locId._nativeDatum));
        String newVdiXml = xmlMapper.writeValueAsString(vdi);
        xml = xml.replaceAll("<vertical-datum-info[\\s\\S]*?</vertical-datum-info>", newVdiXml);
        RatingSet originalRatingSet = RatingXmlFactory.ratingSet(xml);
        double firstElev = originalRatingSet.getRatings()[0].getValues(0)[0].getIndValue();
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;
        String ratingId = originalRatingSet.getRatingSpec().getRatingSpecId();
        AbstractRating originalRating = originalRatingSet.getRatings()[0];
        originalRating.setVerticalDatumContainer(null);

        double expectedFirstElev = firstElev;
        if (testData._requestedVerticalDatum == VerticalDatum.NATIVE || testData._requestedVerticalDatum == null || locId._nativeDatum == null) {
            expectedFirstElev = firstElev;
        }
        else if(testData._requestedVerticalDatum != locId._nativeDatum) {
            //Need to apply offset

            VerticalDatumInfo.Offset offset = vdi.getOffsetForDatum(testData._requestedVerticalDatum);
            //storedValue = NAVD88 + offset
            //-> NAVD88 = storedValue - offset
            expectedFirstElev = firstElev - offset.getValue();
        }

        storeRatingFromXml(xml, user);

        //Use getOne endpoint to get the rating we just stored
        String requestedVerticalDatum = testData._requestedVerticalDatum == null ? "" : testData._requestedVerticalDatum.toString();
        ExtractableResponse<Response> response = given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .contentType(Formats.XMLV2)
                .queryParam(OFFICE, SPK)
                .queryParam(DATUM, requestedVerticalDatum)
            .when()
                .redirects()
                .follow(true)
                .redirects()
                .max(3)
                .get("/ratings/" + ratingId)
            .then()
                .assertThat()
                .log().ifValidationFails(LogDetail.ALL, true)
                .statusCode(is(HttpServletResponse.SC_OK))
                .contentType(is(Formats.XMLV2))
                .extract();

        deleteRatingEffectiveDates(user, ratingId);

        RatingSet receivedRatingSet = RatingXmlFactory.ratingSet(response.body().asString());
        VerticalDatumContainer receivedDatumContainer = receivedRatingSet.getVerticalDatumContainer();
        assertEquals(locId._nativeDatum == null, receivedDatumContainer == null, "Received VerticalDatumContainer presence mismatch.  Expected " + (locId._nativeDatum == null ? "null" : "not null"));

        double receivedFirstElev = receivedRatingSet.getRatings()[0].getValues(0)[0].getIndValue();

        assertEquals(expectedFirstElev, receivedFirstElev, "Unexpected elev value received");
    }

    private static void storeRatingFromXml(String xml, TestAccounts.KeyUser user) {
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.XMLV2)
            .body(xml)
            .header("Authorization", user.toHeaderValue())
            .queryParam(OFFICE, SPK)
        .when()
            .redirects()
            .follow(true)
            .redirects()
            .max(3)
            .post("/ratings")
        .then()
        .assertThat()
            .log().ifValidationFails(LogDetail.ALL, true)
            .statusCode(is(HttpServletResponse.SC_CREATED));
    }

    private static void deleteRatingEffectiveDates(TestAccounts.KeyUser user, String ratingId) {
        given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .contentType(Formats.XMLV2)
                .header("Authorization", user.toHeaderValue())
                .queryParam(OFFICE, SPK)
                .queryParam(BEGIN, "2000-01-01T00:00:00Z")
                .queryParam(END, "2100-01-01T00:00:00Z")
            .when()
                .redirects()
                .follow(true)
                .redirects()
                .max(3)
                .delete("/ratings/" + ratingId)
            .then()
                .assertThat()
                .log().ifValidationFails(LogDetail.ALL, true)
                .statusCode(is(HttpServletResponse.SC_NO_CONTENT));
    }

    private static Stream<Arguments> provideDatumCombinations() {
        //This provides information for 3 locations:
        // - LOC_WITH_NAVD88: native datum NAVD88
        // - LOC_WITH_NGVD29: native datum NGVD29
        //And for each location, we test requesting:
        // - null
        // - NATIVE
        // - NAVD88
        // - NGVD29
        //
        //This creates a 2 x 4 matrix of test cases to cover all combinations of these parameters
        return Stream.of(TestLocationIds.values())
                .filter(locId -> !BASE_LOCATION.equals(locId._locationId))
                .flatMap(locId -> Stream.of(TestLocationVerticalDatumData.values())
                        .map(datum -> Arguments.of(locId, datum)));
    }


    static @NotNull String readVerticalDatumRatingXml(String location) throws IOException {
        return readResourceFile("cwms/cda/api/vertical_datum_example_rating.xml").replace("{office-id}", SPK)
                                                                                 .replace("{location}", location);
    }

    // Remove the vertical-datum-info element from the rating XML so that the controller must
    // rely on the datum query parameter or the location's native datum.
    private static String stripVerticalDatumInfo(String xml) {
        return xml.replaceAll("(?s)<vertical-datum-info.*?</vertical-datum-info>", "");
    }

    private enum TestLocationIds {
        BASE(BASE_LOCATION, null),
        NAVD88(LOC_WITH_NAVD88, VerticalDatum.NAVD88),
        NGVD29(LOC_WITH_NGVD29, VerticalDatum.NGVD29),
        ;

        final String _locationId;
        final VerticalDatum _nativeDatum;

        TestLocationIds(String locationId, VerticalDatum nativeDatum) {
            _locationId = locationId;
            _nativeDatum = nativeDatum;
        }
    }

    private enum TestLocationVerticalDatumData {
        NULL(null),
        NATIVE(VerticalDatum.NATIVE),
        NAVD88(VerticalDatum.NAVD88),
        NGVD29(VerticalDatum.NGVD29),
        ;

        final VerticalDatum _requestedVerticalDatum;

        TestLocationVerticalDatumData(VerticalDatum requestedVerticalDatum) {
            _requestedVerticalDatum = requestedVerticalDatum;
        }
    }
}
