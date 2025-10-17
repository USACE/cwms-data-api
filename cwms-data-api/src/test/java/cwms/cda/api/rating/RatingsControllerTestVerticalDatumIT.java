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

import cwms.cda.api.DataApiTestIT;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dao.VerticalDatum;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import static cwms.cda.api.Controllers.*;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

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
    void test_store_vertical_datum_null_vd_null_create(TestLocationIds locId,
                                                       TestLocationVerticalDatumData testData) throws Exception {
        String xml = readVerticalDatumRatingXml(locId._locationId);
        RatingSet originalRatingSet = RatingXmlFactory.ratingSet(xml);

        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;
        String ratingId = originalRatingSet.getRatingSpec().getRatingSpecId();
        AbstractRating originalRating = originalRatingSet.getRatings()[0];
        originalRating.setVerticalDatumContainer(null);
        VerticalDatum storedVerticalDatum = null;

        storeRatingFromXml(xml, user, storedVerticalDatum);

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
        AbstractRating receivedRating = receivedRatingSet.getRatings()[0];

        VerticalDatumContainer receivedVerticalDatumContainer = receivedRating.getVerticalDatumContainer();
        assertNotNull(receivedVerticalDatumContainer);

        VerticalDatum expectedDatum = testData._requestedVerticalDatum;

        if (testData._requestedVerticalDatum == VerticalDatum.NATIVE) {
            expectedDatum = locId._nativeDatum;
        }

        VerticalDatum receivedDatum = VerticalDatum.getVerticalDatum(
                receivedVerticalDatumContainer.getCurrentVerticalDatum());

        assertEquals(expectedDatum, receivedDatum);
    }

    private static void storeRatingFromXml(String xml, TestAccounts.KeyUser user, VerticalDatum storedVerticalDatum) {
        given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .contentType(Formats.XMLV2)
                .body(xml)
                .header("Authorization", user.toHeaderValue())
                .queryParam(OFFICE, SPK)
                .queryParam(DATUM, storedVerticalDatum)
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
        return Stream.of(TestLocationIds.values())
                     .flatMap(locId -> Stream.of(TestLocationVerticalDatumData.values())
                                             .map(datum -> Arguments.of(locId, datum)));
    }


    private static @NotNull String readVerticalDatumRatingXml(String location) throws IOException {
        return readResourceFile("cwms/cda/api/vertical_datum_example_rating.xml").replace("{office-id}", SPK)
                                                                                 .replace("{location}", location);
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
