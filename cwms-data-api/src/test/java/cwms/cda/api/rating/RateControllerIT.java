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

import static cwms.cda.data.dao.DaoTest.getDslContext;
import static cwms.cda.formatters.Formats.JSON;
import static cwms.cda.security.ApiKeyIdentityProvider.AUTH_HEADER;
import static io.restassured.RestAssured.given;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isOneOf;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.core.JsonProcessingException;
import cwms.cda.api.DataApiTestIT;
import cwms.cda.data.dao.RatingSetDao;
import cwms.cda.data.dao.TimeSeriesDaoImpl;
import cwms.cda.data.dto.TimeSeries;
import cwms.cda.data.dto.rating.RateInputTimeSeries;
import cwms.cda.data.dto.rating.RateInputValues;
import cwms.cda.formatters.json.JsonV1;
import fixtures.CwmsDataApiSetupCallback;
import fixtures.TestAccounts;
import hec.data.RatingException;
import hec.data.cwmsRating.AbstractRatingSet;
import hec.data.cwmsRating.RatingSetFactory;
import hec.data.cwmsRating.io.RatingSetContainer;
import hec.data.cwmsRating.io.RatingSpecContainer;
import io.restassured.filter.log.LogDetail;
import java.io.IOException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import javax.servlet.http.HttpServletResponse;
import mil.army.usace.hec.cwms.rating.io.xml.RatingContainerXmlFactory;
import mil.army.usace.hec.cwms.rating.io.xml.RatingSetContainerXmlFactory;
import org.jooq.DSLContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@Tag("integration")
final class RateControllerIT extends DataApiTestIT {

    private static final String EXISTING_LOC = "RatingsControllerTestIT";
    private static final String SPK = "SPK";
    private static final String TSID_STAGE = EXISTING_LOC + ".Stage.Inst.0.0.IntegrationTest";
    private static final String TSID_FLOW = EXISTING_LOC + ".Flow.Ave.1Day.1Day.IntegrationTest";
    private static AbstractRatingSet ratingSet;
    private static final ZonedDateTime TS_START = ZonedDateTime.of(2015, 5, 3, 0, 0, 0, 0, ZoneId.of("UTC"));

    @BeforeAll
    static void beforeAll() throws Exception {
        createLocation(EXISTING_LOC, true, SPK);
        String ratingXml = readResourceFile("cwms/cda/api/Zanesville_Stage_Flow_COE_Production.xml");
        ratingXml = ratingXml.replaceAll("Zanesville", EXISTING_LOC);
        RatingSetContainer container = RatingSetContainerXmlFactory.ratingSetContainerFromXml(ratingXml);
        RatingSpecContainer specContainer = container.ratingSpecContainer;
        specContainer.officeId = SPK;
        specContainer.specOfficeId = SPK;
        specContainer.locationId = EXISTING_LOC;
        String setXml = RatingContainerXmlFactory.toXml(container, "", 0, true, false);

        ratingSet = RatingSetFactory.ratingSet(container);
        CwmsDataApiSetupCallback.getDatabaseLink()
            .connection(c -> {
                try {
                    DSLContext context = getDslContext(c, SPK);
                    RatingSetDao ratingSetDao = new RatingSetDao(context);
                    ratingSetDao.store(setXml, true);
                    TimeSeriesDaoImpl timeSeriesDao = new TimeSeriesDaoImpl(context, new MetricRegistry());
                    TimeSeries timeSeries = new TimeSeries(null, 0, 0, TSID_STAGE, SPK, null, null, "ft", null);
                    timeSeries.addValue(Timestamp.from(TS_START.toInstant()), 2.49935994, 0);
                    timeSeries.addValue(Timestamp.from(TS_START.plusMonths(2).toInstant()), 2.92608012, 0);
                    timeSeries.addValue(Timestamp.from(TS_START.plusMonths(30).toInstant()), 10.668, 0);
                    timeSeriesDao.store(timeSeries, null);
                    timeSeries = new TimeSeries(null, 0, 0, TSID_FLOW, SPK, null, null, "cfs", null);
                    timeSeries.addValue(Timestamp.from(TS_START.toInstant()), 25.7683304, 0);
                    timeSeries.addValue(Timestamp.from(TS_START.plusDays(1).toInstant()), 33.0, 0);
                    timeSeries.addValue(Timestamp.from(TS_START.plusDays(2).toInstant()), 50.4039869, 0);
                    timeSeriesDao.store(timeSeries, null);
                } catch (IOException | RatingException e) {
                    throw new RuntimeException(e);
                }
            });
    }

    @Test
    void testRateRatingDoesNotExist() throws Exception {
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;
        long suffix = Instant.now().toEpochMilli();
        String unknownRating = ratingSet.getName() + suffix;
        String body = serializeRateInputValues();
        given()
            .accept(JSON)
            .contentType(JSON)
            .header(AUTH_HEADER, user.toHeaderValue())
            .body(body)
        .when()
            .post("/ratings/rate-values/" + SPK + "/" + unknownRating)
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NOT_FOUND));

        body = serializeRateInputTimeSeries();
        given()
            .accept(JSON)
            .contentType(JSON)
            .header(AUTH_HEADER, user.toHeaderValue())
            .body(body)
        .when()
            .post("/ratings/rate-ts/" + SPK + "/" + unknownRating)
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NOT_FOUND));

        body = serializeReverseRateInputValues();
        given()
            .accept(JSON)
            .contentType(JSON)
            .header(AUTH_HEADER, user.toHeaderValue())
            .body(body)
        .when()
            .post("/ratings/reverse-rate-values/" + SPK + "/" + unknownRating)
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NOT_FOUND));

        body = serializeReverseRateInputTimeSeries();
        given()
            .accept(JSON)
            .contentType(JSON)
            .header(AUTH_HEADER, user.toHeaderValue())
            .body(body)
        .when()
            .post("/ratings/reverse-rate-ts/" + SPK + "/" + unknownRating)
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NOT_FOUND));
    }


    @Test
    void testRateFunctions() throws JsonProcessingException {
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;
        String body = serializeRateInputValues();
        given()
            .accept(JSON)
            .contentType(JSON)
            .header(AUTH_HEADER, user.toHeaderValue())
            .body(body)
        .when()
            .post("/ratings/rate-values/" + SPK + "/" + ratingSet.getName())
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("rating-id.name", is(ratingSet.getName()))
            .body("rating-id.office-id", is(SPK))
            .body("unit", is("cfs"))
            .body("values", not(empty()));

        body = serializeRateInputTimeSeries();
        given()
            .accept(JSON)
            .contentType(JSON)
            .header(AUTH_HEADER, user.toHeaderValue())
            .body(body)
        .when()
            .post("/ratings/rate-ts/" + SPK + "/" + ratingSet.getName())
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("rating-id.name", is(ratingSet.getName()))
            .body("rating-id.office-id", is(SPK))
            .body("unit", is("cfs"))
            .body("values", not(empty()))
            .body("values[0]", not(empty()));


        body = serializeReverseRateInputValues();
        given()
            .accept(JSON)
            .contentType(JSON)
            .header(AUTH_HEADER, user.toHeaderValue())
            .body(body)
        .when()
            .post("/ratings/reverse-rate-values/" + SPK + "/" + ratingSet.getName())
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("rating-id.name", is(ratingSet.getName()))
            .body("rating-id.office-id", is(SPK))
            .body("unit", is("ft"))
            .body("values", not(empty()));

        body = serializeReverseRateInputTimeSeries();
        given()
            .accept(JSON)
            .contentType(JSON)
            .header(AUTH_HEADER, user.toHeaderValue())
            .body(body)
        .when()
            .post("/ratings/reverse-rate-ts/" + SPK + "/" + ratingSet.getName())
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("rating-id.name", is(ratingSet.getName()))
            .body("rating-id.office-id", is(SPK))
            .body("unit", is("ft"))
            .body("values", not(empty()))
            .body("values[0]", not(empty()));
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "/ratings/rate-values/",
        "/ratings/rate-ts/",
        "/ratings/reverse-rate-values/",
        "/ratings/reverse-rate-ts/"
    })
    void testRateLimitUnauthorized(String endpointPath) throws Exception {
        boolean rateLimit = false;

        // Expecting to hit the rate limit
        String body = getAppropriateInputTimeSeriesData(endpointPath);

        for (int i = 0; i < 150; i++) {
            int code = given()
                .accept(JSON)
                .contentType(JSON)
                .body(body)
            .when()
                .post(endpointPath + SPK + "/" + ratingSet.getName())
            .then()
                .log().ifValidationFails(LogDetail.ALL, true)
            .assertThat()
                .statusCode(isOneOf(HttpServletResponse.SC_OK, 429))
                .extract().statusCode();
            if (code == 429) {
                rateLimit = true;
                break;
            }
        }
        assertTrue(rateLimit);
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "/ratings/rate-values/",
        "/ratings/rate-ts/",
        "/ratings/reverse-rate-values/",
        "/ratings/reverse-rate-ts/"
    })
    void testRateLimitAuthorized(String endpointPath) throws Exception {
        boolean rateLimit = false;

        String body = getAppropriateInputTimeSeriesData(endpointPath);

        // expecting to hit the rate limit but bypass it via authorization
        for (int i = 0; i < 150; i++) {
            int code = given()
                .accept(JSON)
                .contentType(JSON)
                .body(body)
                .header("Authorization", TestAccounts.KeyUser.SPK_NORMAL.toHeaderValue())
            .when()
                .post(endpointPath + SPK + "/" + ratingSet.getName())
            .then()
                .log().ifValidationFails(LogDetail.ALL, true)
            .assertThat()
                .statusCode(isOneOf(HttpServletResponse.SC_OK, 429))
                .extract().statusCode();
            if (code == 429) {
                rateLimit = true;
                break;
            }
        }
        assertFalse(rateLimit);
    }

    private String getAppropriateInputTimeSeriesData(String path) throws JsonProcessingException {
        switch (path) {
            case "/ratings/rate-ts/":
                return serializeRateInputTimeSeries();
            case "/ratings/reverse-rate-ts/":
                return serializeReverseRateInputTimeSeries();
            case "/ratings/rate-values/":
                return serializeRateInputValues();
            case "/ratings/reverse-rate-values/":
                return serializeReverseRateInputValues();
            default:
                throw new IllegalArgumentException("Unknown endpoint path: " + path);
        }
    }

    private static String serializeReverseRateInputTimeSeries() throws JsonProcessingException {
        RateInputTimeSeries timeSeriesInput = new RateInputTimeSeries.RateInputTimeSeriesBuilder()
            .withTimeSeriesIds(singletonList(TSID_FLOW))
            .withOutputUnit("ft")
            .withStartTime(TS_START.toInstant().toEpochMilli())
            .withEndTime(TS_START.plusDays(2).toInstant().toEpochMilli())
            .build();
        return JsonV1.buildObjectMapper().writeValueAsString(timeSeriesInput);
    }

    private static String serializeReverseRateInputValues() throws JsonProcessingException {
        List<List<Double>> values = singletonList(asList(14.1584233, 25.7683304, 30.0, 50.0));
        RateInputValues valuesInput = new RateInputValues.RateInputValuesBuilder()
            .withValues(values)
            .withInputUnits(singletonList("cfs"))
            .withOutputUnit("ft")
            .build();
        return JsonV1.buildObjectMapper().writeValueAsString(valuesInput);
    }

    private static String serializeRateInputTimeSeries() throws JsonProcessingException {
        RateInputTimeSeries timeSeriesInput = new RateInputTimeSeries.RateInputTimeSeriesBuilder()
            .withTimeSeriesIds(singletonList(TSID_STAGE))
            .withOutputUnit("cfs")
            .withStartTime(TS_START.toInstant().toEpochMilli())
            .withEndTime(TS_START.plusYears(10).toInstant().toEpochMilli())
            .build();
        return JsonV1.buildObjectMapper().writeValueAsString(timeSeriesInput);
    }

    private static String serializeRateInputValues() throws JsonProcessingException {
        List<List<Double>> values = singletonList(asList(7.65048012, 8.47343976, 10.0, 17.3736));
        RateInputValues valuesInput = new RateInputValues.RateInputValuesBuilder()
            .withValues(values)
            .withInputUnits(singletonList("ft"))
            .withOutputUnit("cfs")
            .build();
        return JsonV1.buildObjectMapper().writeValueAsString(valuesInput);
    }
}
