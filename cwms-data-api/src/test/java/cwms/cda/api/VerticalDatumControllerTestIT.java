/*
 * MIT License
 *
 * Copyright (c) 2026 Hydrologic Engineering Center
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to do so, subject to the
 * following conditions:
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

import static cwms.cda.api.Controllers.OFFICE;
import static cwms.cda.security.ApiKeyIdentityProvider.AUTH_HEADER;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import cwms.cda.data.dto.VerticalDatumInfo;
import cwms.cda.data.dto.VerticalDatumInfoList;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import fixtures.TestAccounts;
import fixtures.TestAccounts.KeyUser;
import io.restassured.filter.log.LogDetail;
import java.util.stream.Stream;
import javax.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@Tag("integration")
final class VerticalDatumControllerTestIT extends DataApiTestIT {

    private static final String OFFICE_ID = TestAccounts.KeyUser.SPK_NORMAL.getOperatingOffice();

    private static final String TEST_LOCATION = "VDI_LOC_TEST";
    private static final String TEST_LOCATION2 = "VDI_LOC_TEST2";

    @BeforeAll
    static void setup() throws Exception {
        createLocation(TEST_LOCATION, true, OFFICE_ID);
        createLocation(TEST_LOCATION2, true, OFFICE_ID);
    }


    @AfterAll
    static void cleanup() {
        try {
            KeyUser user = KeyUser.SPK_NORMAL;
            given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(Formats.XML)
                .queryParam(OFFICE, OFFICE_ID)
                .queryParam(Controllers.CASCADE_DELETE, true)
                .header(AUTH_HEADER, user.toHeaderValue())
            .when()
                .delete("/locations/" + TEST_LOCATION)
            .then()
                .log().ifValidationFails(LogDetail.ALL, true);
        } catch (Exception ignore) {
        }

        try {
            // DELETE
            given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(Formats.XML)
                .queryParam(OFFICE, OFFICE_ID)
                .header(AUTH_HEADER, KeyUser.SPK_NORMAL.toHeaderValue())
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .delete("/location/" + TEST_LOCATION + "/vertical-datum")
            .then()
                .log().ifValidationFails(LogDetail.ALL, true);
        } catch (Exception ignore) {
        }
    }

    @BeforeEach
    void beforeEach() {
        try {
            // DELETE
            given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .accept(Formats.XML)
                .queryParam(OFFICE, OFFICE_ID)
                .header(AUTH_HEADER, KeyUser.SPK_NORMAL.toHeaderValue())
            .when()
                .redirects().follow(true)
                .redirects().max(3)
                .delete("/location/" + TEST_LOCATION + "/vertical-datum")
            .then()
                .log().ifValidationFails(LogDetail.ALL, true);
        } catch (Exception ignore) {
        }
    }

    @MethodSource("provideFormats")
    @ParameterizedTest
    void test_vertical_datum_crud(ContentType contentType) {
        // Build a VerticalDatumInfo payload
        VerticalDatumInfo.Offset[] offsets = new VerticalDatumInfo.Offset[] {
            new VerticalDatumInfo.Offset(true, "NAVD-88", -0.5)
        };
        VerticalDatumInfo vdi = new VerticalDatumInfo.Builder()
            .withOffice(OFFICE_ID)
            .withLocation(TEST_LOCATION)
            .withUnit("m")
            .withNativeDatum("NGVD-29")
            .withElevation(100.0)
            .withOffsets(offsets)
            .build();

        String vdiPayload = Formats.format(contentType, vdi);

        KeyUser user = KeyUser.SPK_NORMAL;

        // CREATE
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(contentType.toString())
            .contentType(contentType.toString())
            .body(vdiPayload)
            .queryParam(OFFICE, OFFICE_ID)
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/location/" + TEST_LOCATION + "/vertical-datum")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED));

        // GET
        String getBody =
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(contentType.toString())
            .queryParam(OFFICE, OFFICE_ID)
            .queryParam(Controllers.UNIT, "m")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/location/" + TEST_LOCATION + "/vertical-datum")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .extract()
            .asString();

        VerticalDatumInfo got = Formats.parseContent(contentType, getBody, VerticalDatumInfo.class);
        assertEquals(100.0, got.getElevation(), 0.001);
        assertEquals("NGVD-29", got.getNativeDatum());

        // UPDATE
        VerticalDatumInfo vdiUpdated = new VerticalDatumInfo.Builder()
            .from(vdi)
            .withElevation(101.25)
            .build();
        String vdiUpdatedPayload = Formats.format(contentType, vdiUpdated);

        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(contentType.toString())
            .contentType(contentType.toString())
            .body(vdiUpdatedPayload)
            .queryParam(OFFICE, OFFICE_ID)
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .patch("/location/" + TEST_LOCATION + "/vertical-datum")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK));

        //VERIFY UPDATE
        String verifyUpdateBody =
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(contentType.toString())
            .queryParam(OFFICE, OFFICE_ID)
            .queryParam(Controllers.UNIT, "m")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/location/" + TEST_LOCATION + "/vertical-datum")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .extract()
            .asString();

        VerticalDatumInfo gotUpdated = Formats.parseContent(contentType, verifyUpdateBody, VerticalDatumInfo.class);
        assertEquals(101.25, gotUpdated.getElevation(), 0.001);
        assertEquals("NGVD-29", gotUpdated.getNativeDatum());

        // DELETE
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(contentType.toString())
            .queryParam(OFFICE, OFFICE_ID)
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("/location/" + TEST_LOCATION + "/vertical-datum")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK));

        //VERIFY DELETE
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(contentType.toString())
            .queryParam(OFFICE, OFFICE_ID)
            .queryParam(Controllers.UNIT, "m")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/location/" + TEST_LOCATION + "/vertical-datum")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NOT_FOUND));
    }

    @MethodSource("provideFormats")
    @ParameterizedTest
    void test_vertical_datum_getAll(ContentType contentType) {
        // Build a VerticalDatumInfo payload
        VerticalDatumInfo.Offset[] offsets = new VerticalDatumInfo.Offset[] {
            new VerticalDatumInfo.Offset(true, "NAVD-88", -0.5)
        };
        VerticalDatumInfo vdi = new VerticalDatumInfo.Builder()
            .withOffice(OFFICE_ID)
            .withLocation(TEST_LOCATION)
            .withUnit("m")
            .withNativeDatum("NGVD-29")
            .withElevation(100.0)
            .withOffsets(offsets)
            .build();

        String vdiPayload = Formats.format(contentType, vdi);

        KeyUser user = KeyUser.SPK_NORMAL;

        // CREATE
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(contentType.toString())
            .contentType(contentType.toString())
            .body(vdiPayload)
            .queryParam(OFFICE, OFFICE_ID)
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/location/" + TEST_LOCATION + "/vertical-datum")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED));

        VerticalDatumInfo vdi2 = new VerticalDatumInfo.Builder()
            .withOffice(OFFICE_ID)
            .withLocation(TEST_LOCATION2)
            .withUnit("m")
            .withNativeDatum("NGVD-29")
            .withElevation(200.0)
            .withOffsets(offsets)
            .build();

        vdiPayload = Formats.format(contentType, vdi2);

        // CREATE
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(contentType.toString())
            .contentType(contentType.toString())
            .body(vdiPayload)
            .queryParam(OFFICE, OFFICE_ID)
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/location/" + TEST_LOCATION2 + "/vertical-datum")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED));

        // GET
        String vdiList = given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(contentType.toString())
            .queryParam(OFFICE, OFFICE_ID)
            .queryParam(Controllers.UNIT, "m")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/location/vertical-datum")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .extract().asString();

        VerticalDatumInfoList list = Formats.parseContent(contentType, vdiList, VerticalDatumInfoList.class);

        assertEquals(2, list.getDatumList().size());
        for (VerticalDatumInfo vdiInfo : list.getDatumList()) {
            assertTrue(vdiInfo.equals(vdi) || vdiInfo.equals(vdi2));
        }

        // DELETE
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(contentType.toString())
            .queryParam(OFFICE, OFFICE_ID)
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("/location/" + TEST_LOCATION + "/vertical-datum")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK));

        //VERIFY DELETE
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(contentType.toString())
            .queryParam(OFFICE, OFFICE_ID)
            .queryParam(Controllers.UNIT, "m")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/location/" + TEST_LOCATION + "/vertical-datum")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NOT_FOUND));

        // DELETE
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(contentType.toString())
            .queryParam(OFFICE, OFFICE_ID)
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("/location/" + TEST_LOCATION2 + "/vertical-datum")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK));

        //VERIFY DELETE
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(contentType.toString())
            .queryParam(OFFICE, OFFICE_ID)
            .queryParam(Controllers.UNIT, "m")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/location/" + TEST_LOCATION2 + "/vertical-datum")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NOT_FOUND));
    }

    @MethodSource("provideFormats")
    @ParameterizedTest
    void test_create_vertical_datum_already_exists_fails(ContentType contentType) {
        // Build a VerticalDatumInfo payload
        VerticalDatumInfo.Offset[] offsets = new VerticalDatumInfo.Offset[] {
                new VerticalDatumInfo.Offset(true, "NAVD-88", -0.5)
        };
        VerticalDatumInfo vdi = new VerticalDatumInfo.Builder()
                .withOffice(OFFICE_ID)
                .withLocation(TEST_LOCATION)
                .withUnit("m")
                .withNativeDatum("NGVD-29")
                .withElevation(100.0)
                .withOffsets(offsets)
                .build();

        String vdiPayload = Formats.format(contentType, vdi);

        KeyUser user = KeyUser.SPK_NORMAL;

        // First CREATE should succeed
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(contentType.toString())
            .contentType(contentType.toString())
            .body(vdiPayload)
            .queryParam(OFFICE, OFFICE_ID)
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/location/" + TEST_LOCATION + "/vertical-datum")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED));

        // Second CREATE with same payload should fail
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(contentType.toString())
            .contentType(contentType.toString())
            .body(vdiPayload)
            .queryParam(OFFICE, OFFICE_ID)
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/location/" + TEST_LOCATION + "/vertical-datum")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CONFLICT));
    }

    @MethodSource("provideFormats")
    @ParameterizedTest
    void test_create_vertical_datum_already_exists_overwrite(ContentType contentType) {
        // Build a VerticalDatumInfo payload
        VerticalDatumInfo.Offset[] offsets = new VerticalDatumInfo.Offset[] {
            new VerticalDatumInfo.Offset(true, "NAVD-88", -0.5)
        };
        VerticalDatumInfo vdi = new VerticalDatumInfo.Builder()
            .withOffice(OFFICE_ID)
            .withLocation(TEST_LOCATION)
            .withUnit("m")
            .withNativeDatum("NGVD-29")
            .withElevation(100.0)
            .withOffsets(offsets)
            .build();

        String vdiPayload = Formats.format(contentType, vdi);

        KeyUser user = KeyUser.SPK_NORMAL;

        // First CREATE should succeed
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(contentType.toString())
            .contentType(contentType.toString())
            .body(vdiPayload)
            .queryParam(OFFICE, OFFICE_ID)
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/location/" + TEST_LOCATION + "/vertical-datum")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED));

        // Second CREATE with same payload should succeed
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(contentType.toString())
            .contentType(contentType.toString())
            .body(vdiPayload)
            .queryParam(OFFICE, OFFICE_ID)
            .queryParam(Controllers.OVERWRITE, true)
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/location/" + TEST_LOCATION + "/vertical-datum")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED));
    }

    @MethodSource("provideFormats")
    @ParameterizedTest
    void test_update_vertical_datum_not_found_returns_404(ContentType contentType) {
        // Build a VerticalDatumInfo payload
        VerticalDatumInfo.Offset[] offsets = new VerticalDatumInfo.Offset[] {
                new VerticalDatumInfo.Offset(true, "NAVD-88", -0.5)
        };
        VerticalDatumInfo vdi = new VerticalDatumInfo.Builder()
                .withOffice(OFFICE_ID)
                .withLocation(TEST_LOCATION)
                .withUnit("m")
                .withNativeDatum("NGVD-29")
                .withElevation(100.0)
                .withOffsets(offsets)
                .build();

        String vdiPayload = Formats.format(contentType, vdi);

        KeyUser user = KeyUser.SPK_NORMAL;
        // Attempt UPDATE should return 404 Not Found since no VDI exists yet
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(contentType.toString())
            .contentType(contentType.toString())
            .body(vdiPayload)
            .queryParam(OFFICE, OFFICE_ID)
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .patch("/location/" + TEST_LOCATION + "/vertical-datum")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NOT_FOUND));
    }

    static Stream<Arguments> provideFormats() {
        return Stream.of(
                Arguments.of(Formats.parseHeader(Formats.XMLV1, VerticalDatumInfo.class)),
                Arguments.of(Formats.parseHeader(Formats.JSONV1, VerticalDatumInfo.class))
        );
    }
}
