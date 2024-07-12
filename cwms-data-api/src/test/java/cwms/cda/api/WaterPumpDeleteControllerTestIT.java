/*
 *
 * MIT License
 *
 * Copyright (c) 2024 Hydrologic Engineering Center
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
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
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE
 * SOFTWARE.
 */

package cwms.cda.api;

import cwms.cda.api.enums.Nation;
import cwms.cda.api.watersupply.WaterContractController;
import cwms.cda.data.dao.JooqDao.DeleteMethod;
import cwms.cda.data.dto.Location;
import cwms.cda.data.dto.watersupply.WaterUserContract;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import fixtures.TestAccounts;
import io.restassured.filter.log.LogDetail;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.servlet.http.HttpServletResponse;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;

import static cwms.cda.api.Controllers.*;
import static cwms.cda.security.KeyAccessManager.AUTH_HEADER;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

@Tag("integration")
class WaterPumpDeleteControllerTestIT extends DataApiTestIT {
    private static final String USAGE_ID = "usage-id";
    private static final String OFFICE_ID = "SPK";
    private static final WaterUserContract CONTRACT;
    private static final WaterUserContract CONTRACT_NO_PUMP;
    private static final String CONTRACT_NAME = "contract-name";
    static {
        try (
                InputStream contractStream = WaterContractController.class
                        .getResourceAsStream("/cwms/cda/api/waterusercontract.json");
                InputStream contractStreamNoPump = WaterContractController.class
                        .getResourceAsStream("/cwms/cda/api/waterusercontract_no_pump.json")
        ) {
            assert contractStream != null;
            assert contractStreamNoPump != null;
            String contractJson = IOUtils.toString(contractStream, StandardCharsets.UTF_8);
            String contractJsonNoPump = IOUtils.toString(contractStreamNoPump, StandardCharsets.UTF_8);
            CONTRACT = Formats.parseContent(new ContentType(Formats.JSONV1), contractJson, WaterUserContract.class);
            CONTRACT_NO_PUMP = Formats.parseContent(new ContentType(Formats.JSONV1), contractJsonNoPump,
                    WaterUserContract.class);
        } catch(Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @BeforeAll
    static void setUp() {
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

        Location contractLocation = new Location.Builder(CONTRACT.getContractId().getOfficeId(),
                CONTRACT.getContractId().getName()).withLocationKind("PROJECT").withTimeZoneName(ZoneId.of("UTC"))
                .withHorizontalDatum("WGS84").withLongitude(78.0).withLatitude(67.9).withDescription("Contract Location")
                .withBoundingOfficeId("SPK").withNearestCity("Sacramento").withStateInitial("CA")
                .withCountyName("Sacramento").withTimeZoneName(ZoneId.of("UTC"))
                .withLocationType("WATER PROJECT").withPublicName("TEST WATER PROJECT").withPublishedLatitude(89.7)
                .withPublishedLongitude(56.9).withElevation(459.1).withElevationUnits("m").withNation(Nation.US)
                .withMapLabel("PLACE").withVerticalDatum("WGS84").withActive(true).withLongName("FULL TEST").build();
        Location parentLocation = new Location.Builder(CONTRACT.getWaterUser().getParentLocationRef().getOfficeId(),
                CONTRACT.getWaterUser().getParentLocationRef().getName()).withLocationKind("PROJECT")
                .withTimeZoneName(ZoneId.of("UTC")).withHorizontalDatum("WGS84")
                .withLongitude(38.0).withLatitude(56.5).build();


        String json = Formats.format(Formats.parseHeader(Formats.JSONV1, Location.class), contractLocation);

        // create contract location
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.JSONV1)
            .accept(Formats.JSONV1)
            .body(json)
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/locations")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
        ;

        json = Formats.format(Formats.parseHeader(Formats.JSON, Location.class), parentLocation);

        // create parent site location
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.JSON)
            .body(json)
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/locations")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
        ;

        json = Formats.format(Formats.parseHeader(Formats.JSONV1, WaterUserContract.class), CONTRACT);

        // create contract
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.JSONV1)
            .body(json)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(FAIL_IF_EXISTS, "true")
        .when()
                .redirects().follow(true)
                .redirects().max(3)
                .post("/projects/PROJECT/water-users/SPK/contracts")
            .then()
                .log().ifValidationFails(LogDetail.ALL, true)
            .assertThat()
                .statusCode(is(HttpServletResponse.SC_CREATED))
                .body("office-id", equalTo(CONTRACT.getOfficeId()))
                .body("water-user.entity-name", equalTo(CONTRACT.getWaterUser().getEntityName()))
                .body("water-user.parent-location-ref.office-id", equalTo(CONTRACT.getWaterUser()
                        .getParentLocationRef().getOfficeId()))
                .body("water-user.parent-location-ref.name", equalTo(CONTRACT.getWaterUser()
                        .getParentLocationRef().getName()))
                .body("water-user.water-right", equalTo(CONTRACT.getWaterUser().getWaterRight()))
                .body("water-contract.office-id", equalTo(CONTRACT.getWaterContract().getOfficeId()))
                .body("water-contract.display-value", equalTo(CONTRACT.getWaterContract().getDisplayValue()))
                .body("water-contract.tooltip", equalTo(CONTRACT.getWaterContract().getTooltip()))
                .body("water-contract.active", equalTo(CONTRACT.getWaterContract().getActive()))
                .body("contract-effective-date", equalTo(CONTRACT.getContractEffectiveDate()))
                .body("contract-expiration-date", equalTo(CONTRACT.getContractExpirationDate()))
                .body("contracted-storage", equalTo(CONTRACT.getContractedStorage()))
                .body("initial-use-allocation", equalTo(CONTRACT.getInitialUseAllocation()))
                .body("future-use-allocation", equalTo(CONTRACT.getFutureUseAllocation()))
                .body("storage-units-id", equalTo(CONTRACT.getStorageUnitsId()))
                .body("future-use-percent-activated", equalTo(CONTRACT.getFutureUsePercentActivated()))
                .body("total-alloc-percent-activated", equalTo(CONTRACT.getTotalAllocPercentActivated()))
                .body("pump-out-location.pump-location.office-id", equalTo(CONTRACT.getPumpOutLocation()
                        .getPumpLocation().getOfficeId()))
                .body("pump-out-location.pump-location.name", equalTo(CONTRACT.getPumpOutLocation()
                        .getPumpLocation().getName()))
                .body("pump-out-location.pump-location.latitude", equalTo(CONTRACT.getPumpOutLocation()
                        .getPumpLocation().getLatitude()))
                .body("pump-out-location.pump-location.longitude", equalTo(CONTRACT.getPumpOutLocation()
                        .getPumpLocation().getLongitude()))
                .body("pump-out-location.pump-location.active", equalTo(CONTRACT.getPumpOutLocation()
                        .getPumpLocation().getActive()))
                .body("pump-out-location.pump-location.public-name", equalTo(CONTRACT.getPumpOutLocation()
                        .getPumpLocation().getPublicName()))
                .body("pump-out-location.pump-location.long-name", equalTo(CONTRACT.getPumpOutLocation()
                        .getPumpLocation().getLongName()))
                .body("pump-out-location.pump-location.description", equalTo(CONTRACT.getPumpOutLocation()
                        .getPumpLocation().getDescription()))
                .body("pump-out-location.pump-location.timezone-name", equalTo(CONTRACT.getPumpOutLocation()
                        .getPumpLocation().getTimezoneName()))
                .body("pump-out-location.pump-location.location-type", equalTo(CONTRACT.getPumpOutLocation()
                        .getPumpLocation().getLocationType()))
                .body("pump-out-location.pump-location.location-kind", equalTo(CONTRACT.getPumpOutLocation()
                        .getPumpLocation().getLocationKind()))
                .body("pump-out-location.pump-location.nation", equalTo(CONTRACT.getPumpOutLocation()
                        .getPumpLocation().getNation()))
                .body("pump-out-location.pump-location.state-initial", equalTo(CONTRACT.getPumpOutLocation()
                        .getPumpLocation().getStateInitial()))
                .body("pump-out-location.pump-location.county-name", equalTo(CONTRACT.getPumpOutLocation()
                        .getPumpLocation().getCountyName()))
                .body("pump-out-location.pump-location.nearest-city", equalTo(CONTRACT.getPumpOutLocation()
                        .getPumpLocation().getNearestCity()))
                .body("pump-out-location.pump-location.horizontal-datum", equalTo(CONTRACT.getPumpOutLocation()
                        .getPumpLocation().getHorizontalDatum()))
                .body("pump-out-location.pump-location.published-latitude", equalTo(CONTRACT.getPumpOutLocation()
                        .getPumpLocation().getPublishedLatitude()))
                .body("pump-out-location.pump-location.published-longitude", equalTo(CONTRACT.getPumpOutLocation()
                        .getPumpLocation().getPublishedLongitude()))
                .body("pump-out-location.pump-location.vertical-datum", equalTo(CONTRACT.getPumpOutLocation()
                        .getPumpLocation().getVerticalDatum()))
                .body("pump-out-location.pump-location.elevation", equalTo(CONTRACT.getPumpOutLocation()
                        .getPumpLocation().getElevation()))
                .body("pump-out-location.pump-location.map-label", equalTo(CONTRACT.getPumpOutLocation()
                        .getPumpLocation().getMapLabel()))
                .body("pump-out-location.pump-location.bounding-office-id", equalTo(CONTRACT.getPumpOutLocation()
                        .getPumpLocation().getBoundingOfficeId()))
                .body("pump-out-location.pump-location.elevation-units", equalTo(CONTRACT.getPumpOutLocation()
                        .getPumpLocation().getElevationUnits()))
                .body("pump-out-below-location.pump-location.office-id", equalTo(CONTRACT.getPumpOutBelowLocation()
                        .getPumpLocation().getOfficeId()))
                .body("pump-out-below-location.pump-location.name", equalTo(CONTRACT.getPumpOutBelowLocation()
                        .getPumpLocation().getName()))
                .body("pump-out-below-location.pump-location.latitude", equalTo(CONTRACT.getPumpOutBelowLocation()
                        .getPumpLocation().getLatitude()))
                .body("pump-out-below-location.pump-location.longitude", equalTo(CONTRACT.getPumpOutBelowLocation()
                        .getPumpLocation().getLongitude()))
                .body("pump-out-below-location.pump-location.active", equalTo(CONTRACT.getPumpOutBelowLocation()
                        .getPumpLocation().getActive()))
                .body("pump-out-below-location.pump-location.public-name", equalTo(CONTRACT.getPumpOutBelowLocation()
                        .getPumpLocation().getPublicName()))
                .body("pump-out-below-location.pump-location.long-name", equalTo(CONTRACT.getPumpOutBelowLocation()
                        .getPumpLocation().getLongName()))
                .body("pump-out-below-location.pump-location.description", equalTo(CONTRACT.getPumpOutBelowLocation()
                        .getPumpLocation().getDescription()))
                .body("pump-out-below-location.pump-location.timezone-name", equalTo(CONTRACT.getPumpOutBelowLocation()
                        .getPumpLocation().getTimezoneName()))
                .body("pump-out-below-location.pump-location.location-type", equalTo(CONTRACT.getPumpOutBelowLocation()
                        .getPumpLocation().getLocationType()))
                .body("pump-out-below-location.pump-location.location-kind", equalTo(CONTRACT.getPumpOutBelowLocation()
                        .getPumpLocation().getLocationKind()))
                .body("pump-out-below-location.pump-location.nation", equalTo(CONTRACT.getPumpOutBelowLocation()
                        .getPumpLocation().getNation()))
                .body("pump-out-below-location.pump-location.state-initial", equalTo(CONTRACT.getPumpOutBelowLocation()
                        .getPumpLocation().getStateInitial()))
                .body("pump-out-below-location.pump-location.county-name", equalTo(CONTRACT.getPumpOutBelowLocation()
                        .getPumpLocation().getCountyName()))
                .body("pump-out-below-location.pump-location.nearest-city", equalTo(CONTRACT.getPumpOutBelowLocation()
                        .getPumpLocation().getNearestCity()))
                .body("pump-out-below-location.pump-location.horizontal-datum", equalTo(CONTRACT
                        .getPumpOutBelowLocation().getPumpLocation().getHorizontalDatum()))
                .body("pump-out-below-location.pump-location.published-latitude", equalTo(CONTRACT
                        .getPumpOutBelowLocation().getPumpLocation().getPublishedLatitude()))
                .body("pump-out-below-location.pump-location.published-longitude", equalTo(CONTRACT
                        .getPumpOutBelowLocation().getPumpLocation().getPublishedLongitude()))
                .body("pump-out-below-location.pump-location.vertical-datum", equalTo(CONTRACT
                        .getPumpOutBelowLocation().getPumpLocation().getVerticalDatum()))
                .body("pump-out-below-location.pump-location.elevation", equalTo(CONTRACT.getPumpOutBelowLocation()
                        .getPumpLocation().getElevation()))
                .body("pump-out-below-location.pump-location.map-label", equalTo(CONTRACT.getPumpOutBelowLocation()
                        .getPumpLocation().getMapLabel()))
                .body("pump-out-below-location.pump-location.bounding-office-id", equalTo(CONTRACT
                        .getPumpOutBelowLocation().getPumpLocation().getBoundingOfficeId()))
                .body("pump-out-below-location.pump-location.elevation-units", equalTo(CONTRACT
                        .getPumpOutBelowLocation().getPumpLocation().getElevationUnits()))
                .body("pump-in-location.pump-location.office-id", equalTo(CONTRACT.getPumpInLocation()
                        .getPumpLocation().getOfficeId()))
                .body("pump-in-location.pump-location.name", equalTo(CONTRACT.getPumpInLocation().getPumpLocation()
                        .getName()))
                .body("pump-in-location.pump-location.latitude", equalTo(CONTRACT.getPumpInLocation().getPumpLocation()
                        .getLatitude()))
                .body("pump-in-location.pump-location.longitude", equalTo(CONTRACT.getPumpInLocation().getPumpLocation()
                        .getLongitude()))
                .body("pump-in-location.pump-location.active", equalTo(CONTRACT.getPumpInLocation().getPumpLocation()
                        .getActive()))
                .body("pump-in-location.pump-location.public-name", equalTo(CONTRACT.getPumpInLocation()
                        .getPumpLocation().getPublicName()))
                .body("pump-in-location.pump-location.long-name", equalTo(CONTRACT.getPumpInLocation()
                        .getPumpLocation().getLongName()))
                .body("pump-in-location.pump-location.description", equalTo(CONTRACT.getPumpInLocation()
                        .getPumpLocation().getDescription()))
                .body("pump-in-location.pump-location.timezone-name", equalTo(CONTRACT.getPumpInLocation()
                        .getPumpLocation().getTimezoneName()))
                .body("pump-in-location.pump-location.location-type", equalTo(CONTRACT.getPumpInLocation()
                        .getPumpLocation().getLocationType()))
                .body("pump-in-location.pump-location.location-kind", equalTo(CONTRACT.getPumpInLocation()
                        .getPumpLocation().getLocationKind()))
                .body("pump-in-location.pump-location.nation", equalTo(CONTRACT.getPumpInLocation()
                        .getPumpLocation().getNation()))
                .body("pump-in-location.pump-location.state-initial", equalTo(CONTRACT.getPumpInLocation()
                        .getPumpLocation().getStateInitial()))
                .body("pump-in-location.pump-location.county-name", equalTo(CONTRACT.getPumpInLocation()
                        .getPumpLocation().getCountyName()))
                .body("pump-in-location.pump-location.nearest-city", equalTo(CONTRACT.getPumpInLocation()
                        .getPumpLocation().getNearestCity()))
                .body("pump-in-location.pump-location.horizontal-datum", equalTo(CONTRACT.getPumpInLocation()
                        .getPumpLocation().getHorizontalDatum()))
                .body("pump-in-location.pump-location.published-latitude", equalTo(CONTRACT.getPumpInLocation()
                        .getPumpLocation().getPublishedLatitude()))
                .body("pump-in-location.pump-location.published-longitude", equalTo(CONTRACT.getPumpInLocation()
                        .getPumpLocation().getPublishedLongitude()))
                .body("pump-in-location.pump-location.vertical-datum", equalTo(CONTRACT.getPumpInLocation()
                        .getPumpLocation().getVerticalDatum()))
                .body("pump-in-location.pump-location.elevation", equalTo(CONTRACT.getPumpInLocation()
                        .getPumpLocation().getElevation()))
                .body("pump-in-location.pump-location.map-label", equalTo(CONTRACT.getPumpInLocation()
                        .getPumpLocation().getMapLabel()))
                .body("pump-in-location.pump-location.bounding-office-id", equalTo(CONTRACT.getPumpInLocation()
                        .getPumpLocation().getBoundingOfficeId()))
                .body("pump-in-location.pump-location.elevation-units", equalTo(CONTRACT.getPumpInLocation()
                        .getPumpLocation().getElevationUnits()))
            ;

        String json_no_pump = Formats.format(Formats.parseHeader(Formats.JSONV1, WaterUserContract.class),
                CONTRACT_NO_PUMP);

        // Create contract
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.JSONV1)
            .body(json_no_pump)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(FAIL_IF_EXISTS, "false")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/projects/SPK/PROJECT/water-users/WATERUSER/contracts")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("office-id", equalTo(CONTRACT_NO_PUMP.getOfficeId()))
            .body("water-user.entity-name", equalTo(CONTRACT_NO_PUMP.getWaterUser().getEntityName()))
            .body("water-user.parent-location-ref.office-id", equalTo(CONTRACT_NO_PUMP.getWaterUser()
                    .getParentLocationRef().getOfficeId()))
            .body("water-user.parent-location-ref.name", equalTo(CONTRACT_NO_PUMP.getWaterUser()
                    .getParentLocationRef().getName()))
            .body("water-user.water-right", equalTo(CONTRACT_NO_PUMP.getWaterUser().getWaterRight()))
            .body("water-contract.office-id", equalTo(CONTRACT_NO_PUMP.getWaterContract().getOfficeId()))
            .body("water-contract.display-value", equalTo(CONTRACT_NO_PUMP.getWaterContract().getDisplayValue()))
            .body("water-contract.tooltip", equalTo(CONTRACT_NO_PUMP.getWaterContract().getTooltip()))
            .body("water-contract.active", equalTo(CONTRACT_NO_PUMP.getWaterContract().getActive()))
            .body("contract-effective-date", equalTo(CONTRACT_NO_PUMP.getContractEffectiveDate()))
            .body("contract-expiration-date", equalTo(CONTRACT_NO_PUMP.getContractExpirationDate()))
            .body("contracted-storage", equalTo(CONTRACT_NO_PUMP.getContractedStorage()))
            .body("initial-use-allocation", equalTo(CONTRACT_NO_PUMP.getInitialUseAllocation()))
            .body("future-use-allocation", equalTo(CONTRACT_NO_PUMP.getFutureUseAllocation()))
            .body("storage-units-id", equalTo(CONTRACT_NO_PUMP.getStorageUnitsId()))
            .body("future-use-percent-activated", equalTo(CONTRACT_NO_PUMP.getFutureUsePercentActivated()))
            .body("total-alloc-percent-activated", equalTo(CONTRACT_NO_PUMP.getTotalAllocPercentActivated()))
            .body("pump-out-location.pump-location.office-id", equalTo(CONTRACT_NO_PUMP.getPumpOutLocation()
                    .getPumpLocation().getOfficeId()))
            .body("pump-out-location.pump-location.name", equalTo(CONTRACT_NO_PUMP.getPumpOutLocation()
                    .getPumpLocation().getName()))
            .body("pump-out-location.pump-location.latitude", equalTo(CONTRACT_NO_PUMP.getPumpOutLocation()
                    .getPumpLocation().getLatitude()))
            .body("pump-out-location.pump-location.longitude", equalTo(CONTRACT_NO_PUMP.getPumpOutLocation()
                    .getPumpLocation().getLongitude()))
            .body("pump-out-location.pump-location.active", equalTo(CONTRACT_NO_PUMP.getPumpOutLocation()
                    .getPumpLocation().getActive()))
            .body("pump-out-location.pump-location.public-name", equalTo(CONTRACT_NO_PUMP.getPumpOutLocation()
                    .getPumpLocation().getPublicName()))
            .body("pump-out-location.pump-location.long-name", equalTo(CONTRACT_NO_PUMP.getPumpOutLocation()
                    .getPumpLocation().getLongName()))
            .body("pump-out-location.pump-location.description", equalTo(CONTRACT_NO_PUMP.getPumpOutLocation()
                    .getPumpLocation().getDescription()))
            .body("pump-out-location.pump-location.timezone-name", equalTo(CONTRACT_NO_PUMP.getPumpOutLocation()
                    .getPumpLocation().getTimezoneName()))
            .body("pump-out-location.pump-location.location-type", equalTo(CONTRACT_NO_PUMP.getPumpOutLocation()
                    .getPumpLocation().getLocationType()))
            .body("pump-out-location.pump-location.location-kind", equalTo(CONTRACT_NO_PUMP.getPumpOutLocation()
                    .getPumpLocation().getLocationKind()))
            .body("pump-out-location.pump-location.nation", equalTo(CONTRACT_NO_PUMP.getPumpOutLocation()
                    .getPumpLocation().getNation()))
            .body("pump-out-location.pump-location.state-initial", equalTo(CONTRACT_NO_PUMP.getPumpOutLocation()
                    .getPumpLocation().getStateInitial()))
            .body("pump-out-location.pump-location.county-name", equalTo(CONTRACT_NO_PUMP.getPumpOutLocation()
                    .getPumpLocation().getCountyName()))
            .body("pump-out-location.pump-location.nearest-city", equalTo(CONTRACT_NO_PUMP.getPumpOutLocation()
                    .getPumpLocation().getNearestCity()))
            .body("pump-out-location.pump-location.horizontal-datum", equalTo(CONTRACT_NO_PUMP.getPumpOutLocation()
                    .getPumpLocation().getHorizontalDatum()))
            .body("pump-out-location.pump-location.published-latitude", equalTo(CONTRACT_NO_PUMP
                    .getPumpOutLocation().getPumpLocation().getPublishedLatitude()))
            .body("pump-out-location.pump-location.published-longitude", equalTo(CONTRACT_NO_PUMP
                    .getPumpOutLocation().getPumpLocation().getPublishedLongitude()))
            .body("pump-out-location.pump-location.vertical-datum", equalTo(CONTRACT_NO_PUMP
                    .getPumpOutLocation().getPumpLocation().getVerticalDatum()))
            .body("pump-out-location.pump-location.elevation", equalTo(CONTRACT_NO_PUMP.getPumpOutLocation()
                    .getPumpLocation().getElevation()))
            .body("pump-out-location.pump-location.map-label", equalTo(CONTRACT_NO_PUMP.getPumpOutLocation()
                    .getPumpLocation().getMapLabel()))
            .body("pump-out-location.pump-location.bounding-office-id", equalTo(CONTRACT_NO_PUMP
                    .getPumpOutLocation().getPumpLocation().getBoundingOfficeId()))
            .body("pump-out-location.pump-location.elevation-units", equalTo(CONTRACT_NO_PUMP.getPumpOutLocation()
                    .getPumpLocation().getElevationUnits()))
            .body("pump-out-below-location.pump-location.office-id", equalTo(CONTRACT_NO_PUMP
                    .getPumpOutBelowLocation().getPumpLocation().getOfficeId()))
            .body("pump-out-below-location.pump-location.name", equalTo(CONTRACT_NO_PUMP.getPumpOutBelowLocation()
                    .getPumpLocation().getName()))
            .body("pump-out-below-location.pump-location.latitude", equalTo(CONTRACT_NO_PUMP
                    .getPumpOutBelowLocation().getPumpLocation().getLatitude()))
            .body("pump-out-below-location.pump-location.longitude", equalTo(CONTRACT_NO_PUMP
                    .getPumpOutBelowLocation().getPumpLocation().getLongitude()))
            .body("pump-out-below-location.pump-location.active", equalTo(CONTRACT_NO_PUMP
                    .getPumpOutBelowLocation().getPumpLocation().getActive()))
            .body("pump-out-below-location.pump-location.public-name", equalTo(CONTRACT_NO_PUMP
                    .getPumpOutBelowLocation().getPumpLocation().getPublicName()))
            .body("pump-out-below-location.pump-location.long-name", equalTo(CONTRACT_NO_PUMP
                    .getPumpOutBelowLocation().getPumpLocation().getLongName()))
            .body("pump-out-below-location.pump-location.description", equalTo(CONTRACT_NO_PUMP
                    .getPumpOutBelowLocation().getPumpLocation().getDescription()))
            .body("pump-out-below-location.pump-location.timezone-name", equalTo(CONTRACT_NO_PUMP
                    .getPumpOutBelowLocation().getPumpLocation().getTimezoneName()))
            .body("pump-out-below-location.pump-location.location-type", equalTo(CONTRACT_NO_PUMP
                    .getPumpOutBelowLocation().getPumpLocation().getLocationType()))
            .body("pump-out-below-location.pump-location.location-kind", equalTo(CONTRACT_NO_PUMP
                    .getPumpOutBelowLocation().getPumpLocation().getLocationKind()))
            .body("pump-out-below-location.pump-location.nation", equalTo(CONTRACT_NO_PUMP
                    .getPumpOutBelowLocation().getPumpLocation().getNation()))
            .body("pump-out-below-location.pump-location.state-initial", equalTo(CONTRACT_NO_PUMP
                    .getPumpOutBelowLocation().getPumpLocation().getStateInitial()))
            .body("pump-out-below-location.pump-location.county-name", equalTo(CONTRACT_NO_PUMP
                    .getPumpOutBelowLocation().getPumpLocation().getCountyName()))
            .body("pump-out-below-location.pump-location.nearest-city", equalTo(CONTRACT_NO_PUMP
                    .getPumpOutBelowLocation().getPumpLocation().getNearestCity()))
            .body("pump-out-below-location.pump-location.horizontal-datum", equalTo(CONTRACT_NO_PUMP
                    .getPumpOutBelowLocation().getPumpLocation().getHorizontalDatum()))
            .body("pump-out-below-location.pump-location.published-latitude", equalTo(CONTRACT_NO_PUMP
                    .getPumpOutBelowLocation().getPumpLocation().getPublishedLatitude()))
            .body("pump-out-below-location.pump-location.published-longitude", equalTo(CONTRACT_NO_PUMP
                    .getPumpOutBelowLocation().getPumpLocation().getPublishedLongitude()))
            .body("pump-out-below-location.pump-location.vertical-datum", equalTo(CONTRACT_NO_PUMP
                    .getPumpOutBelowLocation().getPumpLocation().getVerticalDatum()))
            .body("pump-out-below-location.pump-location.elevation", equalTo(CONTRACT_NO_PUMP
                    .getPumpOutBelowLocation().getPumpLocation().getElevation()))
            .body("pump-out-below-location.pump-location.map-label", equalTo(CONTRACT_NO_PUMP
                    .getPumpOutBelowLocation().getPumpLocation().getMapLabel()))
            .body("pump-out-below-location.pump-location.bounding-office-id", equalTo(CONTRACT_NO_PUMP
                    .getPumpOutBelowLocation().getPumpLocation().getBoundingOfficeId()))
            .body("pump-out-below-location.pump-location.elevation-units", equalTo(CONTRACT_NO_PUMP
                    .getPumpOutBelowLocation().getPumpLocation().getElevationUnits()))
            .body("pump-in-location", equalTo(null))
        ;


    }

    @AfterAll
    static void tearDown() throws Exception {
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

        Location contractLocation = new Location.Builder(CONTRACT.getContractId().getOfficeId(),
                CONTRACT.getContractId().getName()).withLocationKind("PROJECT").withTimeZoneName(ZoneId.of("UTC"))
                .withHorizontalDatum("WGS84").withLongitude(78.0).withLatitude(67.9).build();
        Location parentLocation = new Location.Builder(CONTRACT.getWaterUser().getParentLocationRef().getOfficeId(),
                CONTRACT.getWaterUser().getParentLocationRef().getName()).withLocationKind("PROJECT")
                .withTimeZoneName(ZoneId.of("UTC")).withHorizontalDatum("WGS84")
                .withLongitude(38.0).withLatitude(56.5).build();

        String json = Formats.format(Formats.parseHeader(Formats.JSONV1, Location.class), contractLocation);

        // delete contract location
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.JSONV1)
            .body(json)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(OFFICE, OFFICE_ID)
            .queryParam(LOCATION_ID, contractLocation.getName())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("locations")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
        ;

        json = Formats.format(Formats.parseHeader(Formats.JSONV1, Location.class), parentLocation);

        // delete parent site location
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.JSONV1)
            .body(json)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(OFFICE, OFFICE_ID)
            .queryParam(LOCATION_ID, parentLocation.getName())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("locations")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
        ;

        json = Formats.format(Formats.parseHeader(Formats.JSONV1, WaterUserContract.class), CONTRACT);

        // delete contract
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.JSONV1)
            .body(json)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(LOCATION_ID, CONTRACT.getWaterUser().getParentLocationRef().getName())
            .queryParam(DELETE_MODE, DeleteMethod.DELETE_ALL.toString())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("/projects/" + CONTRACT.getContractId().getOfficeId() + "/"
                    + CONTRACT.getWaterUser().getParentLocationRef().getName()
                    + "/water-users/" + CONTRACT.getWaterUser().getEntityName() + "/contracts"
                    + CONTRACT.getContractId().getName())
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NO_CONTENT))
        ;

        String json_no_pump = Formats.format(Formats.parseHeader(Formats.JSONV1, WaterUserContract.class),
                CONTRACT_NO_PUMP);

        // delete contract
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.JSONV1)
            .body(json_no_pump)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(LOCATION_ID, CONTRACT_NO_PUMP.getWaterUser().getParentLocationRef().getName())
            .queryParam(DELETE_MODE, DeleteMethod.DELETE_ALL.toString())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("/projects/" + CONTRACT_NO_PUMP.getContractId().getOfficeId() + "/"
                    + CONTRACT_NO_PUMP.getWaterUser().getParentLocationRef().getName()
                    + "/water-users/" + CONTRACT_NO_PUMP.getWaterUser().getEntityName() + "/contracts"
                    + CONTRACT_NO_PUMP.getContractId().getName())
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NO_CONTENT))
        ;

    }

    @Test
    void test_remove_from_contract() {
        // Structure of test:
        // 1) Remove the pump from the contract
        // 2) Retrieve the contract and assert it does not contain the pump

        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;

        // Remove pump and assert it is removed
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .pathParam(Controllers.OFFICE, OFFICE_ID)
            .pathParam(Controllers.PROJECT_ID, CONTRACT.getContractId().getName())
            .pathParam(CONTRACT_NAME, CONTRACT.getWaterContract().getDisplayValue())
            .pathParam(NAME, CONTRACT.getPumpOutLocation().getPumpLocation().getName())
            .queryParam(USAGE_ID, "PUMP1")
            .queryParam(Controllers.DELETE, DeleteMethod.DELETE_ALL.toString())
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("/projects/" + OFFICE_ID + "/" + CONTRACT.getContractId().getName() + "/water-user/"
                    + CONTRACT.getWaterUser().getEntityName() + "/contracts/"
                    + CONTRACT.getContractId().getName()+ "/pumps/" + OFFICE_ID + "/"
                    + CONTRACT.getPumpInLocation().getPumpId().getName())
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NO_CONTENT))
        ;

        // Retrieve contract and assert pump is removed
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .pathParam(Controllers.OFFICE, OFFICE_ID)
            .pathParam(Controllers.PROJECT_ID, CONTRACT.getContractId().getName())
            .pathParam(CONTRACT_NAME, CONTRACT.getContractId().getName())
            .accept(Formats.JSONV1)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/projects/" + OFFICE_ID + "/" + CONTRACT.getContractId().getName() + "/water-user/"
                    + CONTRACT.getWaterUser().getEntityName() + "/contracts/"
                    + CONTRACT.getContractId().getName())
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("office-id", equalTo(CONTRACT.getOfficeId()))
            .body("water-user.entity-name", equalTo(CONTRACT.getWaterUser().getEntityName()))
            .body("water-user.parent-location-ref.office-id", equalTo(CONTRACT.getWaterUser()
                    .getParentLocationRef().getOfficeId()))
            .body("water-user.parent-location-ref.name", equalTo(CONTRACT.getWaterUser().getParentLocationRef()
                    .getName()))
            .body("water-user.water-right", equalTo(CONTRACT.getWaterUser().getWaterRight()))
            .body("water-contract.office-id", equalTo(CONTRACT.getWaterContract().getOfficeId()))
            .body("water-contract.display-value", equalTo(CONTRACT.getWaterContract().getDisplayValue()))
            .body("water-contract.tooltip", equalTo(CONTRACT.getWaterContract().getTooltip()))
            .body("water-contract.active", equalTo(CONTRACT.getWaterContract().getActive()))
            .body("contract-effective-date", equalTo(CONTRACT.getContractEffectiveDate()))
            .body("contract-expiration-date", equalTo(CONTRACT.getContractExpirationDate()))
            .body("contracted-storage", equalTo(CONTRACT.getContractedStorage()))
            .body("initial-use-allocation", equalTo(CONTRACT.getInitialUseAllocation()))
            .body("future-use-allocation", equalTo(CONTRACT.getFutureUseAllocation()))
            .body("storage-units-id", equalTo(CONTRACT.getStorageUnitsId()))
            .body("future-use-percent-activated", equalTo(CONTRACT.getFutureUsePercentActivated()))
            .body("total-alloc-percent-activated", equalTo(CONTRACT.getTotalAllocPercentActivated()))
            .body("pump-out-location.pump-location.office-id", equalTo(CONTRACT.getPumpOutLocation()
                    .getPumpLocation().getOfficeId()))
            .body("pump-out-location.pump-location.name", equalTo(CONTRACT.getPumpOutLocation().getPumpLocation()
                    .getName()))
            .body("pump-out-location.pump-location.latitude", equalTo(CONTRACT.getPumpOutLocation()
                    .getPumpLocation().getLatitude()))
            .body("pump-out-location.pump-location.longitude", equalTo(CONTRACT.getPumpOutLocation()
                    .getPumpLocation().getLongitude()))
            .body("pump-out-location.pump-location.active", equalTo(CONTRACT.getPumpOutLocation()
                    .getPumpLocation().getActive()))
            .body("pump-out-location.pump-location.public-name", equalTo(CONTRACT.getPumpOutLocation()
                    .getPumpLocation().getPublicName()))
            .body("pump-out-location.pump-location.long-name", equalTo(CONTRACT.getPumpOutLocation()
                    .getPumpLocation().getLongName()))
            .body("pump-out-location.pump-location.description", equalTo(CONTRACT.getPumpOutLocation()
                    .getPumpLocation().getDescription()))
            .body("pump-out-location.pump-location.timezone-name", equalTo(CONTRACT.getPumpOutLocation()
                    .getPumpLocation().getTimezoneName()))
            .body("pump-out-location.pump-location.location-type", equalTo(CONTRACT.getPumpOutLocation()
                    .getPumpLocation().getLocationType()))
            .body("pump-out-location.pump-location.location-kind", equalTo(CONTRACT.getPumpOutLocation()
                    .getPumpLocation().getLocationKind()))
            .body("pump-out-location.pump-location.nation", equalTo(CONTRACT.getPumpOutLocation()
                    .getPumpLocation().getNation()))
            .body("pump-out-location.pump-location.state-initial", equalTo(CONTRACT.getPumpOutLocation()
                    .getPumpLocation().getStateInitial()))
            .body("pump-out-location.pump-location.county-name", equalTo(CONTRACT.getPumpOutLocation()
                    .getPumpLocation().getCountyName()))
            .body("pump-out-location.pump-location.nearest-city", equalTo(CONTRACT.getPumpOutLocation()
                    .getPumpLocation().getNearestCity()))
            .body("pump-out-location.pump-location.horizontal-datum", equalTo(CONTRACT.getPumpOutLocation()
                    .getPumpLocation().getHorizontalDatum()))
            .body("pump-out-location.pump-location.published-latitude", equalTo(CONTRACT.getPumpOutLocation()
                    .getPumpLocation().getPublishedLatitude()))
            .body("pump-out-location.pump-location.published-longitude", equalTo(CONTRACT.getPumpOutLocation()
                    .getPumpLocation().getPublishedLongitude()))
            .body("pump-out-location.pump-location.vertical-datum", equalTo(CONTRACT.getPumpOutLocation()
                    .getPumpLocation().getVerticalDatum()))
            .body("pump-out-location.pump-location.elevation", equalTo(CONTRACT.getPumpOutLocation()
                    .getPumpLocation().getElevation()))
            .body("pump-out-location.pump-location.map-label", equalTo(CONTRACT.getPumpOutLocation()
                    .getPumpLocation().getMapLabel()))
            .body("pump-out-location.pump-location.bounding-office-id", equalTo(CONTRACT.getPumpOutLocation()
                    .getPumpLocation().getBoundingOfficeId()))
            .body("pump-out-location.pump-location.elevation-units", equalTo(CONTRACT.getPumpOutLocation()
                    .getPumpLocation().getElevationUnits()))
            .body("pump-out-below-location.pump-location.office-id", equalTo(CONTRACT.getPumpOutBelowLocation()
                    .getPumpLocation().getOfficeId()))
            .body("pump-out-below-location.pump-location.name", equalTo(CONTRACT.getPumpOutBelowLocation()
                    .getPumpLocation().getName()))
            .body("pump-out-below-location.pump-location.latitude", equalTo(CONTRACT.getPumpOutBelowLocation()
                    .getPumpLocation().getLatitude()))
            .body("pump-out-below-location.pump-location.longitude", equalTo(CONTRACT.getPumpOutBelowLocation()
                    .getPumpLocation().getLongitude()))
            .body("pump-out-below-location.pump-location.active", equalTo(CONTRACT.getPumpOutBelowLocation()
                    .getPumpLocation().getActive()))
            .body("pump-out-below-location.pump-location.public-name", equalTo(CONTRACT.getPumpOutBelowLocation()
                    .getPumpLocation().getPublicName()))
            .body("pump-out-below-location.pump-location.long-name", equalTo(CONTRACT.getPumpOutBelowLocation()
                    .getPumpLocation().getLongName()))
            .body("pump-out-below-location.pump-location.description", equalTo(CONTRACT.getPumpOutBelowLocation()
                    .getPumpLocation().getDescription()))
            .body("pump-out-below-location.pump-location.timezone-name", equalTo(CONTRACT.getPumpOutBelowLocation()
                    .getPumpLocation().getTimezoneName()))
            .body("pump-out-below-location.pump-location.location-type", equalTo(CONTRACT.getPumpOutBelowLocation()
                    .getPumpLocation().getLocationType()))
            .body("pump-out-below-location.pump-location.location-kind", equalTo(CONTRACT.getPumpOutBelowLocation()
                    .getPumpLocation().getLocationKind()))
            .body("pump-out-below-location.pump-location.nation", equalTo(CONTRACT.getPumpOutBelowLocation()
                    .getPumpLocation().getNation()))
            .body("pump-out-below-location.pump-location.state-initial", equalTo(CONTRACT.getPumpOutBelowLocation()
                    .getPumpLocation().getStateInitial()))
            .body("pump-out-below-location.pump-location.county-name", equalTo(CONTRACT.getPumpOutBelowLocation()
                    .getPumpLocation().getCountyName()))
            .body("pump-out-below-location.pump-location.nearest-city", equalTo(CONTRACT.getPumpOutBelowLocation()
                    .getPumpLocation().getNearestCity()))
            .body("pump-out-below-location.pump-location.horizontal-datum", equalTo(CONTRACT
                    .getPumpOutBelowLocation().getPumpLocation().getHorizontalDatum()))
            .body("pump-out-below-location.pump-location.published-latitude", equalTo(CONTRACT
                    .getPumpOutBelowLocation().getPumpLocation().getPublishedLatitude()))
            .body("pump-out-below-location.pump-location.published-longitude", equalTo(CONTRACT
                    .getPumpOutBelowLocation().getPumpLocation().getPublishedLongitude()))
            .body("pump-out-below-location.pump-location.vertical-datum", equalTo(CONTRACT
                    .getPumpOutBelowLocation().getPumpLocation().getVerticalDatum()))
            .body("pump-out-below-location.pump-location.elevation", equalTo(CONTRACT.getPumpOutBelowLocation()
                    .getPumpLocation().getElevation()))
            .body("pump-out-below-location.pump-location.map-label", equalTo(CONTRACT.getPumpOutBelowLocation()
                    .getPumpLocation().getMapLabel()))
            .body("pump-out-below-location.pump-location.bounding-office-id", equalTo(CONTRACT
                    .getPumpOutBelowLocation().getPumpLocation().getBoundingOfficeId()))
            .body("pump-out-below-location.pump-location.elevation-units", equalTo(CONTRACT
                    .getPumpOutBelowLocation().getPumpLocation().getElevationUnits()))
            .body("pump-in-location", equalTo(null))
        ;
    }

    @Test
    void test_remove_does_not_exist() {
        // Structure of test:
        // 1) Remove a pump and assert that an error is thrown
        // 2) Create a contract with an empty pump
        // 3) Try to remove the pump and assert that an error is thrown

        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;


        // Remove pump
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .pathParam(Controllers.OFFICE, OFFICE_ID)
            .pathParam(Controllers.PROJECT_ID, CONTRACT_NO_PUMP.getContractId().getName())
            .pathParam(CONTRACT_NAME, CONTRACT_NO_PUMP.getWaterContract().getDisplayValue())
            .pathParam(NAME, CONTRACT_NO_PUMP.getPumpOutLocation().getPumpLocation().getName())
            .queryParam(USAGE_ID, "PUMP1")
            .queryParam(Controllers.DELETE, DeleteMethod.DELETE_ALL.toString())
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("/projects/" + OFFICE_ID + "/" + CONTRACT_NO_PUMP.getContractId().getName() + "/water-user/"
                    + CONTRACT_NO_PUMP.getWaterUser().getEntityName() + "/contracts/"
                    + CONTRACT_NO_PUMP.getContractId().getName()+ "/pumps/" + OFFICE_ID + "/"
                    + CONTRACT_NO_PUMP.getPumpInLocation().getPumpId().getName())
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_INTERNAL_SERVER_ERROR))
        ;


        // Remove pump
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .pathParam(Controllers.OFFICE, OFFICE_ID)
            .pathParam(Controllers.PROJECT_ID, CONTRACT_NO_PUMP.getContractId().getName())
            .pathParam(CONTRACT_NAME, CONTRACT_NO_PUMP.getWaterContract().getDisplayValue())
            .pathParam(NAME, CONTRACT_NO_PUMP.getPumpOutLocation().getPumpLocation().getName())
            .queryParam(USAGE_ID, "PUMP1")
            .queryParam(Controllers.DELETE, DeleteMethod.DELETE_ALL.toString())
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("/projects/" + OFFICE_ID + "/" + CONTRACT_NO_PUMP.getContractId().getName() + "/water-user/"
                    + CONTRACT_NO_PUMP.getWaterUser().getEntityName() + "/contracts/"
                    + CONTRACT_NO_PUMP.getContractId().getName()+ "/pumps/"
                    + CONTRACT_NO_PUMP.getPumpInLocation().getPumpId().getName())
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_INTERNAL_SERVER_ERROR))
        ;

    }
}
