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
import cwms.cda.data.dao.DeleteRule;
import cwms.cda.data.dao.JooqDao.DeleteMethod;
import cwms.cda.data.dao.LocationsDaoImpl;
import cwms.cda.data.dao.project.ProjectDao;
import cwms.cda.data.dao.watersupply.WaterContractDao;
import cwms.cda.data.dto.Location;
import cwms.cda.data.dto.project.Project;
import cwms.cda.data.dto.watersupply.PumpType;
import cwms.cda.data.dto.watersupply.WaterUser;
import cwms.cda.data.dto.watersupply.WaterUserContract;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.json.JsonV1;
import fixtures.CwmsDataApiSetupCallback;
import fixtures.TestAccounts;
import io.restassured.filter.log.LogDetail;
import mil.army.usace.hec.test.database.CwmsDatabaseContainer;
import org.apache.commons.io.IOUtils;
import org.jooq.DSLContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;

import static cwms.cda.api.Controllers.DELETE;
import static cwms.cda.api.Controllers.DELETE_MODE;
import static cwms.cda.data.dao.DaoTest.getDslContext;
import static cwms.cda.security.KeyAccessManager.AUTH_HEADER;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.*;


@Tag("integration")
class WaterPumpDeleteControllerTestIT extends DataApiTestIT {
    private static final String USAGE_ID = "usage-id";
    private static final String OFFICE_ID = "SWT";
    private static final WaterUserContract CONTRACT;
    private static final WaterUserContract CONTRACT_NO_PUMP;
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
    static void setUp() throws Exception {
        Location contractLocation = new Location.Builder(CONTRACT.getContractId().getOfficeId(),
                CONTRACT.getContractId().getName()).withLocationKind("PROJECT").withTimeZoneName(ZoneId.of("UTC"))
                .withHorizontalDatum("WGS84").withLongitude(78.0).withLatitude(67.9).withVerticalDatum("WGS84")
                .withLongName("TEST CONTRACT LOCATION").withActive(true).withMapLabel("LABEL").withNation(Nation.US)
                .withElevation(456.7).withElevationUnits("m").withPublishedLongitude(78.9).withPublishedLatitude(45.3)
                .withLocationType("PROJECT").withDescription("TEST PROJECT").build();
        Location parentLocation = new Location.Builder(CONTRACT.getWaterUser().getProjectId().getOfficeId(),
                CONTRACT.getWaterUser().getProjectId().getName()).withLocationKind("PROJECT")
                .withTimeZoneName(ZoneId.of("UTC")).withHorizontalDatum("WGS84")
                .withLongitude(38.0).withLatitude(56.5).withVerticalDatum("WGS84")
                .withLongName("TEST CONTRACT LOCATION").withActive(true).withMapLabel("LABEL").withNation(Nation.US)
                .withElevation(456.7).withElevationUnits("m").withPublishedLongitude(78.9).withPublishedLatitude(45.3)
                .withLocationType("PROJECT").withDescription("TEST PROJECT").build();
        Location parentLocation2 = new Location.Builder(CONTRACT_NO_PUMP.getWaterUser().getProjectId().getOfficeId(),
                CONTRACT_NO_PUMP.getWaterUser().getProjectId().getName()).withLocationKind("PROJECT")
                .withTimeZoneName(ZoneId.of("UTC")).withHorizontalDatum("WGS84")
                .withLongitude(38.0).withLatitude(56.5).withVerticalDatum("WGS84")
                .withLongName("TEST CONTRACT LOCATION").withActive(true).withMapLabel("LABEL").withNation(Nation.US)
                .withElevation(456.7).withElevationUnits("m").withPublishedLongitude(78.9).withPublishedLatitude(45.3)
                .withLocationType("PROJECT").withDescription("TEST PROJECT").build();
        Project project = new Project.Builder().withLocation(parentLocation).withFederalCost(BigDecimal.valueOf(123456789))
                .withAuthorizingLaw("NEW LAW").withCostUnit("$").withProjectOwner(CONTRACT.getWaterUser().getEntityName())
                .build();
        Project project1 = new Project.Builder().withLocation(parentLocation2).withFederalCost(BigDecimal.valueOf(123456789))
                .withAuthorizingLaw("NEW LAW").withCostUnit("$").withProjectOwner(CONTRACT_NO_PUMP.getWaterUser().getEntityName())
                .build();
        WaterUser waterUser = new WaterUser(CONTRACT.getWaterUser().getEntityName(),
                CONTRACT.getWaterUser().getProjectId(),
                CONTRACT.getWaterUser().getWaterRight());
        WaterUser waterUserNoPump = new WaterUser(CONTRACT_NO_PUMP.getWaterUser().getEntityName(),
                CONTRACT_NO_PUMP.getWaterUser().getProjectId(),
                CONTRACT_NO_PUMP.getWaterUser().getWaterRight());

        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext ctx = getDslContext(c, OFFICE_ID);
            LocationsDaoImpl locationsDao = new LocationsDaoImpl(ctx);
            ProjectDao projectDao = new ProjectDao(ctx);
            WaterContractDao waterContractDao = new WaterContractDao(ctx);
            try {
                locationsDao.storeLocation(contractLocation);
                locationsDao.storeLocation(parentLocation);
                locationsDao.storeLocation(parentLocation2);
                projectDao.store(project, true);
                projectDao.store(project1, true);
                waterContractDao.storeWaterUser(waterUser, true);
                waterContractDao.storeWaterUser(waterUserNoPump, true);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    @AfterAll
    static void tearDown() throws Exception {
        Location contractLocation = new Location.Builder(CONTRACT.getContractId().getOfficeId(),
                CONTRACT.getContractId().getName()).withLocationKind("PROJECT").withTimeZoneName(ZoneId.of("UTC"))
                .withHorizontalDatum("WGS84").withLongitude(78.0).withLatitude(67.9).build();
        Location parentLocation = new Location.Builder(CONTRACT.getWaterUser().getProjectId().getOfficeId(),
                CONTRACT.getWaterUser().getProjectId().getName()).withLocationKind("PROJECT")
                .withTimeZoneName(ZoneId.of("UTC")).withHorizontalDatum("WGS84")
                .withLongitude(38.0).withLatitude(56.5).build();
        Location parentLocation2 = new Location.Builder(CONTRACT_NO_PUMP.getWaterUser().getProjectId().getOfficeId(),
                CONTRACT_NO_PUMP.getWaterUser().getProjectId().getName()).withLocationKind("PROJECT")
                .withTimeZoneName(ZoneId.of("UTC")).withHorizontalDatum("WGS84")
                .withLongitude(38.0).withLatitude(56.5).build();
        WaterUser waterUser = new WaterUser(CONTRACT.getWaterUser().getEntityName(),
                CONTRACT.getWaterUser().getProjectId(),
                CONTRACT.getWaterUser().getWaterRight());
        WaterUser waterUserNoPump = new WaterUser(CONTRACT_NO_PUMP.getWaterUser().getEntityName(),
                CONTRACT_NO_PUMP.getWaterUser().getProjectId(),
                CONTRACT_NO_PUMP.getWaterUser().getWaterRight());

        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext ctx = getDslContext(c, OFFICE_ID);
            LocationsDaoImpl locationsDao = new LocationsDaoImpl(ctx);
            WaterContractDao waterContractDao = new WaterContractDao(ctx);
            ProjectDao projectDao = new ProjectDao(ctx);
            waterContractDao.deleteWaterUser(waterUser.getProjectId(), waterUser.getEntityName(),
                    DeleteRule.DELETE_ALL.toString());
            waterContractDao.deleteWaterUser(waterUserNoPump.getProjectId(), waterUserNoPump.getEntityName(),
                    DeleteRule.DELETE_ALL.toString());
            projectDao.delete(CONTRACT.getOfficeId(), CONTRACT.getWaterUser().getProjectId().getName(),
                    DeleteRule.DELETE_ALL);
            projectDao.delete(CONTRACT_NO_PUMP.getOfficeId(), CONTRACT_NO_PUMP.getWaterUser().getProjectId().getName(),
                    DeleteRule.DELETE_ALL);
            locationsDao.deleteLocation(contractLocation.getName(), contractLocation.getOfficeId(), true);
            locationsDao.deleteLocation(parentLocation.getName(), parentLocation.getOfficeId(), true);
            locationsDao.deleteLocation(parentLocation2.getName(), parentLocation2.getOfficeId(), true);
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    @Test
    void test_remove_from_contract() throws Exception {
        final String PUMP_TYPE = "pump-type";
        // Structure of test:
        // 1) Create contract with pump
        // 2) Remove the pump from the contract
        // 3) Retrieve the contract and assert it does not contain the pump
        // 4) Delete the contract

        TestAccounts.KeyUser user = TestAccounts.KeyUser.SWT_NORMAL;
        String json = JsonV1.buildObjectMapper().writeValueAsString(CONTRACT);

        // Create contract with pump
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.JSONV1)
            .accept(Formats.JSONV1)
            .header(AUTH_HEADER, user.toHeaderValue())
            .body(json)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/projects/" + OFFICE_ID + "/" + CONTRACT.getWaterUser().getProjectId().getName()
                    + "/water-user/" + CONTRACT.getWaterUser().getEntityName() + "/contracts")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED))
        ;

        // Remove pump and assert it is removed
        given()
            .queryParam(DELETE, false)
            .queryParam(PUMP_TYPE, PumpType.IN.getName())
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("/projects/" + OFFICE_ID + "/" + CONTRACT.getWaterUser().getProjectId().getName()
                    + "/water-user/" + CONTRACT.getWaterUser().getEntityName() + "/contracts/"
                    + CONTRACT.getContractId().getName() + "/pumps/"
                    + CONTRACT.getPumpInLocation().getPumpLocation().getName())
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NO_CONTENT))
        ;

        // Retrieve contract and assert pump is removed
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .accept(Formats.JSONV1)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/projects/" + OFFICE_ID + "/" + CONTRACT.getWaterUser().getProjectId().getName()
                    + "/water-user/" + CONTRACT.getWaterUser().getEntityName() + "/contracts/"
                    + CONTRACT.getContractId().getName())
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("[0].office-id", equalTo(CONTRACT.getOfficeId()))
            .body("[0].water-user.entity-name", equalTo(CONTRACT.getWaterUser().getEntityName()))
            .body("[0].water-user.project-id.office-id", equalTo(CONTRACT.getWaterUser()
                    .getProjectId().getOfficeId()))
            .body("[0].water-user.project-id.name", equalTo(CONTRACT.getWaterUser().getProjectId()
                    .getName()))
            .body("[0].water-user.water-right", equalTo(CONTRACT.getWaterUser().getWaterRight()))
            .body("[0].contract-type.office-id", equalTo(CONTRACT.getContractType().getOfficeId()))
            .body("[0].contract-type.display-value", equalTo(CONTRACT.getContractType().getDisplayValue()))
            .body("[0].contract-type.tooltip", equalTo(CONTRACT.getContractType().getTooltip()))
            .body("[0].contract-type.active", equalTo(CONTRACT.getContractType().getActive()))
            .body("[0].contract-effective-date", hasToString(String.valueOf(CONTRACT.getContractEffectiveDate().getTime())))
            .body("[0].contract-expiration-date", hasToString(String.valueOf(CONTRACT.getContractExpirationDate().getTime())))
            .body("[0].contracted-storage", hasToString(String.valueOf(CONTRACT.getContractedStorage())))
            .body("[0].initial-use-allocation", hasToString(String.valueOf(CONTRACT.getInitialUseAllocation())))
            .body("[0].future-use-allocation", hasToString(String.valueOf(CONTRACT.getFutureUseAllocation())))
            .body("[0].storage-units-id", hasToString(String.valueOf(CONTRACT.getStorageUnitsId())))
            .body("[0].future-use-percent-activated", hasToString(String.valueOf(CONTRACT.getFutureUsePercentActivated())))
            .body("[0].total-alloc-percent-activated", hasToString(String.valueOf(CONTRACT.getTotalAllocPercentActivated())))
            .body("[0].pump-out-location.pump-location.office-id", equalTo(CONTRACT.getPumpOutLocation()
                    .getPumpLocation().getOfficeId()))
            .body("[0].pump-out-location.pump-location.name", hasToString(String.valueOf(CONTRACT.getPumpOutLocation().getPumpLocation()
                    .getName())))
            .body("[0].pump-out-location.pump-location.latitude", hasToString(String.valueOf(CONTRACT.getPumpOutLocation()
                    .getPumpLocation().getLatitude())))
            .body("[0].pump-out-location.pump-location.longitude", hasToString(String.valueOf(CONTRACT.getPumpOutLocation()
                    .getPumpLocation().getLongitude())))
            .body("[0].pump-out-location.pump-location.active", equalTo(CONTRACT.getPumpOutLocation()
                    .getPumpLocation().getActive()))
            .body("[0].pump-out-location.pump-location.public-name", equalTo(CONTRACT.getPumpOutLocation()
                    .getPumpLocation().getPublicName()))
            .body("[0].pump-out-location.pump-location.long-name", equalTo(CONTRACT.getPumpOutLocation()
                    .getPumpLocation().getLongName()))
            .body("[0].pump-out-location.pump-location.description", equalTo(CONTRACT.getPumpOutLocation()
                    .getPumpLocation().getDescription()))
            .body("[0].pump-out-location.pump-location.timezone-name", equalTo(CONTRACT.getPumpOutLocation()
                    .getPumpLocation().getTimezoneName()))
            .body("[0].pump-out-location.pump-location.location-type", hasToString(String.valueOf(CONTRACT.getPumpOutLocation()
                    .getPumpLocation().getLocationType())))
            .body("[0].pump-out-location.pump-location.location-kind", equalTo(CONTRACT.getPumpOutLocation()
                    .getPumpLocation().getLocationKind()))
            .body("[0].pump-out-location.pump-location.nation", equalTo(CONTRACT.getPumpOutLocation()
                    .getPumpLocation().getNation().toString()))
            .body("[0].pump-out-location.pump-location.state-initial", equalTo(CONTRACT.getPumpOutLocation()
                    .getPumpLocation().getStateInitial()))
            .body("[0].pump-out-location.pump-location.county-name", equalTo(CONTRACT.getPumpOutLocation()
                    .getPumpLocation().getCountyName()))
            .body("[0].pump-out-location.pump-location.nearest-city", equalTo(CONTRACT.getPumpOutLocation()
                    .getPumpLocation().getNearestCity()))
            .body("[0].pump-out-location.pump-location.horizontal-datum", equalTo(CONTRACT.getPumpOutLocation()
                    .getPumpLocation().getHorizontalDatum()))
            .body("[0].pump-out-location.pump-location.published-latitude", hasToString(String.valueOf(CONTRACT.getPumpOutLocation()
                    .getPumpLocation().getPublishedLatitude())))
            .body("[0].pump-out-location.pump-location.published-longitude", hasToString(String.valueOf(CONTRACT.getPumpOutLocation()
                    .getPumpLocation().getPublishedLongitude())))
            .body("[0].pump-out-location.pump-location.vertical-datum", equalTo(CONTRACT.getPumpOutLocation()
                    .getPumpLocation().getVerticalDatum()))
            .body("[0].pump-out-location.pump-location.elevation", hasToString(String.valueOf(CONTRACT.getPumpOutLocation()
                    .getPumpLocation().getElevation())))
            .body("[0].pump-out-location.pump-location.map-label", equalTo(CONTRACT.getPumpOutLocation()
                    .getPumpLocation().getMapLabel()))
            .body("[0].pump-out-location.pump-location.bounding-office-id", equalTo(CONTRACT.getPumpOutLocation()
                    .getPumpLocation().getBoundingOfficeId()))
            .body("[0].pump-out-location.pump-location.elevation-units", equalTo(CONTRACT.getPumpOutLocation()
                    .getPumpLocation().getElevationUnits()))
            .body("[0].pump-out-below-location.pump-location.office-id", equalTo(CONTRACT.getPumpOutBelowLocation()
                    .getPumpLocation().getOfficeId()))
            .body("[0].pump-out-below-location.pump-location.name", hasToString(String.valueOf(CONTRACT.getPumpOutBelowLocation()
                    .getPumpLocation().getName())))
            .body("[0].pump-out-below-location.pump-location.latitude", hasToString(String.valueOf(CONTRACT.getPumpOutBelowLocation()
                    .getPumpLocation().getLatitude())))
            .body("[0].pump-out-below-location.pump-location.longitude", hasToString(String.valueOf(CONTRACT.getPumpOutBelowLocation()
                    .getPumpLocation().getLongitude())))
            .body("[0].pump-out-below-location.pump-location.active", equalTo(CONTRACT.getPumpOutBelowLocation()
                    .getPumpLocation().getActive()))
            .body("[0].pump-out-below-location.pump-location.public-name", equalTo(CONTRACT.getPumpOutBelowLocation()
                    .getPumpLocation().getPublicName()))
            .body("[0].pump-out-below-location.pump-location.long-name", equalTo(CONTRACT.getPumpOutBelowLocation()
                    .getPumpLocation().getLongName()))
            .body("[0].pump-out-below-location.pump-location.description", equalTo(CONTRACT.getPumpOutBelowLocation()
                    .getPumpLocation().getDescription()))
            .body("[0].pump-out-below-location.pump-location.timezone-name", equalTo(CONTRACT.getPumpOutBelowLocation()
                    .getPumpLocation().getTimezoneName()))
            .body("[0].pump-out-below-location.pump-location.location-type", hasToString(String.valueOf(CONTRACT.getPumpOutBelowLocation()
                    .getPumpLocation().getLocationType())))
            .body("[0].pump-out-below-location.pump-location.location-kind", equalTo(CONTRACT.getPumpOutBelowLocation()
                    .getPumpLocation().getLocationKind()))
            .body("[0].pump-out-below-location.pump-location.nation", equalTo(CONTRACT.getPumpOutBelowLocation()
                    .getPumpLocation().getNation().toString()))
            .body("[0].pump-out-below-location.pump-location.state-initial", equalTo(CONTRACT.getPumpOutBelowLocation()
                    .getPumpLocation().getStateInitial()))
            .body("[0].pump-out-below-location.pump-location.county-name", equalTo(CONTRACT.getPumpOutBelowLocation()
                    .getPumpLocation().getCountyName()))
            .body("[0].pump-out-below-location.pump-location.nearest-city", equalTo(CONTRACT.getPumpOutBelowLocation()
                    .getPumpLocation().getNearestCity()))
            .body("[0].pump-out-below-location.pump-location.horizontal-datum", equalTo(CONTRACT
                    .getPumpOutBelowLocation().getPumpLocation().getHorizontalDatum()))
            .body("[0].pump-out-below-location.pump-location.published-latitude", hasToString(String.valueOf(CONTRACT
                    .getPumpOutBelowLocation().getPumpLocation().getPublishedLatitude())))
            .body("[0].pump-out-below-location.pump-location.published-longitude", hasToString(String.valueOf(CONTRACT
                    .getPumpOutBelowLocation().getPumpLocation().getPublishedLongitude())))
            .body("[0].pump-out-below-location.pump-location.vertical-datum", equalTo(CONTRACT
                    .getPumpOutBelowLocation().getPumpLocation().getVerticalDatum()))
            .body("[0].pump-out-below-location.pump-location.elevation", hasToString(String.valueOf(CONTRACT.getPumpOutBelowLocation()
                    .getPumpLocation().getElevation())))
            .body("[0].pump-out-below-location.pump-location.map-label", equalTo(CONTRACT.getPumpOutBelowLocation()
                    .getPumpLocation().getMapLabel()))
            .body("[0].pump-out-below-location.pump-location.bounding-office-id", equalTo(CONTRACT
                    .getPumpOutBelowLocation().getPumpLocation().getBoundingOfficeId()))
            .body("[0].pump-out-below-location.pump-location.elevation-units", equalTo(CONTRACT
                    .getPumpOutBelowLocation().getPumpLocation().getElevationUnits()))
            .body("[0].pump-in-location", equalTo(null))
        ;

        // Delete contract
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.JSONV1)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(DELETE_MODE, "DELETE ALL")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("/projects/" + OFFICE_ID + "/" + CONTRACT.getWaterUser().getProjectId().getName()
                    + "/water-user/" + CONTRACT.getWaterUser().getEntityName() + "/contracts/" + CONTRACT.getContractId().getName())
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NO_CONTENT))
        ;
    }

    @Test
    void test_remove_does_not_exist() throws Exception {
        // Structure of test:
        // 1) Create contract with no pump
        // 2) Try to remove the pump from contract and assert that an error is thrown
        // 3) Delete the contract

        TestAccounts.KeyUser user = TestAccounts.KeyUser.SWT_NORMAL;
        String json = JsonV1.buildObjectMapper().writeValueAsString(CONTRACT_NO_PUMP);

        // Create contract with no pump
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.JSONV1)
            .accept(Formats.JSONV1)
            .body(json)
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/projects/" + OFFICE_ID + "/" + CONTRACT_NO_PUMP.getWaterUser().getProjectId().getName()
                    + "/water-user/" + CONTRACT_NO_PUMP.getWaterUser().getEntityName() + "/contracts")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED))
        ;

        // Remove pump
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .queryParam(USAGE_ID, "PUMP1")
            .queryParam(DELETE, DeleteMethod.DELETE_ALL.toString())
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("/projects/" + OFFICE_ID + "/" + CONTRACT_NO_PUMP.getContractId().getName() + "/water-user/"
                    + CONTRACT_NO_PUMP.getWaterUser().getEntityName() + "/contracts/"
                    + CONTRACT_NO_PUMP.getContractId().getName()+ "/pumps/"
                    + null)
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_INTERNAL_SERVER_ERROR))
        ;

        // Delete contract
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.JSONV1)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(DELETE_MODE, "DELETE ALL")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("/projects/" + OFFICE_ID + "/" + CONTRACT_NO_PUMP.getWaterUser().getProjectId().getName()
                    + "/water-user/" + CONTRACT_NO_PUMP.getWaterUser().getEntityName()
                    + "/contracts/" + CONTRACT_NO_PUMP.getContractId().getName())
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NO_CONTENT))
        ;
    }
}
