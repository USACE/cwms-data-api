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
import cwms.cda.api.watersupply.WaterContractCreateController;
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

import static cwms.cda.api.Controllers.*;
import static cwms.cda.data.dao.DaoTest.getDslContext;
import static cwms.cda.security.KeyAccessManager.AUTH_HEADER;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.*;


@Tag("integration")
class WaterPumpDisassociateControllerTestIT extends DataApiTestIT {
    private static final String OFFICE_ID = "SPK";
    private static final String PUMP_TYPE = "pump-type";
    private static final String DELETE_ACCOUNTING = "delete-accounting";
    private static final WaterUserContract CONTRACT;
    private static final WaterUserContract CONTRACT_NO_PUMP;
    static {
        try (
                InputStream contractStream = WaterContractCreateController.class
                        .getResourceAsStream("/cwms/cda/api/waterusercontract_diss.json");
                InputStream contractStreamNoPump = WaterContractCreateController.class
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
        Location parentLocation = new Location.Builder(CONTRACT.getWaterUser().getProjectId())
            .withLocationKind("PROJECT")
            .withTimeZoneName(ZoneId.of("UTC"))
            .withHorizontalDatum("WGS84")
            .withLongitude(38.0).withLatitude(56.5)
            .withVerticalDatum("WGS84")
            .withLongName("TEST CONTRACT LOCATION")
            .withActive(true).withMapLabel("LABEL")
            .withNation(Nation.US)
            .withElevation(456.7)
            .withElevationUnits("m")
            .withPublishedLongitude(78.9)
            .withPublishedLatitude(45.3)
            .withLocationType("PROJECT")
            .withDescription("TEST PROJECT")
            .build();
        Location parentLocation2 = new Location.Builder(CONTRACT_NO_PUMP.getWaterUser().getProjectId())
            .withLocationKind("PROJECT")
            .withTimeZoneName(ZoneId.of("UTC"))
            .withHorizontalDatum("WGS84")
            .withLongitude(38.0)
            .withLatitude(56.5)
            .withVerticalDatum("WGS84")
            .withLongName("TEST CONTRACT LOCATION")
            .withActive(true)
            .withMapLabel("LABEL")
            .withNation(Nation.US)
            .withElevation(456.7)
            .withElevationUnits("m")
            .withPublishedLongitude(78.9)
            .withPublishedLatitude(45.3)
            .withLocationType("PROJECT")
            .withDescription("TEST PROJECT")
            .build();
        Project project1 = new Project.Builder()
            .withLocation(parentLocation)
            .withFederalCost(BigDecimal.valueOf(123456789))
            .withAuthorizingLaw("NEW LAW")
            .withCostUnit("$")
            .withProjectOwner(CONTRACT.getWaterUser().getEntityName())
            .build();
        Project project2 = new Project.Builder()
            .withLocation(parentLocation2)
            .withFederalCost(BigDecimal.valueOf(123456789))
            .withAuthorizingLaw("NEW LAW")
            .withCostUnit("$")
            .withProjectOwner(CONTRACT_NO_PUMP.getWaterUser().getEntityName())
            .build();

        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext ctx = getDslContext(c, OFFICE_ID);
            LocationsDaoImpl locationsDao = new LocationsDaoImpl(ctx);
            ProjectDao projectDao = new ProjectDao(ctx);
            WaterContractDao waterContractDao = new WaterContractDao(ctx);
            try {
                locationsDao.storeLocation(parentLocation);
                locationsDao.storeLocation(parentLocation2);
                projectDao.store(project1, true);
                projectDao.store(project2, true);
                waterContractDao.storeWaterUser(CONTRACT.getWaterUser(), false);
                waterContractDao.storeWaterContractTypes(CONTRACT.getContractType(), false);
                waterContractDao.storeWaterUser(CONTRACT_NO_PUMP.getWaterUser(), false);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    @AfterAll
    static void tearDown() throws Exception {
        WaterUser waterUser = new WaterUser.Builder().withEntityName(CONTRACT.getWaterUser().getEntityName())
                .withProjectId(CONTRACT.getWaterUser().getProjectId())
                .withWaterRight(CONTRACT.getWaterUser().getWaterRight()).build();
        WaterUser waterUserNoPump = new WaterUser.Builder().withEntityName(CONTRACT_NO_PUMP.getWaterUser().getEntityName())
                .withProjectId(CONTRACT_NO_PUMP.getWaterUser().getProjectId())
                .withWaterRight(CONTRACT_NO_PUMP.getWaterUser().getWaterRight()).build();

        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext ctx = getDslContext(c, OFFICE_ID);
            LocationsDaoImpl locationsDao = new LocationsDaoImpl(ctx);
            WaterContractDao waterContractDao = new WaterContractDao(ctx);
            ProjectDao projectDao = new ProjectDao(ctx);
            waterContractDao.deleteWaterUser(waterUser.getProjectId(), waterUser.getEntityName(),
                    DeleteMethod.DELETE_ALL);
            waterContractDao.deleteWaterUser(waterUserNoPump.getProjectId(), waterUserNoPump.getEntityName(),
                    DeleteMethod.DELETE_ALL);
            projectDao.delete(CONTRACT.getOfficeId(), CONTRACT.getWaterUser().getProjectId().getName(), DeleteRule.DELETE_ALL);
            projectDao.delete(CONTRACT_NO_PUMP.getOfficeId(), CONTRACT_NO_PUMP.getWaterUser().getProjectId().getName(),
                    DeleteRule.DELETE_ALL);
            locationsDao.deleteLocation(CONTRACT.getWaterUser().getProjectId().getName(), 
                CONTRACT.getWaterUser().getProjectId().getOfficeId(), true);
            locationsDao.deleteLocation(CONTRACT_NO_PUMP.getWaterUser().getProjectId().getName(), 
                CONTRACT_NO_PUMP.getWaterUser().getProjectId().getOfficeId(), true);
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    @Test
    void test_remove_from_contract() throws Exception {
        // Structure of test:
        // 1) Create contract with pump
        // 2) Remove the pump from the contract
        // 3) Retrieve the contract and assert it does not contain the pump
        // 4) Delete the contract

        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;
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
            .queryParam(DELETE_ACCOUNTING, false)
            .queryParam(PUMP_TYPE, PumpType.IN)
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
            .body("office-id", equalTo(CONTRACT.getOfficeId()))
            .body("water-user.entity-name", equalTo(CONTRACT.getWaterUser().getEntityName()))
            .body("water-user.project-id.office-id", equalTo(CONTRACT.getWaterUser()
                    .getProjectId().getOfficeId()))
            .body("water-user.project-id.name", equalTo(CONTRACT.getWaterUser().getProjectId()
                    .getName()))
            .body("water-user.water-right", equalTo(CONTRACT.getWaterUser().getWaterRight()))
            .body("contract-type.office-id", equalTo(CONTRACT.getContractType().getOfficeId()))
            .body("contract-type.display-value", equalTo(CONTRACT.getContractType().getDisplayValue()))
            .body("contract-type.tooltip", equalTo(CONTRACT.getContractType().getTooltip()))
            .body("contract-type.active", equalTo(CONTRACT.getContractType().getActive()))
            .body("contract-effective-date", hasToString(String.valueOf(CONTRACT.getContractEffectiveDate().toEpochMilli())))
            .body("contract-expiration-date", hasToString(String.valueOf(CONTRACT.getContractExpirationDate().toEpochMilli())))
            .body("contracted-storage", hasToString(String.valueOf(CONTRACT.getContractedStorage())))
            .body("initial-use-allocation", hasToString(String.valueOf(CONTRACT.getInitialUseAllocation())))
            .body("future-use-allocation", hasToString(String.valueOf(CONTRACT.getFutureUseAllocation())))
            .body("storage-units-id", hasToString(String.valueOf(CONTRACT.getStorageUnitsId())))
            .body("future-use-percent-activated", hasToString(String.valueOf(CONTRACT.getFutureUsePercentActivated())))
            .body("total-alloc-percent-activated", hasToString(String.valueOf(CONTRACT.getTotalAllocPercentActivated())))
            .body("pump-out-location.pump-location.office-id", equalTo(CONTRACT.getPumpOutLocation()
                    .getPumpLocation().getOfficeId()))
            .body("pump-out-location.pump-location.name", hasToString(String.valueOf(CONTRACT.getPumpOutLocation().getPumpLocation()
                    .getName())))
            .body("pump-out-location.pump-location.latitude", hasToString(String.valueOf(CONTRACT.getPumpOutLocation()
                    .getPumpLocation().getLatitude())))
            .body("pump-out-location.pump-location.longitude", hasToString(String.valueOf(CONTRACT.getPumpOutLocation()
                    .getPumpLocation().getLongitude())))
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
            .body("pump-out-location.pump-location.location-type", hasToString(String.valueOf(CONTRACT.getPumpOutLocation()
                    .getPumpLocation().getLocationType())))
            .body("pump-out-location.pump-location.location-kind", equalTo(CONTRACT.getPumpOutLocation()
                    .getPumpLocation().getLocationKind()))
            .body("pump-out-location.pump-location.nation", equalTo(CONTRACT.getPumpOutLocation()
                    .getPumpLocation().getNation().toString()))
            .body("pump-out-location.pump-location.state-initial", equalTo(CONTRACT.getPumpOutLocation()
                    .getPumpLocation().getStateInitial()))
            .body("pump-out-location.pump-location.county-name", equalTo(CONTRACT.getPumpOutLocation()
                    .getPumpLocation().getCountyName()))
            .body("pump-out-location.pump-location.nearest-city", equalTo(CONTRACT.getPumpOutLocation()
                    .getPumpLocation().getNearestCity()))
            .body("pump-out-location.pump-location.horizontal-datum", equalTo(CONTRACT.getPumpOutLocation()
                    .getPumpLocation().getHorizontalDatum()))
            .body("pump-out-location.pump-location.published-latitude", hasToString(String.valueOf(CONTRACT.getPumpOutLocation()
                    .getPumpLocation().getPublishedLatitude())))
            .body("pump-out-location.pump-location.published-longitude", hasToString(String.valueOf(CONTRACT.getPumpOutLocation()
                    .getPumpLocation().getPublishedLongitude())))
            .body("pump-out-location.pump-location.vertical-datum", equalTo(CONTRACT.getPumpOutLocation()
                    .getPumpLocation().getVerticalDatum()))
            .body("pump-out-location.pump-location.elevation", hasToString(String.valueOf(CONTRACT.getPumpOutLocation()
                    .getPumpLocation().getElevation())))
            .body("pump-out-location.pump-location.map-label", equalTo(CONTRACT.getPumpOutLocation()
                    .getPumpLocation().getMapLabel()))
            .body("pump-out-location.pump-location.bounding-office-id", equalTo(CONTRACT.getPumpOutLocation()
                    .getPumpLocation().getBoundingOfficeId()))
            .body("pump-out-location.pump-location.elevation-units", equalTo(CONTRACT.getPumpOutLocation()
                    .getPumpLocation().getElevationUnits()))
            .body("pump-out-below-location.pump-location.office-id", equalTo(CONTRACT.getPumpOutBelowLocation()
                    .getPumpLocation().getOfficeId()))
            .body("pump-out-below-location.pump-location.name", hasToString(String.valueOf(CONTRACT.getPumpOutBelowLocation()
                    .getPumpLocation().getName())))
            .body("pump-out-below-location.pump-location.latitude", hasToString(String.valueOf(CONTRACT.getPumpOutBelowLocation()
                    .getPumpLocation().getLatitude())))
            .body("pump-out-below-location.pump-location.longitude", hasToString(String.valueOf(CONTRACT.getPumpOutBelowLocation()
                    .getPumpLocation().getLongitude())))
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
            .body("pump-out-below-location.pump-location.location-type", hasToString(String.valueOf(CONTRACT.getPumpOutBelowLocation()
                    .getPumpLocation().getLocationType())))
            .body("pump-out-below-location.pump-location.location-kind", equalTo(CONTRACT.getPumpOutBelowLocation()
                    .getPumpLocation().getLocationKind()))
            .body("pump-out-below-location.pump-location.nation", equalTo(CONTRACT.getPumpOutBelowLocation()
                    .getPumpLocation().getNation().toString()))
            .body("pump-out-below-location.pump-location.state-initial", equalTo(CONTRACT.getPumpOutBelowLocation()
                    .getPumpLocation().getStateInitial()))
            .body("pump-out-below-location.pump-location.county-name", equalTo(CONTRACT.getPumpOutBelowLocation()
                    .getPumpLocation().getCountyName()))
            .body("pump-out-below-location.pump-location.nearest-city", equalTo(CONTRACT.getPumpOutBelowLocation()
                    .getPumpLocation().getNearestCity()))
            .body("pump-out-below-location.pump-location.horizontal-datum", equalTo(CONTRACT
                    .getPumpOutBelowLocation().getPumpLocation().getHorizontalDatum()))
            .body("pump-out-below-location.pump-location.published-latitude", hasToString(String.valueOf(CONTRACT
                    .getPumpOutBelowLocation().getPumpLocation().getPublishedLatitude())))
            .body("pump-out-below-location.pump-location.published-longitude", hasToString(String.valueOf(CONTRACT
                    .getPumpOutBelowLocation().getPumpLocation().getPublishedLongitude())))
            .body("pump-out-below-location.pump-location.vertical-datum", equalTo(CONTRACT
                    .getPumpOutBelowLocation().getPumpLocation().getVerticalDatum()))
            .body("pump-out-below-location.pump-location.elevation", hasToString(String.valueOf(CONTRACT.getPumpOutBelowLocation()
                    .getPumpLocation().getElevation())))
            .body("pump-out-below-location.pump-location.map-label", equalTo(CONTRACT.getPumpOutBelowLocation()
                    .getPumpLocation().getMapLabel()))
            .body("pump-out-below-location.pump-location.bounding-office-id", equalTo(CONTRACT
                    .getPumpOutBelowLocation().getPumpLocation().getBoundingOfficeId()))
            .body("pump-out-below-location.pump-location.elevation-units", equalTo(CONTRACT
                    .getPumpOutBelowLocation().getPumpLocation().getElevationUnits()))
            .body("pump-in-location", equalTo(null))
        ;

        // Delete contract
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.JSONV1)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(METHOD, DeleteMethod.DELETE_ALL)
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

        TestAccounts.KeyUser user = TestAccounts.KeyUser.SPK_NORMAL;
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
            .queryParam(DELETE_ACCOUNTING, DeleteMethod.DELETE_ALL)
            .queryParam(PUMP_TYPE, PumpType.IN)
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("/projects/" + OFFICE_ID + "/" + CONTRACT_NO_PUMP.getContractId().getName() + "/water-user/"
                    + CONTRACT_NO_PUMP.getWaterUser().getEntityName() + "/contracts/"
                    + CONTRACT_NO_PUMP.getContractId().getName()+ "/pumps/" + null)
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NOT_FOUND))
        ;

        // Delete contract
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.JSONV1)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(METHOD, DeleteMethod.DELETE_ALL)
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
