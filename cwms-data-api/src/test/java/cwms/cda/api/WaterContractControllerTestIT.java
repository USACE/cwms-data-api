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
import cwms.cda.data.dao.LookupTypeDao;
import cwms.cda.data.dao.project.ProjectDao;
import cwms.cda.data.dao.watersupply.WaterContractDao;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.Location;
import cwms.cda.data.dto.LookupType;
import cwms.cda.data.dto.project.Project;
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
import static org.hamcrest.Matchers.hasToString;


@Tag("integration")
class WaterContractControllerTestIT extends DataApiTestIT {
    private static final String OFFICE_ID = "SWT";
    private static final WaterUserContract CONTRACT;

    static {
        try (InputStream contractStream = WaterContractCreateController.class
            .getResourceAsStream("/cwms/cda/api/waterusercontract.json")) {
            assert contractStream != null;
            String contractJson = IOUtils.toString(contractStream, StandardCharsets.UTF_8);
            CONTRACT = Formats.parseContent(new ContentType(Formats.JSONV1), contractJson, WaterUserContract.class);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @BeforeAll
    static void setup() throws Exception {
        // create water user, locations, and project
        Location projectLocation = new Location.Builder(CONTRACT.getWaterUser().getProjectId())
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
        Project project = new Project.Builder().withLocation(projectLocation)
            .withFederalCost(BigDecimal.valueOf(123456789))
            .withAuthorizingLaw("NEW LAW").withCostUnit("$")
            .withProjectOwner(CONTRACT.getWaterUser().getEntityName())
            .build();
        LookupType contractType = CONTRACT.getContractType();

        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext ctx = getDslContext(c, OFFICE_ID);
            LocationsDaoImpl locationsDao = new LocationsDaoImpl(ctx);
            ProjectDao projectDao = new ProjectDao(ctx);
            LookupTypeDao lookupTypeDao = new LookupTypeDao(ctx);
            WaterContractDao waterContractDao = new WaterContractDao(ctx);
            try {
                locationsDao.storeLocation(projectLocation);
                lookupTypeDao.storeLookupType("AT_WS_CONTRACT_TYPE", "WS_CONTRACT_TYPE", contractType);
                projectDao.store(project, true);
                waterContractDao.storeWaterUser(CONTRACT.getWaterUser(), true);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    @AfterAll
    static void cleanup() throws Exception {
        LookupType contractType = CONTRACT.getContractType();

        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext ctx = getDslContext(c, OFFICE_ID);
            LocationsDaoImpl locationsDao = new LocationsDaoImpl(ctx);
            LookupTypeDao lookupTypeDao = new LookupTypeDao(ctx);
            ProjectDao projectDao = new ProjectDao(ctx);
            WaterContractDao waterContractDao = new WaterContractDao(ctx);
            waterContractDao.deleteWaterUser(CONTRACT.getWaterUser().getProjectId(),
                CONTRACT.getWaterUser().getEntityName(), DeleteMethod.DELETE_ALL);
            projectDao.delete(CONTRACT.getOfficeId(), CONTRACT.getWaterUser().getProjectId().getName(),
                DeleteRule.DELETE_ALL);
            locationsDao.deleteLocation(CONTRACT.getWaterUser().getProjectId().getName(),
                CONTRACT.getWaterUser().getProjectId().getOfficeId(), true);
            lookupTypeDao.deleteLookupType("AT_WS_CONTRACT_TYPE", "WS_CONTRACT_TYPE", contractType.getOfficeId(),
                contractType.getDisplayValue());
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    @Test
    void test_create_get_delete_WaterUserContract() {
        // Test Structure:
        // 1) Create a Water Contract
        // 2) Get the WaterUserContract, assert that it is same as created contract
        // 3) Delete the WaterUserContract
        // 4) Get the WaterUserContract, assert that it is not found

        TestAccounts.KeyUser user = TestAccounts.KeyUser.SWT_NORMAL;
        String json = Formats.format(Formats.parseHeader(Formats.JSONV1, WaterUserContract.class), CONTRACT);

        // Create contract
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.JSONV1)
            .body(json)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(FAIL_IF_EXISTS, "true")
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

        // get contract
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.JSONV1)
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/projects/" + OFFICE_ID + "/" + CONTRACT.getWaterUser().getProjectId().getName()
                    + "/water-user/" + CONTRACT.getWaterUser().getEntityName()
                    + "/contracts/" + CONTRACT.getContractId().getName())
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("office-id", equalTo(CONTRACT.getOfficeId()))
            .body("water-user.entity-name", equalTo(CONTRACT.getWaterUser().getEntityName()))
            .body("water-user.project-id.office-id", equalTo(CONTRACT.getWaterUser()
                    .getProjectId().getOfficeId()))
            .body("water-user.project-id.name", equalTo(CONTRACT.getWaterUser().getProjectId().getName()))
            .body("water-user.water-right", equalTo(CONTRACT.getWaterUser().getWaterRight()))
            .body("contract-type.office-id", equalTo(CONTRACT.getContractType().getOfficeId()))
            .body("contract-type.display-value", equalTo(CONTRACT.getContractType().getDisplayValue()))
            .body("contract-type.tooltip", equalTo(CONTRACT.getContractType().getTooltip()))
            .body("contract-type.active", equalTo(CONTRACT.getContractType().getActive()))
            .body("contract-effective-date",
                    hasToString(String.valueOf(CONTRACT.getContractEffectiveDate().toEpochMilli())))
            .body("contract-expiration-date",
                    hasToString(String.valueOf(CONTRACT.getContractExpirationDate().toEpochMilli())))
            .body("contracted-storage", hasToString(String.valueOf(CONTRACT.getContractedStorage())))
            .body("initial-use-allocation", hasToString(String.valueOf(CONTRACT.getInitialUseAllocation())))
            .body("future-use-allocation", hasToString(String.valueOf(CONTRACT.getFutureUseAllocation())))
            .body("storage-units-id", hasToString(String.valueOf(CONTRACT.getStorageUnitsId())))
            .body("future-use-percent-activated",
                    hasToString(String.valueOf(CONTRACT.getFutureUsePercentActivated())))
            .body("total-alloc-percent-activated",
                    hasToString(String.valueOf(CONTRACT.getTotalAllocPercentActivated())))
            .body("pump-out-location.pump-location.office-id", equalTo(CONTRACT.getPumpOutLocation()
                    .getPumpLocation().getOfficeId()))
            .body("pump-out-location.pump-location.name",
                    hasToString(String.valueOf(CONTRACT.getPumpOutLocation().getPumpLocation().getName())))
            .body("pump-out-location.pump-location.latitude",
                    hasToString(String.valueOf(CONTRACT.getPumpOutLocation().getPumpLocation().getLatitude())))
            .body("pump-out-location.pump-location.longitude",
                    hasToString(String.valueOf(CONTRACT.getPumpOutLocation().getPumpLocation().getLongitude())))
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
            .body("pump-out-location.pump-location.location-type",
                    hasToString(String.valueOf(CONTRACT.getPumpOutLocation().getPumpLocation().getLocationType())))
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
            .body("pump-out-location.pump-location.published-latitude",
                    hasToString(String.valueOf(CONTRACT.getPumpOutLocation()
                            .getPumpLocation().getPublishedLatitude())))
            .body("pump-out-location.pump-location.published-longitude",
                    hasToString(String.valueOf(CONTRACT.getPumpOutLocation()
                    .getPumpLocation().getPublishedLongitude())))
            .body("pump-out-location.pump-location.vertical-datum", equalTo(CONTRACT.getPumpOutLocation()
                    .getPumpLocation().getVerticalDatum()))
            .body("pump-out-location.pump-location.elevation",
                    hasToString(String.valueOf(CONTRACT.getPumpOutLocation().getPumpLocation().getElevation())))
            .body("pump-out-location.pump-location.map-label", equalTo(CONTRACT.getPumpOutLocation()
                    .getPumpLocation().getMapLabel()))
            .body("pump-out-location.pump-location.bounding-office-id", equalTo(CONTRACT.getPumpOutLocation()
                    .getPumpLocation().getBoundingOfficeId()))
            .body("pump-out-location.pump-location.elevation-units", equalTo(CONTRACT.getPumpOutLocation()
                    .getPumpLocation().getElevationUnits()))
            .body("pump-out-below-location.pump-location.office-id",
                    equalTo(CONTRACT.getPumpOutBelowLocation().getPumpLocation().getOfficeId()))
            .body("pump-out-below-location.pump-location.name",
                    hasToString(String.valueOf(CONTRACT.getPumpOutBelowLocation().getPumpLocation().getName())))
            .body("pump-out-below-location.pump-location.latitude",
                    hasToString(String.valueOf(CONTRACT.getPumpOutBelowLocation().getPumpLocation().getLatitude())))
            .body("pump-out-below-location.pump-location.longitude",
                    hasToString(String.valueOf(CONTRACT.getPumpOutBelowLocation()
                            .getPumpLocation().getLongitude())))
            .body("pump-out-below-location.pump-location.active", equalTo(CONTRACT.getPumpOutBelowLocation()
                    .getPumpLocation().getActive()))
            .body("pump-out-below-location.pump-location.public-name",
                    equalTo(CONTRACT.getPumpOutBelowLocation().getPumpLocation().getPublicName()))
            .body("pump-out-below-location.pump-location.long-name",
                    equalTo(CONTRACT.getPumpOutBelowLocation().getPumpLocation().getLongName()))
            .body("pump-out-below-location.pump-location.description",
                    equalTo(CONTRACT.getPumpOutBelowLocation().getPumpLocation().getDescription()))
            .body("pump-out-below-location.pump-location.timezone-name",
                    equalTo(CONTRACT.getPumpOutBelowLocation().getPumpLocation().getTimezoneName()))
            .body("pump-out-below-location.pump-location.location-type",
                    hasToString(String.valueOf(CONTRACT.getPumpOutBelowLocation()
                    .getPumpLocation().getLocationType())))
            .body("pump-out-below-location.pump-location.location-kind",
                    equalTo(CONTRACT.getPumpOutBelowLocation().getPumpLocation().getLocationKind()))
            .body("pump-out-below-location.pump-location.nation", equalTo(CONTRACT.getPumpOutBelowLocation()
                    .getPumpLocation().getNation().toString()))
            .body("pump-out-below-location.pump-location.state-initial",
                    equalTo(CONTRACT.getPumpOutBelowLocation().getPumpLocation().getStateInitial()))
            .body("pump-out-below-location.pump-location.county-name",
                    equalTo(CONTRACT.getPumpOutBelowLocation().getPumpLocation().getCountyName()))
            .body("pump-out-below-location.pump-location.nearest-city",
                    equalTo(CONTRACT.getPumpOutBelowLocation().getPumpLocation().getNearestCity()))
            .body("pump-out-below-location.pump-location.horizontal-datum",
                    equalTo(CONTRACT.getPumpOutBelowLocation().getPumpLocation().getHorizontalDatum()))
            .body("pump-out-below-location.pump-location.published-latitude",
                    hasToString(String.valueOf(CONTRACT.getPumpOutBelowLocation()
                            .getPumpLocation().getPublishedLatitude())))
            .body("pump-out-below-location.pump-location.published-longitude",
                    hasToString(String.valueOf(CONTRACT.getPumpOutBelowLocation()
                            .getPumpLocation().getPublishedLongitude())))
            .body("pump-out-below-location.pump-location.vertical-datum",
                    equalTo(CONTRACT.getPumpOutBelowLocation().getPumpLocation().getVerticalDatum()))
            .body("pump-out-below-location.pump-location.elevation",
                    hasToString(String.valueOf(CONTRACT.getPumpOutBelowLocation()
                            .getPumpLocation().getElevation())))
            .body("pump-out-below-location.pump-location.map-label",
                    equalTo(CONTRACT.getPumpOutBelowLocation().getPumpLocation().getMapLabel()))
            .body("pump-out-below-location.pump-location.bounding-office-id",
                    equalTo(CONTRACT.getPumpOutBelowLocation().getPumpLocation().getBoundingOfficeId()))
            .body("pump-out-below-location.pump-location.elevation-units",
                    equalTo(CONTRACT.getPumpOutBelowLocation().getPumpLocation().getElevationUnits()))
            .body("pump-in-location.pump-location.office-id",
                    equalTo(CONTRACT.getPumpInLocation().getPumpLocation().getOfficeId()))
            .body("pump-in-location.pump-location.name",
                    equalTo(CONTRACT.getPumpInLocation().getPumpLocation().getName()))
            .body("pump-in-location.pump-location.active",
                    equalTo(CONTRACT.getPumpInLocation().getPumpLocation().getActive()))
            .body("pump-in-location.pump-location.public-name",
                    equalTo(CONTRACT.getPumpInLocation().getPumpLocation().getPublicName()))
            .body("pump-in-location.pump-location.long-name",
                    equalTo(CONTRACT.getPumpInLocation().getPumpLocation().getLongName()))
            .body("pump-in-location.pump-location.description",
                    equalTo(CONTRACT.getPumpInLocation().getPumpLocation().getDescription()))
            .body("pump-in-location.pump-location.timezone-name",
                    equalTo(CONTRACT.getPumpInLocation().getPumpLocation().getTimezoneName()))
            .body("pump-in-location.pump-location.location-kind",
                    equalTo(CONTRACT.getPumpInLocation().getPumpLocation().getLocationKind()))
            .body("pump-in-location.pump-location.location-type",
                    hasToString(String.valueOf(CONTRACT.getPumpInLocation().getPumpLocation().getLocationType())))
            .body("pump-in-location.pump-location.nation",
                    equalTo(CONTRACT.getPumpInLocation().getPumpLocation().getNation().toString()))
                .body("pump-in-location.pump-location.published-latitude",
                        hasToString(String.valueOf(CONTRACT
                        .getPumpInLocation().getPumpLocation().getPublishedLatitude())))
                .body("pump-in-location.pump-location.published-longitude",
                        hasToString(String.valueOf(CONTRACT.getPumpInLocation()
                                .getPumpLocation().getPublishedLongitude())))
            .body("pump-in-location.pump-location.state-initial",
                    equalTo(CONTRACT.getPumpInLocation().getPumpLocation().getStateInitial()))
            .body("pump-in-location.pump-location.county-name",
                    equalTo(CONTRACT.getPumpInLocation().getPumpLocation().getCountyName()))
            .body("pump-in-location.pump-location.nearest-city",
                    equalTo(CONTRACT.getPumpInLocation().getPumpLocation().getNearestCity()))
            .body("pump-in-location.pump-location.horizontal-datum",
                    equalTo(CONTRACT.getPumpInLocation().getPumpLocation().getHorizontalDatum()))
            .body("pump-in-location.pump-location.vertical-datum",
                    equalTo(CONTRACT.getPumpInLocation().getPumpLocation().getVerticalDatum()))
            .body("pump-in-location.pump-location.map-label",
                    equalTo(CONTRACT.getPumpInLocation().getPumpLocation().getMapLabel()))
            .body("pump-in-location.pump-location.bounding-office-id",
                    equalTo(CONTRACT.getPumpInLocation().getPumpLocation().getBoundingOfficeId()))
            .body("pump-in-location.pump-location.elevation-units",
                    equalTo(CONTRACT.getPumpInLocation().getPumpLocation().getElevationUnits()))
            .body("pump-in-location.pump-location.latitude",
                    hasToString(String.valueOf(CONTRACT.getPumpInLocation().getPumpLocation().getLatitude())))
            .body("pump-in-location.pump-location.longitude",
                    hasToString(String.valueOf(CONTRACT.getPumpInLocation().getPumpLocation().getLongitude())))
            .body("pump-in-location.pump-location.elevation",
                    hasToString(String.valueOf(CONTRACT.getPumpInLocation().getPumpLocation().getElevation())))
        ;

        // delete contract
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .queryParam(METHOD, DeleteMethod.DELETE_ALL)
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("/projects/" + OFFICE_ID + "/" + CONTRACT.getWaterUser().getProjectId().getName() + "/water-user/"
                    + CONTRACT.getWaterUser().getEntityName() + "/contracts/"
                    + CONTRACT.getContractId().getName())
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NO_CONTENT))
        ;

        // get contract, assert that it doesn't exist
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.JSONV1)
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/projects/" + OFFICE_ID + "/" + CONTRACT.getWaterUser().getProjectId().getName()
                    + "/water-user/" + CONTRACT.getWaterUser().getEntityName()
                    + "/contracts/" + CONTRACT.getContractId().getName())
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NOT_FOUND))
        ;
    }


    @Test
    void test_rename_WaterUserContract() {
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SWT_NORMAL;
        final String NEW_CONTRACT_NAME = "NEW CONTRACT NAME";
        String json = Formats.format(Formats.parseHeader(Formats.JSONV1, WaterUserContract.class), CONTRACT);

        // create contract
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.JSONV1)
            .body(json)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(FAIL_IF_EXISTS, "false")
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

        // rename contract
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.JSONV1)
            .body(json)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(CONTRACT_NAME, NEW_CONTRACT_NAME)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .patch("/projects/" + OFFICE + "/" + CONTRACT.getWaterUser().getProjectId().getName()
                    + "/water-user/" + CONTRACT.getWaterUser().getEntityName()
                    + "/contracts/" + CONTRACT.getContractId().getName())
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
        ;

        // get contract, assert name is changed
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.JSONV1)
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/projects/" + OFFICE_ID + "/" + CONTRACT.getWaterUser().getProjectId().getName()
                    + "/water-user/" + CONTRACT.getWaterUser().getEntityName() + "/contracts/"
                    + NEW_CONTRACT_NAME)
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("office-id", equalTo(CONTRACT.getOfficeId()))
            .body("contract-id.name", equalTo(NEW_CONTRACT_NAME))
        ;

        // delete contract
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .queryParam(METHOD, "DELETE ALL")
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("/projects/" + OFFICE_ID + "/"
                    + CONTRACT.getWaterUser().getProjectId().getName() + "/water-user/"
                    + CONTRACT.getWaterUser().getEntityName() + "/contracts/"
                    + NEW_CONTRACT_NAME)
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NO_CONTENT))
        ;
    }

    @Test
    void test_getAllWaterContracts() throws Exception {
        TestAccounts.KeyUser user = TestAccounts.KeyUser.SWT_NORMAL;
        String json = Formats.format(Formats.parseHeader(Formats.JSONV1, WaterUserContract.class), CONTRACT);

        // create contract
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.JSONV1)
            .body(json)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(FAIL_IF_EXISTS, "false")
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

        WaterUserContract waterContract = new WaterUserContract.Builder()
                .withContractId(new CwmsId.Builder().withOfficeId(OFFICE_ID).withName("NEW CONTRACT").build())
                .withContractEffectiveDate(CONTRACT.getContractEffectiveDate())
                .withContractExpirationDate(CONTRACT.getContractExpirationDate())
                .withContractType(CONTRACT.getContractType())
                .withContractedStorage(CONTRACT.getContractedStorage())
                .withFutureUseAllocation(CONTRACT.getFutureUseAllocation())
                .withInitialUseAllocation(CONTRACT.getInitialUseAllocation())
                .withStorageUnitsId(CONTRACT.getStorageUnitsId())
                .withTotalAllocPercentActivated(CONTRACT.getTotalAllocPercentActivated())
                .withFutureUsePercentActivated(CONTRACT.getFutureUsePercentActivated())
                .withOfficeId(OFFICE_ID)
                .withWaterUser(CONTRACT.getWaterUser()).build();
        String json2 = JsonV1.buildObjectMapper().writeValueAsString(waterContract);

        // create contract
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.JSONV1)
            .body(json2)
            .header(AUTH_HEADER, user.toHeaderValue())
            .queryParam(FAIL_IF_EXISTS, "true")
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

        // get all contracts
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.JSONV1)
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/projects/" + OFFICE_ID + "/" + CONTRACT.getWaterUser().getProjectId().getName()
                    + "/water-user/" + CONTRACT.getWaterUser().getEntityName() + "/contracts")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("[0].office-id", equalTo(OFFICE_ID))
            .body("[0].contract-id.name", equalTo("NEW CONTRACT"))
            .body("[1].office-id", equalTo(CONTRACT.getOfficeId()))
            .body("[1].contract-id.name", equalTo(CONTRACT.getContractId().getName()))
        ;

        // delete contract
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .queryParam(METHOD, "DELETE ALL")
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .delete("/projects/" + OFFICE_ID + "/"
                    + CONTRACT.getWaterUser().getProjectId().getName() + "/water-user/"
                    + CONTRACT.getWaterUser().getEntityName() + "/contracts/"
                    + CONTRACT.getContractId().getName())
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_NO_CONTENT))
        ;

        // delete contract
        given()
                .log().ifValidationFails(LogDetail.ALL, true)
                .queryParam(METHOD, "DELETE ALL")
                .header(AUTH_HEADER, user.toHeaderValue())
                .when()
                .redirects().follow(true)
                .redirects().max(3)
                .delete("/projects/" + OFFICE_ID + "/"
                        + CONTRACT.getWaterUser().getProjectId().getName() + "/water-user/"
                        + CONTRACT.getWaterUser().getEntityName() + "/contracts/"
                        + "NEW CONTRACT")
                .then()
                .log().ifValidationFails(LogDetail.ALL, true)
                .assertThat()
                .statusCode(is(HttpServletResponse.SC_NO_CONTENT))
        ;
    }
}