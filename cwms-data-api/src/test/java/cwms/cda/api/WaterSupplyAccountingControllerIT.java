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
import cwms.cda.data.dao.DeleteRule;
import cwms.cda.data.dao.JooqDao.DeleteMethod;
import cwms.cda.data.dao.LocationsDaoImpl;
import cwms.cda.data.dao.LookupTypeDao;
import cwms.cda.data.dao.project.ProjectDao;
import cwms.cda.data.dao.watersupply.WaterContractDao;
import cwms.cda.data.dto.Location;
import cwms.cda.data.dto.LookupType;
import cwms.cda.data.dto.project.Project;
import cwms.cda.data.dto.watersupply.WaterSupplyAccounting;
import cwms.cda.data.dto.watersupply.WaterUser;
import cwms.cda.data.dto.watersupply.WaterUserContract;
import cwms.cda.formatters.ContentType;
import cwms.cda.formatters.Formats;
import cwms.cda.formatters.json.JsonV1;
import fixtures.CwmsDataApiSetupCallback;
import fixtures.TestAccounts;
import io.restassured.filter.log.LogDetail;
import mil.army.usace.hec.test.database.CwmsDatabaseContainer;
import org.jooq.DSLContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.org.apache.commons.io.IOUtils;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;

import static cwms.cda.data.dao.DaoTest.getDslContext;
import static cwms.cda.security.KeyAccessManager.AUTH_HEADER;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

@Tag("integration")
class WaterSupplyAccountingControllerIT extends DataApiTestIT {
    private static final String OFFICE_ID = "SWT";
    private static final WaterSupplyAccounting WATER_SUPPLY_ACCOUNTING;
    private static final String START_TIME = "start";
    private static final String START_INCLUSIVE = "start-inclusive";
    private static final String END_INCLUSIVE = "end-inclusive";
    private static final String END_TIME = "end";
    private static final String ROW_LIMIT = "row-limit";
    private static final String ASCENDING = "ascending";
    private static final WaterUserContract CONTRACT;
    private static final LookupType testTransferType;
    private static final LookupType testContractType;
    private static final Location pump1;
    private static final Location pump3;

    static {
        try (InputStream accountStream = WaterSupplyAccounting.class
                .getResourceAsStream("/cwms/cda/api/pump_accounting.json");
                InputStream contractStream = WaterUserContract.class
                        .getResourceAsStream("/cwms/cda/api/waterusercontract.json")) {
            assert accountStream != null;
            assert contractStream != null;
            String contractJson = org.apache.commons.io.IOUtils.toString(contractStream, StandardCharsets.UTF_8);
            CONTRACT = Formats.parseContent(new ContentType(Formats.JSONV1), contractJson, WaterUserContract.class);
            String accountingJson = IOUtils.toString(accountStream, StandardCharsets.UTF_8);
            WATER_SUPPLY_ACCOUNTING = Formats.parseContent(new ContentType(Formats.JSONV1),
                    accountingJson, WaterSupplyAccounting.class);
            testTransferType = WATER_SUPPLY_ACCOUNTING.getPumpAccounting().get(0).getTransferType();
            testContractType = CONTRACT.getContractType();
            pump1 = buildTestLocation(WATER_SUPPLY_ACCOUNTING.getPumpAccounting().get(0).getPumpLocation().getName(),
                    "PUMP");
            pump3 = buildTestLocation(WATER_SUPPLY_ACCOUNTING.getPumpAccounting().get(1).getPumpLocation().getName(),
                    "PUMP");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @BeforeAll
    static void setup() throws Exception {
        // create water contract parent location
        // create water user
        // create water user contract

        Location contractLocation = buildTestLocation(CONTRACT.getContractId().getName(), "PROJECT");
        Location parentLocation = buildTestLocation(CONTRACT.getWaterUser().getProjectId().getName(), "PROJECT");

        Project project = new Project.Builder().withLocation(parentLocation)
                .withFederalCost(BigDecimal.valueOf(123456789))
                .withAuthorizingLaw("NEW LAW").withCostUnit("$")
                .withProjectOwner(CONTRACT.getWaterUser().getEntityName())
                .build();

        WaterUser waterUser = CONTRACT.getWaterUser();

        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext ctx = getDslContext(c, OFFICE_ID);
            LocationsDaoImpl locationsDao = new LocationsDaoImpl(ctx);
            ProjectDao projectDao = new ProjectDao(ctx);
            LookupTypeDao lookupTypeDao = new LookupTypeDao(ctx);
            WaterContractDao waterContractDao = new WaterContractDao(ctx);
            try {
                lookupTypeDao.storeLookupType("AT_PHYSICAL_TRANSFER_TYPE","PHYS_TRANS_TYPE",
                        testTransferType);
                lookupTypeDao.storeLookupType("AT_WS_CONTRACT_TYPE","WS_CONTRACT_TYPE",
                        testContractType);
                locationsDao.storeLocation(contractLocation);
                locationsDao.storeLocation(parentLocation);
                locationsDao.storeLocation(pump1);
                locationsDao.storeLocation(pump3);
                projectDao.store(project, true);
                waterContractDao.storeWaterUser(waterUser, true);
                waterContractDao.storeWaterContract(CONTRACT, true, true);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }, CwmsDataApiSetupCallback.getWebUser());

    }

    @AfterAll
    static void cleanup() throws Exception {
        // delete water user contract
        // delete water user
        // delete water contract parent location

        Location contractLocation = new Location.Builder(CONTRACT.getContractId().getOfficeId(),
                CONTRACT.getContractId().getName()).withLocationKind("PROJECT")
                .withTimeZoneName(ZoneId.of("UTC"))
                .withHorizontalDatum("WGS84").withLongitude(78.0).withLatitude(67.9).build();
        Location parentLocation = new Location.Builder(CONTRACT.getWaterUser().getProjectId().getOfficeId(),
                CONTRACT.getWaterUser().getProjectId().getName()).withLocationKind("PROJECT")
                .withTimeZoneName(ZoneId.of("UTC")).withHorizontalDatum("WGS84")
                .withLongitude(38.0).withLatitude(56.5).build();

        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
            DSLContext ctx = getDslContext(c, OFFICE_ID);
            LocationsDaoImpl locationsDao = new LocationsDaoImpl(ctx);
            LookupTypeDao lookupTypeDao = new LookupTypeDao(ctx);
            ProjectDao projectDao = new ProjectDao(ctx);
            WaterContractDao waterContractDao = new WaterContractDao(ctx);
            waterContractDao.deleteWaterContract(CONTRACT, DeleteMethod.DELETE_ALL);
            lookupTypeDao.deleteLookupType("AT_PHYSICAL_TRANSFER_TYPE", "PHYS_TRANS_TYPE",
                    OFFICE_ID, testTransferType.getDisplayValue());
            lookupTypeDao.deleteLookupType("AT_WS_CONTRACT_TYPE", "WS_CONTRACT_TYPE",
                    OFFICE_ID, testContractType.getDisplayValue());
            projectDao.delete(CONTRACT.getOfficeId(), CONTRACT.getWaterUser().getProjectId().getName(),
                    DeleteRule.DELETE_ALL);
            locationsDao.deleteLocation(pump1.getName(), pump1.getOfficeId(), true);
            locationsDao.deleteLocation(pump3.getName(), pump3.getOfficeId(), true);
            locationsDao.deleteLocation(contractLocation.getName(), contractLocation.getOfficeId(), true);
            locationsDao.deleteLocation(parentLocation.getName(), parentLocation.getOfficeId(), true);
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    @Test
    void testCreateRetrieveWaterAccounting() throws Exception {
        // Test Structure
        // 1) Create pump accounting
        // 2) Store pump accounting
        // 3) Retrieve pump accounting
        // 4) Assert pump accounting is same as created

        TestAccounts.KeyUser user = TestAccounts.KeyUser.SWT_NORMAL;
        String json = JsonV1.buildObjectMapper().writeValueAsString(WATER_SUPPLY_ACCOUNTING);


        // create pump accounting
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.JSONV1)
            .body(json)
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/projects/" + OFFICE_ID + "/" + CONTRACT.getWaterUser().getProjectId().getName() + "/water-user/"
                + CONTRACT.getWaterUser().getEntityName() + "/contracts/"
                + CONTRACT.getContractId().getName() + "/accounting")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED))
        ;

        // retrieve pump accounting
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.JSONV1)
            .header(AUTH_HEADER, user.toHeaderValue())
            .accept(Formats.JSONV1)
            .queryParam(START_TIME, "2005-04-05T00:00:00Z")
            .queryParam(END_TIME, "2335-04-06T00:00:00Z")
            .queryParam(START_INCLUSIVE, "true")
            .queryParam(END_INCLUSIVE, "true")
            .queryParam(ASCENDING, "true")
            .queryParam(ROW_LIMIT, 100)
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/projects/" + OFFICE_ID + "/" + CONTRACT.getWaterUser().getProjectId().getName() + "/water-user/"
                + CONTRACT.getWaterUser().getEntityName() + "/contracts/"
                + CONTRACT.getContractId().getName() + "/accounting")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body("[0].contract-name", equalTo(WATER_SUPPLY_ACCOUNTING.getContractName()))
            .body("[0].water-user.entity-name", equalTo(WATER_SUPPLY_ACCOUNTING.getWaterUser().getEntityName()))
            .body("[0].water-user.project-id.name", equalTo(WATER_SUPPLY_ACCOUNTING.getWaterUser().getProjectId().getName()))
            .body("[0].water-user.project-id.office-id", equalTo(WATER_SUPPLY_ACCOUNTING.getWaterUser().getProjectId().getOfficeId()))
            .body("[0].water-user.water-right", equalTo(WATER_SUPPLY_ACCOUNTING.getWaterUser().getWaterRight()))
            .body("[0].pump-accounting[0].transfer-type.display-value", equalTo(testTransferType.getDisplayValue()))
            .body("[0].pump-accounting[1].pump-location.name", equalTo(WATER_SUPPLY_ACCOUNTING.getPumpAccounting().get(0).getPumpLocation().getName()))
            .body("[0].pump-accounting[0].pump-location.name", equalTo(WATER_SUPPLY_ACCOUNTING.getPumpAccounting().get(1).getPumpLocation().getName()))
        ;
    }

    @Test
    void testRetrieveNotFoundOutsideTimeWindow() throws Exception {

        // Test Structure
        // 1) Store accounting
        // 2) Retrieve accounting outside time window
        // 3) Assert not found

        TestAccounts.KeyUser user = TestAccounts.KeyUser.SWT_NORMAL;
        String json = JsonV1.buildObjectMapper().writeValueAsString(WATER_SUPPLY_ACCOUNTING);


        // create pump accounting
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.JSONV1)
            .body(json)
            .header(AUTH_HEADER, user.toHeaderValue())
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .post("/projects/" + OFFICE_ID + "/" + CONTRACT.getWaterUser().getProjectId().getName() + "/water-user/"
                    + CONTRACT.getWaterUser().getEntityName() + "/contracts/"
                    + CONTRACT.getContractId().getName() + "/accounting")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_CREATED))
        ;

        // retrieve pump accounting
        given()
            .log().ifValidationFails(LogDetail.ALL, true)
            .contentType(Formats.JSONV1)
            .header(AUTH_HEADER, user.toHeaderValue())
            .accept(Formats.JSONV1)
            .queryParam(START_TIME, "2055-04-05T00:00:00Z")
            .queryParam(END_TIME, "2085-04-06T00:00:00Z")
        .when()
            .redirects().follow(true)
            .redirects().max(3)
            .get("/projects/" + OFFICE_ID + "/" + CONTRACT.getWaterUser().getProjectId().getName() + "/water-user/"
                    + CONTRACT.getWaterUser().getEntityName() + "/contracts/"
                    + CONTRACT.getContractId().getName() + "/accounting")
        .then()
            .log().ifValidationFails(LogDetail.ALL, true)
        .assertThat()
            .statusCode(is(HttpServletResponse.SC_OK))
            .body(is("[]"))
        ;
    }

    private static Location buildTestLocation(String name, String locationKind) {
        return new Location.Builder(OFFICE_ID, name).withLocationKind(locationKind)
                .withTimeZoneName(ZoneId.of("UTC"))
                .withHorizontalDatum("NAD84").withLongitude(-121.73).withLatitude(38.56).withVerticalDatum("WGS84")
                .withLongName("TEST CONTRACT LOCATION").withActive(true).withMapLabel("LABEL").withNation(Nation.US)
                .withElevation(456.7).withElevationUnits("m").withPublishedLongitude(-121.73).withPublishedLatitude(38.56)
                .withLocationType(locationKind).withDescription("TEST PROJECT").build();
    }
}
