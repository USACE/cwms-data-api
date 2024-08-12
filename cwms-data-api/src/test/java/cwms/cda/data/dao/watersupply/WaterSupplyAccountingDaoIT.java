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

package cwms.cda.data.dao.watersupply;

import cwms.cda.api.DataApiTestIT;
import cwms.cda.api.enums.Nation;
import cwms.cda.data.dao.DeleteRule;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dao.LocationsDaoImpl;
import cwms.cda.data.dao.LookupTypeDao;
import cwms.cda.data.dao.project.ProjectDao;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.Location;
import cwms.cda.data.dto.LookupType;
import cwms.cda.data.dto.project.Project;
import cwms.cda.data.dto.watersupply.PumpAccounting;
import cwms.cda.data.dto.watersupply.PumpType;
import cwms.cda.data.dto.watersupply.WaterSupplyAccounting;
import cwms.cda.data.dto.watersupply.WaterSupplyPump;
import cwms.cda.data.dto.watersupply.WaterUser;
import cwms.cda.data.dto.watersupply.WaterUserContract;
import cwms.cda.helpers.DTOMatch;
import fixtures.CwmsDataApiSetupCallback;
import mil.army.usace.hec.test.database.CwmsDatabaseContainer;
import org.jooq.DSLContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import java.util.ArrayList;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;
import static cwms.cda.data.dao.JooqDao.getDslContext;
import static org.junit.jupiter.api.Assertions.*;

@Tag("integration")
class WaterSupplyAccountingDaoIT extends DataApiTestIT {
    private static final String OFFICE_ID = "SPK";
    private static final JooqDao.DeleteMethod TEST_DELETE_ACTION = JooqDao.DeleteMethod.DELETE_ALL;
    private static final String PROJECT_NAME = "Test Project Name";
    private static final String WATER_USER_ENTITY_NAME = "Test entity";
    private static final Location testLocation = buildTestLocation(PROJECT_NAME, "Test Location");
    private static final Project testProject = buildTestProject();
    private static final WaterUser testUser = buildTestWaterUser(WATER_USER_ENTITY_NAME);
    private static final WaterUserContract contract = buildTestWaterContract("Contract Name");
    private static final LookupType testTransferType = new LookupType.Builder()
            .withDisplayValue("Test Transfer")
            .withTooltip("Test Location Tooltip")
            .withActive(true)
            .withOfficeId(OFFICE_ID)
            .build();

    @BeforeAll
    static void setup() throws Exception {
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection(c -> {
            DSLContext ctx = getDslContext(c, OFFICE_ID);
            LocationsDaoImpl dao = new LocationsDaoImpl(ctx);
            ProjectDao projectDao = new ProjectDao(ctx);
            WaterContractDao contractDao = new WaterContractDao(ctx);
            LookupTypeDao lookupTypeDao = new LookupTypeDao(ctx);
            try {
                dao.storeLocation(testLocation);
                lookupTypeDao.storeLookupType("AT_PHYSICAL_TRANSFER_TYPE", "PHYS_TRANS_TYPE", testTransferType);
                projectDao.store(testProject, true);
                contractDao.storeWaterUser(testUser, true);
                contractDao.storeWaterContract(contract, true, false);
            } catch (IOException e) {
                throw new RuntimeException("Failed to store location or project", e);
            }
        });
    }

    @AfterAll
    static void tearDown() throws Exception {
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection(c -> {
            DSLContext ctx = getDslContext(c, OFFICE_ID);
            LocationsDaoImpl dao = new LocationsDaoImpl(ctx);
            ProjectDao projectDao = new ProjectDao(ctx);
            LookupTypeDao lookupTypeDao = new LookupTypeDao(ctx);
            WaterContractDao contractDao = new WaterContractDao(ctx);
            contractDao.deleteWaterContract(contract, TEST_DELETE_ACTION);
            contractDao.deleteWaterUser(testUser.getProjectId(), testUser.getEntityName(), TEST_DELETE_ACTION);
            lookupTypeDao.deleteLookupType("AT_PHYSICAL_TRANSFER_TYPE", "PHYS_TRANS_TYPE",
                    OFFICE_ID, testTransferType.getDisplayValue());
            projectDao.delete(testProject.getLocation().getOfficeId(), testProject.getLocation().getName(),
                    DeleteRule.DELETE_ALL);
            dao.deleteLocation(testLocation.getName(), OFFICE_ID);
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"default", "startInclusiveTrue", "endInclusiveTrue", "startEndInclusiveTrue",
            "startInclusiveFalse", "endInclusiveFalse", "startEndInclusiveFalse"})
    void testStoreAndRetrieveWaterSupplyPumpAccounting(String method) throws Exception {

        // Test Structure
        // 1) Create and store a Water Supply Contract
        // 2) Create and store Water Supply Pump Accounting
        // 3) Retrieve Water Supply Pump Accounting and assert it is the same (or not in DB)

        WaterSupplyAccounting accounting = buildTestAccounting();

        Calendar instance = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        instance.clear();
        instance.set(2000, 2, 1, 0, 0);
        Instant startTime = instance.getTime().toInstant();
        instance.set(2326, 2, 1, 0, 0);
        Instant endTime = instance.getTime().toInstant();
        instance.clear();
        boolean startInclusive = true;
        boolean endInclusive = true;
        boolean inDB = true;
        switch (method) {
            case "default":
                instance.set(2025, 10, 1, 0, 0);
                startTime = instance.getTime().toInstant();
                break;
            case "startInclusiveTrue":
                instance.set(2000, 2, 1, 0, 0);
                startTime = instance.getTime().toInstant();
                break;
            case "endInclusiveTrue":
                instance.set(2326, 2, 1, 0, 0);
                endTime = instance.getTime().toInstant();
                break;
            case "startInclusiveFalse":
                instance.set(2286, 10, 21, 8, 53, 19);
                startTime = instance.getTime().toInstant();
                startInclusive = false;
                inDB = false;
                break;
            case "endInclusiveFalse":
                instance.set(2286, 10, 20, 21, 17, 28);
                endTime = instance.getTime().toInstant();
                endInclusive = false;
                inDB = false;
                break;
            case "startEndInclusiveFalse":
                instance.set(2325, 10, 1, 0, 0);
                startTime = instance.getTime().toInstant();
                startInclusive = false;
                endInclusive = false;
                inDB = false;
                break;
        }
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection(c -> {
            DSLContext ctx = getDslContext(c, OFFICE_ID);
            WaterSupplyAccountingDao accountingDao = new WaterSupplyAccountingDao(ctx);
            accountingDao.storeAccounting(accounting);
        }, CwmsDataApiSetupCallback.getWebUser());

        int rowLimit = 20;
        boolean headFlag = false;

        if (inDB) {
            // retrieve and assert in db
            assertPumpAccountingInDB(contract, startTime, endTime, startInclusive,
                    endInclusive, headFlag, rowLimit);
        } else {
            // retrieve and assert not in db
            assertPumpAccountingInDBEmpty(contract, startTime, endTime, startInclusive,
                    endInclusive, headFlag, rowLimit);
        }
    }

    private void assertPumpAccountingInDB(WaterUserContract contract, Instant startTime,
            Instant endTime, boolean startInclusive, boolean endInclusive, boolean headFlag,
            int rowLimit) throws Exception {

        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection(c -> {
            DSLContext ctx = getDslContext(c, OFFICE_ID);
            WaterSupplyAccountingDao accountingDao = new WaterSupplyAccountingDao(ctx);
            List<WaterSupplyAccounting> pumpAccounting = accountingDao.retrieveAccounting(contract.getContractId()
                            .getName(), contract.getWaterUser(), new CwmsId.Builder().withOfficeId(OFFICE_ID)
                            .withName(contract.getWaterUser().getProjectId().getName()).build(),
                    null, startTime, endTime, startInclusive, endInclusive, headFlag, rowLimit);
            assertFalse(pumpAccounting.isEmpty());
            for (WaterSupplyAccounting returnedAccounting : pumpAccounting) {
                assertNotNull(returnedAccounting.getPumpAccounting());
                DTOMatch.assertMatch(returnedAccounting, buildTestAccounting());
            }
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    private void assertPumpAccountingInDBEmpty(WaterUserContract contract,
            Instant startTime, Instant endTime, boolean startInclusive, boolean endInclusive,
            boolean headFlag, int rowLimit) throws Exception {
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection(c -> {
            DSLContext ctx = getDslContext(c, OFFICE_ID);
            WaterSupplyAccountingDao accountingDao = new WaterSupplyAccountingDao(ctx);
            List<WaterSupplyAccounting> pumpAccounting = accountingDao.retrieveAccounting(contract.getContractId()
                            .getName(), contract.getWaterUser(), new CwmsId.Builder().withName(contract.getWaterUser()
                            .getProjectId().getName()).withOfficeId(OFFICE_ID).build(),
                    null, startTime, endTime, startInclusive, endInclusive, headFlag, rowLimit);
            for (WaterSupplyAccounting returnedAccounting : pumpAccounting) {
                assertTrue(returnedAccounting.getPumpAccounting().isEmpty());
            }
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    protected static WaterUser buildTestWaterUser(String entityName) {
        return new WaterUser.Builder().withEntityName(entityName).withProjectId(new CwmsId.Builder()
                .withName(PROJECT_NAME)
                .withOfficeId(OFFICE_ID)
                .build())
            .withWaterRight("Test Water Right").build();
    }

    protected static WaterUserContract buildTestWaterContract(String entityName) {
        return new WaterUserContract.Builder()
                .withContractType(new LookupType.Builder()
                        .withTooltip("Storage contract")
                        .withActive(true)
                        .withDisplayValue("Storage")
                        .withOfficeId(OFFICE_ID).build())
                .withStorageUnitsId("m3")
                .withOfficeId(OFFICE_ID)
                .withFutureUseAllocation(158900.6)
                .withContractedStorage(1589005.6)
                .withInitialUseAllocation(1500.2)
                .withContractExpirationDate(Instant.ofEpochMilli(1979252516000L))
                .withContractEffectiveDate(Instant.ofEpochMilli(1766652851000L))
                .withTotalAllocPercentActivated(55.1)
                .withContractId(new CwmsId.Builder()
                        .withName(entityName)
                        .withOfficeId(OFFICE_ID)
                        .build())
                .withFutureUsePercentActivated(35.7)
                .withWaterUser(testUser)
                .withPumpInLocation(new WaterSupplyPump.Builder()
                        .withPumpLocation(buildTestLocation("Pump 1 " + entityName,
                        "PUMP")).withPumpType(PumpType.IN).build())
                .withPumpOutLocation(new WaterSupplyPump.Builder()
                        .withPumpLocation(buildTestLocation("Pump 2 " + entityName,
                        "PUMP")).withPumpType(PumpType.OUT).build())
                .withPumpOutBelowLocation(new WaterSupplyPump.Builder()
                        .withPumpLocation(buildTestLocation("Pump 3 " + entityName,
                        "PUMP")).withPumpType(PumpType.BELOW).build())
                .build();

    }

    protected static Location buildTestLocation(String locationName, String locationType) {
        return new Location.Builder(OFFICE_ID, locationName)
                .withBoundingOfficeId(OFFICE_ID)
                .withMapLabel("Test Map Label")
                .withElevation(150.6)
                .withNation(Nation.US)
                .withStateInitial("CA")
                .withCountyName("Sacramento")
                .withTimeZoneName(ZoneId.of("UTC"))
                .withElevationUnits("m")
                .withVerticalDatum("NGVD29")
                .withHorizontalDatum("WGS84")
                .withPublicName("Test Public Name")
                .withLongName("Test Long Name")
                .withDescription("Test Description")
                .withNearestCity("Davis")
                .withLatitude(35.6)
                .withLongitude(-120.6)
                .withPublishedLatitude(35.6)
                .withPublishedLongitude(-120.6)
                .withActive(true)
                .withLocationType(locationType)
                .withLocationKind(locationType)
                .build();
    }

    protected static Project buildTestProject() {
        return new Project.Builder().withLocation(buildTestLocation(PROJECT_NAME,
                        "Test Location Type"))
                .withFederalCost(new BigDecimal("15980654.55"))
                .build();
    }

    private WaterSupplyAccounting buildTestAccounting() {
        return new WaterSupplyAccounting.Builder().withWaterUser(testUser)
                .withContractName(contract.getContractId().getName()).withPumpAccounting(buildTestPumpAccountingList())
                .build();
    }

    private List<PumpAccounting> buildTestPumpAccountingList() {
        List<PumpAccounting> retList = new ArrayList<>();
        retList.add(new PumpAccounting.Builder().withPumpLocation(new CwmsId.Builder().withOfficeId(OFFICE_ID)
                        .withName(contract.getPumpInLocation().getPumpLocation().getName()).build())
                .withTransferType(testTransferType).withFlow(1.0)
                .withTransferDate(Instant.ofEpochMilli(10000012648000L)).withComment("Test Comment").build());
        retList.add(new PumpAccounting.Builder().withPumpLocation(new CwmsId.Builder().withOfficeId(OFFICE_ID)
                        .withName(contract.getPumpOutLocation().getPumpLocation().getName()).build()).withTransferType(testTransferType).withFlow(2.0)
                .withTransferDate(Instant.ofEpochMilli(10000054399000L)).withComment("Test Comment 2").build());
        return retList;
    }
}
