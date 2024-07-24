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
import cwms.cda.data.dao.LocationsDaoImpl;
import cwms.cda.data.dao.LookupTypeDao;
import cwms.cda.data.dao.project.ProjectDao;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.Location;
import cwms.cda.data.dto.LookupType;
import cwms.cda.data.dto.project.Project;
import cwms.cda.data.dto.watersupply.PumpType;
import cwms.cda.data.dto.watersupply.WaterSupplyAccounting;
import cwms.cda.data.dto.watersupply.WaterSupplyPump;
import cwms.cda.data.dto.watersupply.WaterSupplyPumpAccounting;
import cwms.cda.data.dto.watersupply.WaterUser;
import cwms.cda.data.dto.watersupply.WaterUserContract;
import cwms.cda.helpers.DTOMatch;
import fixtures.CwmsDataApiSetupCallback;
import mil.army.usace.hec.test.database.CwmsDatabaseContainer;
import org.jooq.DSLContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import java.util.Date;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Calendar;
import java.util.HashMap;
import java.util.NavigableMap;
import java.util.TimeZone;
import java.util.TreeMap;
import static cwms.cda.data.dao.JooqDao.getDslContext;
import static org.junit.jupiter.api.Assertions.*;

class WaterSupplyAccountingDaoIT extends DataApiTestIT {
    private static final String OFFICE_ID = "SPK";
    private static final String TEST_DELETE_ACTION = "DELETE ALL";
    private static final Location testLocation = buildTestLocation("Test Project Name", "Test Location");
    private static final Project testProject = buildTestProject();
    private static final WaterUser testUser = buildTestWaterUser("Test User");
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
            LookupTypeDao lookupTypeDao = new LookupTypeDao(ctx);
            try {
                dao.storeLocation(testLocation);
                lookupTypeDao.storeLookupType("AT_PHYSICAL_TRANSFER_TYPE", "PHYS_TRANS_TYPE", testTransferType);
                projectDao.store(testProject, true);
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

        WaterUserContract contract = buildTestWaterContract("Test entity");

        WaterSupplyAccounting accounting = new WaterSupplyAccounting.Builder()
                .withContractName(contract.getContractId().getName()).withWaterUser(testUser)
                .withPumpLocationMap(new HashMap<>()).withPumpTimeWindowMap(new HashMap<>()).build();

        WaterSupplyPump pumpIn = buildTestWaterSupplyPump(contract.getPumpInLocation()
                .getPumpLocation().getName(), PumpType.IN);

        Calendar instance = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        instance.clear();

        boolean generateModifiedTimeWindow = true;
        boolean preserveModifiedData = true;
        boolean startInclusive = true;
        boolean endInclusive = true;
        boolean inDB = true;
        switch (method) {
            case "default":
                instance.set(2025, 10, 1, 0, 0);
                break;
            case "startInclusiveTrue":
                instance.set(2000, 2, 1, 0, 0);
                break;
            case "endInclusiveTrue":
                instance.set(2026, 2, 1, 0, 0);
                break;
            case "startInclusiveFalse":
                instance.set(2000, 2, 1, 0, 0);
                startInclusive = false;
                inDB = false;
                break;
            case "endInclusiveFalse":
                instance.set(2026, 2, 1, 0, 0);
                endInclusive = false;
                inDB = false;
                break;
            case "startEndInclusiveFalse":
                instance.set(2025, 10, 1, 0, 0);
                startInclusive = false;
                endInclusive = false;
                inDB = false;
                break;
        }
        Instant pumpChangeDate = instance.getTime().toInstant();
        CwmsId pumpId = new CwmsId.Builder().withName(pumpIn.getPumpLocation().getName())
                .withOfficeId(OFFICE_ID).build();
        NavigableMap<Instant, WaterSupplyPumpAccounting> pumpChangesMap = createPumpChangesMap(pumpId,
                contract.getWaterUser(), contract, pumpChangeDate);
        accounting.mergeAccounting(new CwmsId.Builder().withName(pumpIn.getPumpLocation().getName())
                        .withOfficeId(OFFICE_ID).build(),
                pumpChangesMap, generateModifiedTimeWindow, preserveModifiedData);

        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection(c -> {
            DSLContext ctx = getDslContext(c, OFFICE_ID);
            WaterContractDao contractDao = new WaterContractDao(ctx);
            WaterSupplyAccountingDao accountingDao = new WaterSupplyAccountingDao(ctx);
            contractDao.storeWaterUser(testUser, false);
            contractDao.storeWaterContract(contract, true, false);
            accountingDao.storeAccounting(accounting);
        }, CwmsDataApiSetupCallback.getWebUser());

        instance.clear();
        instance.set(2000, 2, 1, 0, 0);
        Instant startTime = instance.getTime().toInstant();
        instance.set(2026, 2, 1, 0, 0);
        Instant endTime = instance.getTime().toInstant();
        int rowLimit = 20;
        boolean headFlag = false;

        if (inDB) {
            // retrieve and assert in db
            assertPumpAccountingInDB(contract, pumpChangesMap, pumpChangeDate, startTime, endTime, startInclusive,
                    endInclusive, headFlag, rowLimit);
        } else {
            // retrieve and assert not in db
            assertPumpAccountingInDBEmpty(contract, pumpChangeDate, startTime, endTime, startInclusive,
                    endInclusive, headFlag, rowLimit);
        }

        // cleanup
        cleanupPump(pumpIn, contract);
        cleanupContractRoutine(contract);
    }


    private WaterSupplyPump buildTestWaterSupplyPump(String locationName, PumpType pumpType) {
        return new WaterSupplyPump.Builder().withPumpLocation(buildTestLocation(locationName, "PUMP"))
                .withPumpType(pumpType).build();
    }

    private NavigableMap<Instant, WaterSupplyPumpAccounting> createPumpChangesMap(CwmsId pumpLocation, WaterUser user,
            WaterUserContract contract, Instant date) {
        NavigableMap<Instant, WaterSupplyPumpAccounting> changeMap = new TreeMap<>();
        double randomAccountingFlow = 100 + (int) (Math.random() * ((1000 - 100) + 1));
        String comment = "Test Comment";
        WaterSupplyPumpAccounting pumpAccounting = new WaterSupplyPumpAccounting.Builder().withWaterUser(user)
                .withContractName(contract.getContractId().getName()).withPumpLocation(pumpLocation)
                .withTransferType(testTransferType).withFlow(randomAccountingFlow).withTransferDate(date)
                .withComment(comment).build();
        changeMap.put(date, pumpAccounting);
        return changeMap;
    }

    private void assertPumpAccountingInDB(WaterUserContract contract,
            NavigableMap<Instant, WaterSupplyPumpAccounting> pumpChangesMap, Instant pumpChangeDate, Instant startTime,
            Instant endTime, boolean startInclusive, boolean endInclusive, boolean headFlag,
            int rowLimit) throws Exception {

        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection(c -> {
            DSLContext ctx = getDslContext(c, OFFICE_ID);
            WaterSupplyAccountingDao accountingDao = new WaterSupplyAccountingDao(ctx);
            WaterSupplyAccounting pumpAccounting = accountingDao.retrieveAccounting(contract.getContractId()
                            .getName(), contract.getWaterUser(), startTime, endTime, startInclusive, endInclusive,
                    headFlag, rowLimit);
            for (CwmsId key : pumpAccounting.getPumpLocationMap().keySet()) {
                NavigableMap<Instant, WaterSupplyPumpAccounting> returnedPumpAccounting
                        = pumpAccounting.getPumpAccounting(key);
                assertNotNull(returnedPumpAccounting.get(pumpChangeDate));
                DTOMatch.assertMatch(pumpChangesMap.get(pumpChangeDate), returnedPumpAccounting.get(pumpChangeDate));
            }
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    private void assertPumpAccountingInDBEmpty(WaterUserContract contract,
            Instant pumpChangeDate, Instant startTime, Instant endTime, boolean startInclusive, boolean endInclusive,
            boolean headFlag, int rowLimit) throws Exception {
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection(c -> {
            DSLContext ctx = getDslContext(c, OFFICE_ID);
            WaterSupplyAccountingDao accountingDao = new WaterSupplyAccountingDao(ctx);
            WaterSupplyAccounting pumpAccounting = accountingDao.retrieveAccounting(contract.getContractId()
                            .getName(), contract.getWaterUser(), startTime, endTime, startInclusive, endInclusive,
                    headFlag, rowLimit);
            for (CwmsId key : pumpAccounting.getPumpLocationMap().keySet()) {
                NavigableMap<Instant, WaterSupplyPumpAccounting> returnedPumpAccounting
                        = pumpAccounting.getPumpAccounting(key);
                assertTrue(returnedPumpAccounting.isEmpty()
                        || returnedPumpAccounting.get(pumpChangeDate) == null);
            }
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    private void cleanupPump(WaterSupplyPump pump, WaterUserContract contract) throws Exception {
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection(c -> {
            DSLContext ctx = getDslContext(c, OFFICE_ID);
            WaterContractDao pumpDao = new WaterContractDao(ctx);
            pumpDao.removePumpFromContract(contract, pump.getPumpLocation().getName(),
                    pump.getPumpType().toString(), true);
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    protected void cleanupUserRoutine(WaterUser user) throws Exception {
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection(c -> {
            DSLContext ctx = getDslContext(c, OFFICE_ID);
            WaterContractDao dao = new WaterContractDao(ctx);
            dao.deleteWaterUser(user.getProjectId(), user.getEntityName(), TEST_DELETE_ACTION);
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    protected void cleanupContractRoutine(WaterUserContract contract) throws Exception {
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection(c -> {
            DSLContext ctx = getDslContext(c, OFFICE_ID);
            WaterContractDao dao = new WaterContractDao(ctx);
            dao.deleteWaterContract(contract, TEST_DELETE_ACTION);
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    protected static WaterUser buildTestWaterUser(String entityName) {
        return new WaterUser.Builder().withEntityName(entityName).withProjectId(new CwmsId.Builder()
                .withName("Test Project Name")
                .withOfficeId(OFFICE_ID)
                .build())
            .withWaterRight("Test Water Right").build();
    }

    protected WaterUserContract buildTestWaterContract(String entityName) {
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
                .withContractExpirationDate(new Date(Instant.ofEpochMilli(1979252516000L).toEpochMilli()))
                .withContractEffectiveDate(new Date(Instant.ofEpochMilli(1766652851000L).toEpochMilli()))
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
        return new Project.Builder().withLocation(buildTestLocation("Test Project Name",
                        "Test Location Type"))
                .withFederalCost(new BigDecimal("15980654.55"))
                .build();
    }
}
