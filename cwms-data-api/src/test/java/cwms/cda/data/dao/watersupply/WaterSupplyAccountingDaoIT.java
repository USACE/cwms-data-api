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
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.ZoneId;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.NavigableMap;
import java.util.TimeZone;
import java.util.TreeMap;

import static cwms.cda.data.dao.JooqDao.getDslContext;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class WaterSupplyAccountingDaoIT extends DataApiTestIT {
    private static final String OFFICE_ID = "SPK";
    private static final String TEST_DELETE_ACTION = "DELETE ALL";
    private static final Location testLocation = buildTestLocation("Test Location Name", "Test Location");
    private static final Project testProject = buildTestProject();
    private static final WaterUser testUser = buildTestWaterUser("Test User");
    private static final Location testPumpInLocation = buildTestLocation("Test Pump IN", "PUMP");
    private static final Location testPumpOutLocation = buildTestLocation("Test Pump OUT", "PUMP");
    private static final Location testPumpBelowLocation = buildTestLocation("Test Pump BELOW", "PUMP");

    @BeforeAll
    static void setup() throws Exception {
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection(c -> {
            DSLContext ctx = getDslContext(c, OFFICE_ID);
            LocationsDaoImpl dao = new LocationsDaoImpl(ctx);
            ProjectDao projectDao = new ProjectDao(ctx);
            try {
                dao.storeLocation(testLocation);
                projectDao.store(testProject, true);
                dao.storeLocation(testPumpInLocation);
                dao.storeLocation(testPumpOutLocation);
                dao.storeLocation(testPumpBelowLocation);
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
            projectDao.delete(testProject.getLocation().getOfficeId(), testProject.getLocation().getName(),
                    DeleteRule.DELETE_ALL);
            dao.deleteLocation(testLocation.getName(), OFFICE_ID);
            dao.deleteLocation(testPumpInLocation.getName(), OFFICE_ID);
            dao.deleteLocation(testPumpOutLocation.getName(), OFFICE_ID);
            dao.deleteLocation(testPumpBelowLocation.getName(), OFFICE_ID);
        });
    }

    @Test
    void testStoreWaterSupplyPumpAccounting() throws Exception {

        // Test Structure
        // 1) Create and store a Water Supply Contract
        // 2) Create and store Water Supply Pump Accounting
        // 3) Retrieve Water Supply Pump Accounting and assert it is the same

        WaterUserContract contract = buildTestWaterContract("Test entity", false);

        WaterSupplyAccounting accounting = new WaterSupplyAccounting(contract.getContractId().getName(), testUser,
                new HashMap<>(), new HashMap<>());

        WaterSupplyPump pumpIn = buildTestWaterSupplyPump(contract.getPumpInLocation().getPumpLocation().getName(), PumpType.IN);

        Calendar instance = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        instance.set(Calendar.MILLISECOND, 0);

        boolean generateModifiedTimeWindow = true;
        boolean preserveModifiedData = true;
        instance.set(2025, Calendar.NOVEMBER, 1, 0, 0);
        Date pumpChangeDate = instance.getTime();
        NavigableMap<Date, WaterSupplyPumpAccounting> pumpChangesMap = createPumpChangesMap(new CwmsId.Builder()
                .withOfficeId(OFFICE_ID).withName(pumpIn.getPumpLocation().getName()).build(),
                contract.getWaterUser(), contract, pumpChangeDate);
        accounting.mergeAccounting(new CwmsId.Builder().withName(pumpIn.getPumpLocation().getName())
                        .withOfficeId(OFFICE_ID).build(),
                pumpChangesMap, generateModifiedTimeWindow, preserveModifiedData);

        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection(c -> {
            DSLContext ctx = getDslContext(c, OFFICE_ID);
            WaterContractDao contractDao = new WaterContractDao(ctx);
            WaterSupplyAccountingDao accountingDao = new WaterSupplyAccountingDao(ctx);
            contractDao.storeWaterUser(testUser, true);
            contractDao.storeWaterContract(contract, true, false);
            accountingDao.storeAccounting(accounting);
        }, CwmsDataApiSetupCallback.getWebUser());

        instance.set(2000, Calendar.MARCH, 1);
        Date startTime = instance.getTime();
        instance.set(2026, Calendar.MARCH, 1);
        Date endTime = instance.getTime();
        int rowLimit = 20;
        boolean headFlag = false;
        boolean startInclusive = true;
        boolean endInclusive = true;

        // retrieve and assert in db
        assertPumpAccountingInDB(contract, pumpChangesMap, pumpChangeDate, pumpIn, startTime, endTime, startInclusive,
                endInclusive, headFlag, rowLimit);

        // cleanup
        cleanupPump(pumpIn, contract);
        cleanupContractRoutine(contract);
    }

    @Test
    void testRetrieveWaterSupplyPumpAccounting() {
        // test
    }

    @Test
    void testRetrieveWaterSupplyPumpAccounting_startInclusiveTrue() {
        // test
    }

    @Test
    void testRetrieveWaterSupplyPumpAccounting_startInclusiveFalse() {
        // test
    }

    @Test
    void testRetrieveWaterSupplyPumpAccounting_endInclusiveTrue() {
        // test
    }

    @Test
    void testRetrieveWaterSupplyPumpAccounting_endInclusiveFalse() {
        // test
    }

    private WaterSupplyPump buildTestWaterSupplyPump(String locationName, PumpType pumpType) {
        return new WaterSupplyPump(buildTestLocation(locationName, "PUMP"), pumpType);
    }

    private NavigableMap<Date, WaterSupplyPumpAccounting> createPumpChangesMap(CwmsId pumpLocation, WaterUser user,
            WaterUserContract contract, Date date) {
        NavigableMap<Date, WaterSupplyPumpAccounting> changeMap = new TreeMap<>();
        double randomAccountingFlow = 100 + (int) (Math.random() * ((1000 - 100) + 1));
        String comment = "Test Comment";
        LookupType transferType = new LookupType.Builder().withDisplayValue("Transfer").withOfficeId(OFFICE_ID)
                .withTooltip("Tooltip test").withActive(true).build();
        WaterSupplyPumpAccounting pumpAccounting = new WaterSupplyPumpAccounting(user, contract.getContractId().getName(),
                pumpLocation, transferType, randomAccountingFlow, date, comment);
        changeMap.put(date, pumpAccounting);
        return changeMap;
    }

    private void assertPumpAccountingInDB(WaterUserContract contract,
            NavigableMap<Date, WaterSupplyPumpAccounting> pumpChangesMap, Date pumpChangeDate, WaterSupplyPump pumpIn, Date startTime,
            Date endTime, boolean startInclusive, boolean endInclusive, boolean headFlag, int rowLimit) throws Exception {

        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection(c -> {
            DSLContext ctx = getDslContext(c, OFFICE_ID);
            WaterSupplyAccountingDao accountingDao = new WaterSupplyAccountingDao(ctx);
            WaterSupplyAccounting pumpAccounting = accountingDao.retrieveAccounting(contract.getContractId()
                            .getName(), contract.getWaterUser(), startTime, endTime, startInclusive, endInclusive,
                    headFlag, rowLimit);
            NavigableMap<Date, WaterSupplyPumpAccounting> returnedPumpAccounting
                    = pumpAccounting.getPumpAccounting(new CwmsId.Builder().withName(pumpIn.getPumpLocation().getName())
                    .withOfficeId(OFFICE_ID).build());
            assertNotNull(returnedPumpAccounting.get(pumpChangeDate));
            DTOMatch.assertMatch(pumpChangesMap.get(pumpChangeDate), returnedPumpAccounting.get(pumpChangeDate));
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
            dao.deleteWaterUser(user.getProjectLocationRef(), user.getEntityName(), TEST_DELETE_ACTION);
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
        return new WaterUser(entityName, new CwmsId.Builder()
                .withName("Test Location Name")
                .withOfficeId(OFFICE_ID)
                .build(),
                "Test Water Right");
    }

    protected WaterUserContract buildTestWaterContract(String entityName, boolean renameTest) {
        if (!renameTest){
            return new WaterUserContract.Builder()
                    .withContractType(new LookupType.Builder()
                            .withTooltip("Test Tooltip")
                            .withActive(true)
                            .withDisplayValue("Test Display Value")
                            .withOfficeId(OFFICE_ID).build())
                    .withStorageUnitsId("m3")
                    .withOfficeId(OFFICE_ID)
                    .withFutureUseAllocation(158900.6)
                    .withContractedStorage(1589005.6)
                    .withInitialUseAllocation(1500.2)
                    .withContractExpirationDate(new Date(1979252516))
                    .withContractEffectiveDate(new Date(1766652851))
                    .withTotalAllocPercentActivated(55.1)
                    .withContractId(new CwmsId.Builder()
                            .withName(entityName)
                            .withOfficeId(OFFICE_ID)
                            .build())
                    .withFutureUsePercentActivated(35.7)
                    .withWaterUser(testUser)
                    .withPumpInLocation(new WaterSupplyPump(buildTestLocation("Pump 1 " + entityName,
                            "PUMP"), PumpType.IN))
                    .withPumpOutLocation(new WaterSupplyPump(buildTestLocation("Pump 2 " + entityName,
                            "PUMP"), PumpType.OUT))
                    .withPumpOutBelowLocation(new WaterSupplyPump(buildTestLocation("Pump 3 " + entityName,
                            "PUMP"), PumpType.BELOW))
                    .build();
        } else {
            return new WaterUserContract.Builder()
                    .withContractType(new LookupType.Builder()
                            .withTooltip("Test Tooltip")
                            .withActive(true)
                            .withDisplayValue("Test Display Value")
                            .withOfficeId(OFFICE_ID).build())
                    .withStorageUnitsId("m3")
                    .withOfficeId(OFFICE_ID)
                    .withFutureUseAllocation(158900.6)
                    .withContractedStorage(1589005.6)
                    .withInitialUseAllocation(1500.2)
                    .withContractExpirationDate(new Date(1979252516))
                    .withContractEffectiveDate(new Date(1766652851))
                    .withTotalAllocPercentActivated(55.1)
                    .withContractId(new CwmsId.Builder()
                            .withName(entityName)
                            .withOfficeId(OFFICE_ID)
                            .build())
                    .withFutureUsePercentActivated(35.7)
                    .withWaterUser(buildTestWaterUser("Water User Name"))
                    .withPumpInLocation(new WaterSupplyPump(buildTestLocation("Pump 1",
                            "PUMP"), PumpType.IN))
                    .withPumpOutLocation(new WaterSupplyPump(buildTestLocation("Pump 2",
                            "PUMP"), PumpType.OUT))
                    .withPumpOutBelowLocation(new WaterSupplyPump(buildTestLocation("Pump 3",
                            "PUMP"), PumpType.BELOW))
                    .build();
        }

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
                .withVerticalDatum("LOCAL")
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
        return new Project.Builder().withLocation(buildTestLocation("Test Location Name",
                        "Test Location Type"))
                .withFederalCost(new BigDecimal("15980654.55"))
                .build();
    }
}
