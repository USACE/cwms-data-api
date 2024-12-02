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
import cwms.cda.data.dto.watersupply.PumpLocation;
import cwms.cda.data.dto.watersupply.PumpTransfer;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static cwms.cda.data.dao.JooqDao.getDslContext;
import static org.junit.jupiter.api.Assertions.*;

@Tag("integration")
class WaterSupplyAccountingDaoIT extends DataApiTestIT {
    private static final String OFFICE_ID = "SPK";
    private static final JooqDao.DeleteMethod TEST_DELETE_ACTION = JooqDao.DeleteMethod.DELETE_ALL;
    private static final String PROJECT_NAME = "Test Project Name";
    private static final String PROJECT_NAME_2 = "Test Project Name 2";
    private static final String WATER_USER_ENTITY_NAME = "Test entity";
    private static final Location testLocation = buildTestLocation(PROJECT_NAME, "Test Location");
    private static final Location testLocation2 = buildTestLocation(PROJECT_NAME_2, "Test Location 2");
    private static final Project testProject = buildTestProject(PROJECT_NAME);
    private static final Project testProject2 = buildTestProject(PROJECT_NAME_2);
    private static final WaterUser testUser = buildTestWaterUser(WATER_USER_ENTITY_NAME, PROJECT_NAME);
    private static final WaterUser testUser2 = buildTestWaterUser(WATER_USER_ENTITY_NAME, PROJECT_NAME_2);
    private static final WaterUserContract contract = buildTestWaterContract("Contract Name", testUser, true);
    private static final WaterUserContract contract2 = buildTestWaterContract("Contract Name 2", testUser2, false);
    private static final LookupType testTransferType = new LookupType.Builder()
            .withDisplayValue("Pipeline")
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
                dao.storeLocation(testLocation2);
                lookupTypeDao.storeLookupType("AT_PHYSICAL_TRANSFER_TYPE", "PHYS_TRANS_TYPE", testTransferType);
                projectDao.store(testProject, true);
                projectDao.store(testProject2, true);
                contractDao.storeWaterUser(testUser, true);
                contractDao.storeWaterUser(testUser2, true);
                contractDao.storeWaterContract(contract, true, false);
                contractDao.storeWaterContract(contract2, true, false);
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
            contractDao.deleteWaterContract(contract2, TEST_DELETE_ACTION);
            contractDao.deleteWaterUser(testUser.getProjectId(), testUser.getEntityName(), TEST_DELETE_ACTION);
            contractDao.deleteWaterUser(testUser2.getProjectId(), testUser2.getEntityName(), TEST_DELETE_ACTION);
            lookupTypeDao.deleteLookupType("AT_PHYSICAL_TRANSFER_TYPE", "PHYS_TRANS_TYPE",
                    OFFICE_ID, testTransferType.getDisplayValue());
            projectDao.delete(testProject.getLocation().getOfficeId(), testProject.getLocation().getName(),
                    DeleteRule.DELETE_ALL);
            projectDao.delete(testProject2.getLocation().getOfficeId(), testProject2.getLocation().getName(),
                    DeleteRule.DELETE_ALL);
            dao.deleteLocation(testLocation.getName(), OFFICE_ID);
            dao.deleteLocation(testLocation2.getName(), OFFICE_ID);
        });
    }

    @ParameterizedTest
    @EnumSource(TestDates.class)
    void testStoreAndRetrieveWaterSupplyPumpAccounting(TestDates testDates) throws Exception {

        // Test Structure
        // 1) Create and store a Water Supply Contract
        // 2) Create and store Water Supply Pump Accounting
        // 3) Retrieve Water Supply Pump Accounting and assert it is the same (or not in DB)

        WaterSupplyAccounting accounting = buildTestAccounting();
        Instant startTime = testDates.startTime();
        Instant endTime = testDates.endTime();
        boolean startInclusive = testDates.startInclusive();
        boolean endInclusive = testDates.endInclusive();
        boolean inDB = testDates.isInDb();
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
            assertPumpAccountingInDB(accounting, contract, startTime, endTime, startInclusive,
                    endInclusive, headFlag, rowLimit);
        } else {
            // retrieve and assert not in db
            assertPumpAccountingInDBEmpty(contract, startTime, endTime, startInclusive,
                    endInclusive, headFlag, rowLimit);
        }
    }

    @Test
    void testStoreAndRetrieveWithFewerPumps() throws Exception {

        // Test Structure
        // 1) Create and store a Water Supply Contract
        // 2) Create and store Water Supply Pump Accounting
        // 3) Retrieve Water Supply Pump Accounting and assert it is the same (or not in DB)

        WaterSupplyAccounting accounting = buildTestAccountingWithFewerPumps();
        Instant startTime = Instant.parse("2015-10-01T00:00:00Z");
        Instant endTime = Instant.parse("2035-10-03T00:00:00Z");
        boolean startInclusive = true;
        boolean endInclusive = true;
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection(c -> {
            DSLContext ctx = getDslContext(c, OFFICE_ID);
            WaterSupplyAccountingDao accountingDao = new WaterSupplyAccountingDao(ctx);
            accountingDao.storeAccounting(accounting);
        }, CwmsDataApiSetupCallback.getWebUser());

        int rowLimit = 20;
        boolean headFlag = false;

        // retrieve and assert in db
        assertPumpAccountingInDB(accounting, contract2, startTime, endTime, startInclusive,
                endInclusive, headFlag, rowLimit);
    }

    @Test
    void testStoreRetrievePaginated() throws Exception {
        // Test Structure
        // 1) Create and store a Water Supply Contract
        // 2) Create and store Water Supply Pump Accounting
        // 3) Retrieve Water Supply Pump Accounting and assert it is the same (or not in DB)
        WaterSupplyAccounting accounting = buildTestAccounting();

        Instant endTime = Instant.parse("2025-10-01T12:00:00Z");
        boolean startInclusive = true;
        boolean endInclusive = true;
        Instant startTime = Instant.parse("2025-10-01T00:00:00Z");

        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection(c -> {
            DSLContext ctx = getDslContext(c, OFFICE_ID);
            WaterSupplyAccountingDao accountingDao = new WaterSupplyAccountingDao(ctx);
            accountingDao.storeAccounting(accounting);
        }, CwmsDataApiSetupCallback.getWebUser());

        int rowLimit = 2;
        boolean headFlag = false;

        Map<Instant, List<PumpTransfer>> firstPageMap = new TreeMap<>();
        List<PumpTransfer> transfers = accounting.getPumpAccounting().get(Instant.parse("2025-10-01T00:00:00Z"));
        firstPageMap.put(Instant.parse("2025-10-01T00:00:00Z"), transfers);

        WaterSupplyAccounting accounting2 = new WaterSupplyAccounting.Builder()
                .withWaterUser(accounting.getWaterUser())
                .withContractName(accounting.getContractName())
                .withWaterUser(accounting.getWaterUser())
                .withPumpLocations(accounting.getPumpLocations())
                .withPumpAccounting(firstPageMap)
                .build();

        // retrieve and assert in db
        // page 1
        final Instant finalStartTime = startTime;
        final Instant finalEndTime = endTime;
        final boolean finalStartInclusive = startInclusive;
        db.connection(c -> {
            DSLContext ctx = getDslContext(c, OFFICE_ID);
            WaterSupplyAccountingDao accountingDao = new WaterSupplyAccountingDao(ctx);
            List<WaterSupplyAccounting> pumpAccounting = accountingDao.retrieveAccounting(contract.getContractId()
                            .getName(), contract.getWaterUser(), contract.getWaterUser().getProjectId(),
                    null, finalStartTime, finalEndTime, finalStartInclusive, endInclusive, headFlag, rowLimit);
            assertFalse(pumpAccounting.isEmpty());
            for (WaterSupplyAccounting returnedAccounting : pumpAccounting) {
                assertNotNull(returnedAccounting.getPumpAccounting());
                DTOMatch.assertMatch(accounting2, returnedAccounting);
            }
        }, CwmsDataApiSetupCallback.getWebUser());

        Map<Instant, List<PumpTransfer>> secondPageMap = new TreeMap<>();
        List<PumpTransfer> transfers2 = accounting.getPumpAccounting().get(Instant.parse("2025-10-02T00:00:00Z"));
        secondPageMap.put(Instant.parse("2025-10-02T00:00:00Z"), transfers2);

        WaterSupplyAccounting accounting3 = new WaterSupplyAccounting.Builder()
                .withWaterUser(accounting.getWaterUser())
                .withContractName(accounting.getContractName())
                .withWaterUser(accounting.getWaterUser())
                .withPumpLocations(accounting.getPumpLocations())
                .withPumpAccounting(secondPageMap)
                .build();

        startTime = Instant.parse("2025-10-01T12:00:00Z");
        endTime = Instant.parse("2025-10-03T00:00:00Z");
        startInclusive = false;
        final Instant finalStartTime1 = startTime;
        final Instant finalEndTime1 = endTime;
        final boolean finalStartInclusive1 = startInclusive;
        db.connection(c -> {
            DSLContext ctx = getDslContext(c, OFFICE_ID);
            WaterSupplyAccountingDao accountingDao = new WaterSupplyAccountingDao(ctx);
            List<WaterSupplyAccounting> pumpAccounting = accountingDao.retrieveAccounting(contract.getContractId()
                            .getName(), contract.getWaterUser(), contract.getWaterUser().getProjectId(),
                    null, finalStartTime1, finalEndTime1, finalStartInclusive1, endInclusive, headFlag, rowLimit);
            assertFalse(pumpAccounting.isEmpty());
            for (WaterSupplyAccounting returnedAccounting : pumpAccounting) {
                assertNotNull(returnedAccounting.getPumpAccounting());
                DTOMatch.assertMatch(accounting3, returnedAccounting);
            }
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    @Test
    void testStoreRetrieve() throws Exception {
        WaterSupplyAccounting accounting = buildTestAccounting();
        final Instant startTime = Instant.parse("2015-10-01T00:00:00Z");
        final Instant endTime = Instant.parse("2035-10-03T00:00:00Z");
        final boolean startInclusive = false;
        final boolean endInclusive = false;
        final boolean headFlag = false;
        final int rowLimit = 20;
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection(c -> {
            DSLContext ctx = getDslContext(c, OFFICE_ID);
            WaterSupplyAccountingDao accountingDao = new WaterSupplyAccountingDao(ctx);
            accountingDao.storeAccounting(accounting);
            List<WaterSupplyAccounting> pumpAccounting = accountingDao.retrieveAccounting(contract.getContractId()
                            .getName(), contract.getWaterUser(), contract.getWaterUser().getProjectId(),
                    null, startTime, endTime, startInclusive, endInclusive, headFlag, rowLimit);
            assertFalse(pumpAccounting.isEmpty());
            for (WaterSupplyAccounting returnedAccounting : pumpAccounting) {
                assertNotNull(returnedAccounting.getPumpAccounting());
                DTOMatch.assertMatch(accounting, returnedAccounting);
            }
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    private void assertPumpAccountingInDB(WaterSupplyAccounting expected, WaterUserContract waterContract,
            Instant startTime, Instant endTime, boolean startInclusive, boolean endInclusive, boolean headFlag,
            int rowLimit) throws Exception {

        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection(c -> {
            DSLContext ctx = getDslContext(c, OFFICE_ID);
            WaterSupplyAccountingDao accountingDao = new WaterSupplyAccountingDao(ctx);
            List<WaterSupplyAccounting> pumpAccounting = accountingDao.retrieveAccounting(waterContract.getContractId()
                            .getName(), waterContract.getWaterUser(), waterContract.getWaterUser().getProjectId(),
                    null, startTime, endTime, startInclusive, endInclusive, headFlag, rowLimit);
            assertFalse(pumpAccounting.isEmpty());
            for (WaterSupplyAccounting returnedAccounting : pumpAccounting) {
                assertNotNull(returnedAccounting.getPumpAccounting());
                DTOMatch.assertMatch(expected, returnedAccounting);
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

    protected static WaterUser buildTestWaterUser(String entityName, String projectName) {
        return new WaterUser.Builder()
            .withEntityName(entityName)
            .withProjectId(new CwmsId.Builder()
                .withName(projectName)
                .withOfficeId(OFFICE_ID)
                .build())
            .withWaterRight("Test Water Right").build();
    }

    protected static WaterUserContract buildTestWaterContract(String contractName, WaterUser user, boolean allPumps) {
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
                        .withName(contractName)
                        .withOfficeId(OFFICE_ID)
                        .build())
                .withFutureUsePercentActivated(35.7)
                .withWaterUser(user)
                .withPumpInLocation(new WaterSupplyPump.Builder()
                        .withPumpLocation(buildTestLocation(contractName + "-Pump 1",
                        "PUMP")).withPumpType(PumpType.IN).build())
                .withPumpOutLocation(allPumps ? new WaterSupplyPump.Builder()
                        .withPumpLocation(buildTestLocation(contractName + "-Pump 2",
                        "PUMP")).withPumpType(PumpType.OUT).build() : null)
                .withPumpOutBelowLocation(allPumps ? new WaterSupplyPump.Builder()
                        .withPumpLocation(buildTestLocation(contractName + "-Pump 3",
                        "PUMP")).withPumpType(PumpType.BELOW).build() : null)
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

    protected static Project buildTestProject(String projectName) {
        return new Project.Builder().withLocation(buildTestLocation(projectName,
                        "PROJECT"))
                .withFederalCost(new BigDecimal("15980654.55"))
                .build();
    }

    private WaterSupplyAccounting buildTestAccounting() {
        return new WaterSupplyAccounting.Builder().withWaterUser(testUser)
                .withContractName(contract.getContractId().getName())
                .withPumpAccounting(buildTestPumpAccountingList())
                .withPumpLocations(new PumpLocation.Builder()
                        .withPumpIn(CwmsId.buildCwmsId(OFFICE_ID, contract.getPumpInLocation().getPumpLocation().getName()))
                        .withPumpOut(CwmsId.buildCwmsId(OFFICE_ID, contract.getPumpOutLocation().getPumpLocation().getName()))
                        .withPumpBelow(CwmsId.buildCwmsId(OFFICE_ID, contract.getPumpOutBelowLocation().getPumpLocation().getName()))
                        .build())
                .build();
    }

    private WaterSupplyAccounting buildTestAccountingWithFewerPumps() {
        return new WaterSupplyAccounting.Builder().withWaterUser(testUser2)
                .withContractName(contract2.getContractId().getName())
                .withPumpAccounting(buildTestPumpAccountingListWithFewerPumps())
                .withPumpLocations(new PumpLocation.Builder()
                        .withPumpIn(CwmsId.buildCwmsId(OFFICE_ID, contract2.getPumpInLocation().getPumpLocation().getName()))
                        .build())
                .build();
    }

    private Map<Instant, List<PumpTransfer>> buildTestPumpAccountingList() {
        Map<Instant, List<PumpTransfer>> retList = new TreeMap<>();
        List<PumpTransfer> transfers = new ArrayList<>();
        transfers.add(new PumpTransfer(PumpType.IN, "Conduit", 100.0, "Test Transfer"));
        transfers.add(new PumpTransfer(PumpType.OUT, "Pipeline", 200.0, "Emergency Transfer"));
        retList.put(Instant.parse("2025-10-01T00:00:00Z"), transfers);
        transfers.clear();
        transfers.add(new PumpTransfer(PumpType.OUT, "Canal", 300.0, "Test Transfer"));
        transfers.add(new PumpTransfer(PumpType.BELOW, "Stream", 400.0, "Emergency Transfer"));
        retList.put(Instant.parse("2025-10-02T00:00:00Z"), transfers);
        return retList;
    }

    private Map<Instant, List<PumpTransfer>> buildTestPumpAccountingListWithFewerPumps() {
        Map<Instant, List<PumpTransfer>> retList = new TreeMap<>();
        retList.put(Instant.parse("2025-10-01T00:00:00Z"),
                Collections.singletonList(new PumpTransfer(PumpType.IN, "Conduit", 560.0, "Test Transfer")));
        retList.put(Instant.parse("2025-10-02T00:00:00Z"),
                Collections.singletonList(new PumpTransfer(PumpType.IN, "Canal", 750.0, "Test Transfer")));
        return retList;
    }

    private enum TestDates
    {
        DEFAULT
        {
            @Override
            public Instant startTime() {
                this.startTime = Instant.parse("2025-08-30T00:00:00Z");
                return this.startTime;
            }

            @Override
            public Instant endTime() {
                this.endTime = Instant.parse("2326-02-01T00:00:00Z");
                return this.endTime;
            }

            @Override
            public boolean startInclusive() {
                this.startInclusive = true;
                return this.startInclusive;
            }

            @Override
            public boolean endInclusive() {
                this.endInclusive = true;
                return this.endInclusive;
            }

            @Override
            public boolean isInDb() {
                this.inDb = true;
                return this.inDb;
            }
        },
        START_INCLUSIVE
        {
            @Override
            public Instant startTime() {
                this.startTime = Instant.parse("2025-08-30T00:00:00Z");
                return this.startTime;
            }

            @Override
            public Instant endTime() {
                this.endTime = Instant.parse("2030-02-01T00:00:00Z");
                return this.endTime;
            }

            @Override
            public boolean startInclusive() {
                this.startInclusive = true;
                return this.startInclusive;
            }

            @Override
            public boolean endInclusive() {
                this.endInclusive = true;
                return this.endInclusive;
            }

            @Override
            public boolean isInDb() {
                this.inDb = true;
                return this.inDb;
            }
        },
        END_INCLUSIVE
        {
            @Override
            public Instant startTime() {
                this.startTime = Instant.parse("2025-08-30T00:00:00Z");
                return this.startTime;
            }

            @Override
            public Instant endTime() {
                this.endTime = Instant.parse("2326-02-01T00:00:00Z");
                return this.endTime;
            }

            @Override
            public boolean startInclusive() {
                this.startInclusive = true;
                return this.startInclusive;
            }

            @Override
            public boolean endInclusive() {
                this.endInclusive = true;
                return this.endInclusive;
            }

            @Override
            public boolean isInDb() {
                this.inDb = true;
                return this.inDb;
            }
        },
        START_INCLUSIVE_FALSE
        {
            @Override
            public Instant startTime() {
                this.startTime = Instant.parse("2286-10-21T08:53:19Z");
                return this.startTime;
            }

            @Override
            public Instant endTime() {
                this.endTime = Instant.parse("2326-02-01T00:00:00Z");
                return this.endTime;
            }

            @Override
            public boolean startInclusive() {
                this.startInclusive = false;
                return this.startInclusive;
            }

            @Override
            public boolean endInclusive() {
                this.endInclusive = true;
                return this.endInclusive;
            }

            @Override
            public boolean isInDb() {
                this.inDb = false;
                return this.inDb;
            }
        },
        END_INCLUSIVE_FALSE
        {
            @Override
            public Instant startTime() {
                this.startTime = Instant.parse("2025-08-30T00:00:00Z");
                return this.startTime;
            }

            @Override
            public Instant endTime() {
                this.endTime = Instant.parse("2025-10-01T00:00:00Z");
                return this.endTime;
            }

            @Override
            public boolean startInclusive() {
                this.startInclusive = true;
                return this.startInclusive;
            }

            @Override
            public boolean endInclusive() {
                this.endInclusive = false;
                return this.endInclusive;
            }

            @Override
            public boolean isInDb() {
                this.inDb = false;
                return this.inDb;
            }
        },
        START_END_INCLUSIVE_FALSE
        {
            @Override
            public Instant startTime() {
                this.startTime = Instant.parse("2325-10-01T00:00:00Z");
                return this.startTime;
            }

            @Override
            public Instant endTime() {
                this.endTime = Instant.parse("2326-02-01T00:00:00Z");
                return this.endTime;
            }

            @Override
            public boolean startInclusive() {
                this.startInclusive = false;
                return this.startInclusive;
            }

            @Override
            public boolean endInclusive() {
                this.endInclusive = false;
                return this.endInclusive;
            }

            @Override
            public boolean isInDb() {
                this.inDb = false;
                return this.inDb;
            }
        };

        boolean inDb = true;
        boolean startInclusive = true;
        boolean endInclusive = true;
        Instant startTime = Instant.now();
        Instant endTime = Instant.now();

        public boolean isInDb() {
            return this.inDb;
        }

        public Instant startTime() {
            return this.startTime;
        }

        public Instant endTime() {
            return this.endTime;
        }

        public boolean startInclusive() {
            return this.startInclusive;
        }

        public boolean endInclusive() {
            return this.endInclusive;
        }

    }
}
