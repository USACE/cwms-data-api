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

import static cwms.cda.data.dao.JooqDao.getDslContext;
import static org.junit.jupiter.api.Assertions.*;

import cwms.cda.api.DataApiTestIT;
import cwms.cda.api.enums.Nation;
import cwms.cda.api.errors.NotFoundException;
import cwms.cda.data.dao.DeleteRule;
import cwms.cda.data.dao.LocationsDaoImpl;
import cwms.cda.data.dao.LookupTypeDao;
import cwms.cda.data.dao.project.ProjectDao;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.Location;
import cwms.cda.data.dto.LookupType;
import cwms.cda.data.dto.project.Project;
import cwms.cda.data.dto.watersupply.PumpType;
import cwms.cda.data.dto.watersupply.WaterSupplyPump;
import cwms.cda.data.dto.watersupply.WaterUser;
import cwms.cda.data.dto.watersupply.WaterUserContract;
import cwms.cda.helpers.DTOMatch;
import fixtures.CwmsDataApiSetupCallback;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import mil.army.usace.hec.test.database.CwmsDatabaseContainer;
import org.jooq.DSLContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;


class WaterContractDaoTestIT extends DataApiTestIT {
    private static final String OFFICE_ID = "SPK";
    private static final String DELETE_ACTION = "DELETE ALL";
    private static final Location testLocation = buildTestLocation("Test Location Name", "Test Location");
    private static final Project testProject = buildTestProject();
    private static final WaterUser testUser = buildTestWaterUser("Test User");
    private final List<WaterUser> waterUserList = new ArrayList<>();
    private final List<WaterUserContract> waterUserContractList = new ArrayList<>();

    @BeforeAll
    static void setup() throws Exception {
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection(c -> {
            DSLContext ctx = getDslContext(c, OFFICE_ID);
            LocationsDaoImpl dao = new LocationsDaoImpl(ctx);
            LookupTypeDao lookupTypeDao = new LookupTypeDao(ctx);
            ProjectDao projectDao = new ProjectDao(ctx);
            try {
                projectDao.create(testProject);
                dao.storeLocation(testLocation);
                lookupTypeDao.storeLookupType("AT_WS_CONTRACT_TYPE", "WS_CONTRACT_TYPE",
                        buildTestWaterContract("Test", false).getContractType());
            } catch (IOException e) {
                throw new RuntimeException("Failed to store location or project", e);
            }
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    @AfterAll
    static void tearDown() throws Exception {
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection(c -> {
            DSLContext ctx = getDslContext(c, OFFICE_ID);
            ProjectDao projectDao = new ProjectDao(ctx);
            LookupTypeDao lookupTypeDao = new LookupTypeDao(ctx);
            LocationsDaoImpl locationDao = new LocationsDaoImpl(ctx);
            projectDao.delete(OFFICE_ID, testProject.getLocation().getName(), DeleteRule.DELETE_ALL);
            locationDao.deleteLocation(testLocation.getName(), testLocation.getOfficeId(), false);
            lookupTypeDao.deleteLookupType("AT_WS_CONTRACT_TYPE", "WS_CONTRACT_TYPE", OFFICE_ID,
                    buildTestWaterContract("Test", false).getContractType().getDisplayValue());
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    @AfterEach
    void cleanup() throws SQLException {
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection(c -> {
            DSLContext ctx = getDslContext(c, OFFICE_ID);
            WaterContractDao dao = new WaterContractDao(ctx);
            for (WaterUserContract waterUserContract : waterUserContractList) {
                dao.deleteWaterContract(waterUserContract, DELETE_ACTION);
            }
            waterUserContractList.clear();
            for (WaterUser waterUser : waterUserList) {
                dao.deleteWaterUser(waterUser.getProjectId(), waterUser.getEntityName(), DELETE_ACTION);
            }
            waterUserList.clear();
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    @Test
    void testStoreAndRetrieveWaterUserList() throws Exception {
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection(c -> {
            DSLContext ctx = getDslContext(c, OFFICE_ID);
            WaterContractDao dao = new WaterContractDao(ctx);
            WaterUser user1 = buildTestWaterUser("TEST USER 1");
            WaterUser user2 = buildTestWaterUser("TEST USER 2");
            dao.storeWaterUser(user1, false);
            dao.storeWaterUser(user2, false);
            waterUserList.add(user1);
            waterUserList.add(user2);
            List<WaterUser> results = dao.getAllWaterUsers(new CwmsId.Builder().withOfficeId(OFFICE_ID)
                    .withName(testLocation.getName()).build());
            DTOMatch.assertMatch(buildTestWaterUser("TEST USER 1"), results.get(0));
            DTOMatch.assertMatch(buildTestWaterUser("TEST USER 2"), results.get(1));
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    @Test
    void testStoreAndRetrieveWaterUser() throws Exception {
        CwmsId projectLocation = new CwmsId.Builder()
                .withName("Test Location Name")
                .withOfficeId(OFFICE_ID)
                .build();
        String entityName = "Test Entity";
        WaterUser newUser = buildTestWaterUser(entityName);
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection(c -> {
            DSLContext ctx = getDslContext(c, OFFICE_ID);
            WaterContractDao dao = new WaterContractDao(ctx);
            dao.storeWaterUser(newUser, false);
            waterUserList.add(newUser);
            WaterUser retrievedUser = dao.getWaterUser(projectLocation, entityName);
            DTOMatch.assertMatch(newUser, retrievedUser);
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    @Test
    void testStoreAndRetrieveWaterContractList() throws Exception {
        WaterUserContract contract = buildTestWaterContract("Test retrieve", false);
        WaterUserContract contract2 = buildTestWaterContract("Test retrieve 2", false);
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection(c -> {
            DSLContext ctx = getDslContext(c, OFFICE_ID);
            WaterContractDao dao = new WaterContractDao(ctx);
            dao.storeWaterUser(contract.getWaterUser(), false);
            dao.storeWaterContract(contract, false, false);
            dao.storeWaterContract(contract2, false, false);
            waterUserContractList.add(contract);
            waterUserContractList.add(contract2);
            waterUserList.add(contract.getWaterUser());
            waterUserList.add(contract2.getWaterUser());
            List<WaterUserContract> retrievedContract = dao.getAllWaterContracts(contract.getWaterUser().getProjectId(),
                    contract.getWaterUser().getEntityName());
            WaterUserContract result = retrievedContract.get(0);
            DTOMatch.assertMatch(contract, result);
            result = retrievedContract.get(1);
            DTOMatch.assertMatch(contract2, result);
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    @Test
    void testStoreAndRetrieveWaterContract() throws Exception {
        WaterUserContract contract = buildTestWaterContract("Test retrieve 1", false);
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection(c -> {
            DSLContext ctx = getDslContext(c, OFFICE_ID);
            WaterContractDao dao = new WaterContractDao(ctx);
            dao.storeWaterUser(contract.getWaterUser(), false);
            waterUserList.add(contract.getWaterUser());
            dao.storeWaterContract(contract, false, false);
            waterUserContractList.add(contract);
            WaterUserContract retrievedContract = dao.getWaterContract(contract.getContractId().getName(),
                    contract.getWaterUser().getProjectId(), contract.getWaterUser().getEntityName());
            DTOMatch.assertMatch(contract, retrievedContract);
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    @Test
    void testStoreAndRetrieveWaterContractTypeList() throws Exception {
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection(c -> {
            DSLContext ctx = getDslContext(c, OFFICE_ID);
            WaterContractDao dao = new WaterContractDao(ctx);
            List<LookupType> contractType = new ArrayList<>();
            contractType.add(new LookupType.Builder()
                    .withTooltip("Test Tooltip")
                    .withActive(true)
                    .withDisplayValue("Test Display Value")
                    .withOfficeId(OFFICE_ID).build());
            contractType.add(new LookupType.Builder()
                    .withTooltip("Test Tooltip 2")
                    .withActive(true)
                    .withDisplayValue("Test Display Value 2")
                    .withOfficeId(OFFICE_ID).build());
            dao.storeWaterContractTypes(contractType, false);
            List<LookupType> results = dao.getAllWaterContractTypes(OFFICE_ID);
            DTOMatch.assertMatch(contractType.get(0), results.get(0));
            DTOMatch.assertMatch(contractType.get(1), results.get(1));
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    @Test
    void testRenameWaterUser() throws Exception {
        WaterUser newUser = buildTestWaterUser("TEST USER");
        WaterUser updatedUser = buildTestWaterUser("TEST USER 2");
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection(c -> {
            DSLContext ctx = getDslContext(c, OFFICE_ID);
            WaterContractDao dao = new WaterContractDao(ctx);
            dao.storeWaterUser(newUser, false);
            waterUserList.add(newUser);
            waterUserList.add(updatedUser);
            dao.renameWaterUser(newUser.getEntityName(), updatedUser.getEntityName(), newUser.getProjectId());
            waterUserList.remove(newUser);
            WaterUser retrievedUser = dao.getWaterUser(newUser.getProjectId(), updatedUser.getEntityName());
            String entityName = newUser.getEntityName();
            CwmsId projectId = newUser.getProjectId();
            assertThrows(NotFoundException.class, () -> dao.getWaterUser(projectId, entityName));
            DTOMatch.assertMatch(updatedUser, retrievedUser);
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    @Test
    void testRenameWaterContract() throws Exception {
        WaterUserContract oldContract = buildTestWaterContract("Test contract", true);
        WaterUserContract renamedContract = buildTestWaterContract("Test contract 2", true);
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection(c -> {
            DSLContext ctx = getDslContext(c, OFFICE_ID);
            WaterContractDao dao = new WaterContractDao(ctx);
            dao.storeWaterUser(oldContract.getWaterUser(), false);
            dao.storeWaterContract(oldContract, false, false);
            WaterUser user = new WaterUser.Builder().withEntityName(oldContract.getWaterUser().getEntityName())
                    .withProjectId(oldContract.getWaterUser().getProjectId())
                    .withWaterRight(oldContract.getContractId().getName()).build();
            waterUserList.add(user);
            waterUserContractList.add(oldContract);
            waterUserContractList.add(renamedContract);
            dao.renameWaterContract(user, oldContract.getContractId().getName(),
                    renamedContract.getContractId().getName());
            waterUserContractList.remove(oldContract);
            WaterUserContract retrievedContract = dao.getWaterContract(renamedContract.getContractId().getName(),
                    renamedContract.getWaterUser().getProjectId(), renamedContract.getWaterUser().getEntityName());
            assertNotNull(retrievedContract);
            String contractName = oldContract.getContractId().getName();
            CwmsId projectId = oldContract.getWaterUser().getProjectId();
            String entityName = oldContract.getWaterUser().getEntityName();
            assertThrows(NotFoundException.class, () -> dao.getWaterContract(contractName,
                    projectId, entityName));
            DTOMatch.assertMatch(retrievedContract, renamedContract);
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    @Test
    void testDeleteWaterUser() throws Exception {
        WaterUser newUser = buildTestWaterUser("TEST USER");
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection(c -> {
            DSLContext ctx = getDslContext(c, OFFICE_ID);
            WaterContractDao dao = new WaterContractDao(ctx);
            dao.storeWaterUser(newUser, false);
            dao.deleteWaterUser(newUser.getProjectId(), newUser.getEntityName(), DELETE_ACTION);
            CwmsId projectId = newUser.getProjectId();
            String entityName = newUser.getEntityName();
            assertThrows(NotFoundException.class, () -> dao.getWaterUser(projectId, entityName));
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    @Test
    void testDeleteWaterContract() throws Exception {
        WaterUserContract contract = buildTestWaterContract("Test contract", false);
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection(c -> {
            DSLContext ctx = getDslContext(c, OFFICE_ID);
            WaterContractDao dao = new WaterContractDao(ctx);
            dao.storeWaterUser(contract.getWaterUser(), false);
            dao.storeWaterContract(contract, false, false);
            dao.deleteWaterContract(contract, DELETE_ACTION);
            waterUserList.add(contract.getWaterUser());
            String contractName = contract.getContractId().getName();
            CwmsId projectId = contract.getWaterUser().getProjectId();
            String entityName = contract.getWaterUser().getEntityName();
            assertThrows(NotFoundException.class, () -> dao.getWaterContract(contractName,
                    projectId, entityName));
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    @Test
    void testRemovePumpFromContract() throws Exception {
        WaterUserContract contract = buildTestWaterContract("Test contract", false);
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection(c -> {
            DSLContext ctx = getDslContext(c, OFFICE_ID);
            WaterContractDao dao = new WaterContractDao(ctx);
            assertNotNull(contract);
            dao.storeWaterUser(contract.getWaterUser(), false);
            dao.storeWaterContract(contract, false, false);
            waterUserContractList.add(contract);
            waterUserList.add(contract.getWaterUser());
            dao.removePumpFromContract(contract, contract.getPumpInLocation().getPumpLocation().getName(),
                    PumpType.IN, false);
            WaterUserContract retrievedContract = dao.getWaterContract(contract.getContractId().getName(),
                    contract.getWaterUser().getProjectId(), contract.getWaterUser().getEntityName());
            assertNotNull(retrievedContract);
            assertNull(retrievedContract.getPumpInLocation().getPumpLocation());
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    private static WaterUser buildTestWaterUser(String entityName) {
        return new WaterUser.Builder().withEntityName(entityName).withProjectId(new CwmsId.Builder()
                                .withName("Test Location Name")
                                .withOfficeId(OFFICE_ID)
                                .build())
                .withWaterRight("Test Water Right").build();
    }

    private static WaterUserContract buildTestWaterContract(String contractName, boolean renameTest) {
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
                            .withName(contractName)
                            .withOfficeId(OFFICE_ID)
                            .build())
                    .withFutureUsePercentActivated(35.7)
                    .withWaterUser(testUser)
                    .withPumpInLocation(new WaterSupplyPump.Builder()
                            .withPumpLocation(buildTestLocation("Pump 1 " + contractName,
                            "PUMP")).withPumpType(PumpType.IN).build())
                    .withPumpOutLocation(new WaterSupplyPump.Builder()
                            .withPumpLocation(buildTestLocation("Pump 2 " + contractName,
                            "PUMP")).withPumpType(PumpType.OUT).build())
                    .withPumpOutBelowLocation(new WaterSupplyPump.Builder()
                            .withPumpLocation(buildTestLocation("Pump 3 " + contractName,
                            "PUMP")).withPumpType(PumpType.BELOW).build())
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
                            .withName(contractName)
                            .withOfficeId(OFFICE_ID)
                            .build())
                    .withFutureUsePercentActivated(35.7)
                    .withWaterUser(buildTestWaterUser("Water User Name"))
                    .withPumpInLocation(new WaterSupplyPump.Builder().withPumpLocation(buildTestLocation("Pump 1",
                            "PUMP")).withPumpType(PumpType.IN).build())
                    .withPumpOutLocation(new WaterSupplyPump.Builder().withPumpLocation(buildTestLocation("Pump 2",
                            "PUMP")).withPumpType(PumpType.OUT).build())
                    .withPumpOutBelowLocation(new WaterSupplyPump.Builder().withPumpLocation(buildTestLocation("Pump 3",
                            "PUMP")).withPumpType(PumpType.BELOW).build())
                    .build();
        }
    }

    private static Location buildTestLocation(String locationName, String locationType) {
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

    private static Project buildTestProject() {
        return new Project.Builder().withLocation(buildTestLocation("Test Location Name",
                        "Test Location Type"))
                .withFederalCost(new BigDecimal("15980654.55"))
            .build();
    }
}
