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
import cwms.cda.api.errors.NotFoundException;
import cwms.cda.data.dao.DeleteRule;
import cwms.cda.data.dao.LocationsDaoImpl;
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
import mil.army.usace.hec.test.database.CwmsDatabaseContainer;
import org.jooq.DSLContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static cwms.cda.data.dao.JooqDao.getDslContext;
import static org.junit.jupiter.api.Assertions.*;

class WaterContractDaoTestIT extends DataApiTestIT {
    private static final String OFFICE_ID = "SPK";
    private static final String DELETE_ACTION = "DELETE ALL";
    private static final Location testLocation = buildTestLocation("Test Location Name", "Test Location");
    private static final Project testProject = buildTestProject();
    private static final WaterUser testUser = buildTestWaterUser("Test User");

    @BeforeAll
    static void setup() throws Exception {
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection(c -> {
            DSLContext ctx = getDslContext(c, OFFICE_ID);
            LocationsDaoImpl dao = new LocationsDaoImpl(ctx);
            ProjectDao projectDao = new ProjectDao(ctx);
            try {
                projectDao.create(testProject);
                dao.storeLocation(testLocation);
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
            LocationsDaoImpl locationDao = new LocationsDaoImpl(ctx);
            projectDao.delete(OFFICE_ID, testProject.getLocation().getName(), DeleteRule.DELETE_ALL);
            locationDao.deleteLocation(testLocation.getName(), testLocation.getOfficeId(), false);
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    @Test
    void testStoreAndRetrieveWaterUserList() throws Exception {
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection(c -> {
            DSLContext ctx = getDslContext(c, OFFICE_ID);
            WaterContractDao dao = new WaterContractDao(ctx);
            dao.storeWaterUser(buildTestWaterUser("TEST RETRIEVE USER 1"), false);
            dao.storeWaterUser(buildTestWaterUser("TEST RETRIEVE USER 2"), false);
            List<WaterUser> results = dao.getAllWaterUsers(new CwmsId.Builder().withOfficeId(OFFICE_ID).withName(testLocation.getName()).build());
            DTOMatch.assertMatch(results.get(0), buildTestWaterUser("TEST RETRIEVE USER 1"));
            DTOMatch.assertMatch(results.get(1), buildTestWaterUser("TEST RETRIEVE USER 2"));
        }, CwmsDataApiSetupCallback.getWebUser());
        cleanupUserRoutine(buildTestWaterUser("TEST RETRIEVE USER 1"));
        cleanupUserRoutine(buildTestWaterUser("TEST RETRIEVE USER 2"));
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
            WaterUser retrievedUser = dao.getWaterUser(projectLocation, entityName);
            DTOMatch.assertMatch(newUser, retrievedUser);
        }, CwmsDataApiSetupCallback.getWebUser());
        cleanupUserRoutine(newUser);
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
            List<WaterUserContract> retrievedContract = dao.getAllWaterContracts(contract.getWaterUser().getProjectId(),
                    contract.getWaterUser().getEntityName());
            WaterUserContract result = retrievedContract.get(0);
            DTOMatch.assertMatch(contract, result);
            result = retrievedContract.get(1);
            DTOMatch.assertMatch(contract2, result);
        }, CwmsDataApiSetupCallback.getWebUser());
        cleanupContractRoutine(contract);
        cleanupContractRoutine(contract2);
        cleanupUserRoutine(contract.getWaterUser());
    }

    @Test
    void testStoreAndRetrieveWaterContract() throws Exception {
        WaterUserContract contract = buildTestWaterContract("Test retrieve", false);
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection(c -> {
            DSLContext ctx = getDslContext(c, OFFICE_ID);
            WaterContractDao dao = new WaterContractDao(ctx);
            dao.storeWaterUser(contract.getWaterUser(), false);
            dao.storeWaterContract(contract, false, false);
            WaterUserContract retrievedContract = dao.getWaterContract(contract.getContractId().getName(),
                    contract.getWaterUser().getProjectId(), contract.getWaterUser().getEntityName());
            DTOMatch.assertMatch(contract, retrievedContract);
        }, CwmsDataApiSetupCallback.getWebUser());
        cleanupContractRoutine(contract);
        cleanupUserRoutine(contract.getWaterUser());
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
        WaterUser newUser = buildTestWaterUser("TEST RENAME USER");
        WaterUser updatedUser = buildTestWaterUser("TEST RENAME USER 2");
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection(c -> {
            DSLContext ctx = getDslContext(c, OFFICE_ID);
            WaterContractDao dao = new WaterContractDao(ctx);
            dao.storeWaterUser(newUser, false);
            dao.renameWaterUser(newUser.getEntityName(), updatedUser.getEntityName(), newUser.getProjectId());
            WaterUser retrievedUser = dao.getWaterUser(newUser.getProjectId(), updatedUser.getEntityName());
            String entityName = newUser.getEntityName();
            CwmsId projectId = newUser.getProjectId();
            assertThrows(NotFoundException.class, () -> dao.getWaterUser(projectId, entityName));
            DTOMatch.assertMatch(updatedUser, retrievedUser);
        }, CwmsDataApiSetupCallback.getWebUser());
        cleanupUserRoutine(updatedUser);
    }

    @Test
    void testRenameWaterContract() throws Exception {
        WaterUserContract oldContract = buildTestWaterContract("Test Old Name", true);
        WaterUserContract renamedContract = buildTestWaterContract("Test New Name", true);
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection(c -> {
            DSLContext ctx = getDslContext(c, OFFICE_ID);
            WaterContractDao dao = new WaterContractDao(ctx);
            dao.storeWaterUser(oldContract.getWaterUser(), false);
            dao.storeWaterContract(oldContract, false, false);
            WaterUser user = new WaterUser.Builder().withEntityName(oldContract.getWaterUser().getEntityName())
                    .withProjectId(oldContract.getWaterUser().getProjectId())
                    .withWaterRight(oldContract.getContractId().getName()).build();
            dao.renameWaterContract(user, oldContract.getContractId().getName(),
                    renamedContract.getContractId().getName());
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
        cleanupContractRoutine(renamedContract);
        cleanupUserRoutine(renamedContract.getWaterUser());
    }

    @Test
    void testDeleteWaterUser() throws Exception {
        WaterUser newUser = buildTestWaterUser("TEST DELETE USER");
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
        cleanupUserRoutine(newUser);
    }

    @Test
    void testDeleteWaterContract() throws Exception {
        WaterUserContract contract = buildTestWaterContract("Test Delete", false);
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection(c -> {
            DSLContext ctx = getDslContext(c, OFFICE_ID);
            WaterContractDao dao = new WaterContractDao(ctx);
            dao.storeWaterUser(contract.getWaterUser(), false);
            dao.storeWaterContract(contract, false, false);
            dao.deleteWaterContract(contract, DELETE_ACTION);
            String contractName = contract.getContractId().getName();
            CwmsId projectId = contract.getWaterUser().getProjectId();
            String entityName = contract.getWaterUser().getEntityName();
            assertThrows(NotFoundException.class, () -> dao.getWaterContract(contractName,
                    projectId, entityName));
        }, CwmsDataApiSetupCallback.getWebUser());
        cleanupUserRoutine(contract.getWaterUser());
    }

    @Test
    void testRemovePumpFromContract() throws Exception {
        WaterUserContract contract = buildTestWaterContract("Test Remove Pump", false);
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection(c -> {
            DSLContext ctx = getDslContext(c, OFFICE_ID);
            WaterContractDao dao = new WaterContractDao(ctx);
            assertNotNull(contract);
            dao.storeWaterUser(contract.getWaterUser(), false);
            dao.storeWaterContract(contract, false, false);
            dao.removePumpFromContract(contract, contract.getPumpInLocation().getPumpLocation().getName(),
                    PumpType.IN, false);
            WaterUserContract retrievedContract = dao.getWaterContract(contract.getContractId().getName(),
                    contract.getWaterUser().getProjectId(), contract.getWaterUser().getEntityName());
            assertNotNull(retrievedContract);
            assertNull(retrievedContract.getPumpInLocation().getPumpLocation());
        }, CwmsDataApiSetupCallback.getWebUser());
        cleanupContractRoutine(contract);
        cleanupUserRoutine(contract.getWaterUser());
    }

    private void cleanupUserRoutine(WaterUser user) throws Exception {
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection(c -> {
            DSLContext ctx = getDslContext(c, OFFICE_ID);
            WaterContractDao dao = new WaterContractDao(ctx);
            dao.deleteWaterUser(user.getProjectId(), user.getEntityName(), DELETE_ACTION);
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    private void cleanupContractRoutine(WaterUserContract contract) throws Exception {
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection(c -> {
            DSLContext ctx = getDslContext(c, OFFICE_ID);
            WaterContractDao dao = new WaterContractDao(ctx);
            dao.deleteWaterContract(contract, DELETE_ACTION);
        }, CwmsDataApiSetupCallback.getWebUser());
    }

    private static WaterUser buildTestWaterUser(String entityName) {
        return new WaterUser.Builder().withEntityName(entityName).withProjectId(new CwmsId.Builder()
                                .withName("Test Location Name")
                                .withOfficeId(OFFICE_ID)
                                .build())
                .withWaterRight("Test Water Right").build();
    }

    private WaterUserContract buildTestWaterContract(String contractName, boolean renameTest) {
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
