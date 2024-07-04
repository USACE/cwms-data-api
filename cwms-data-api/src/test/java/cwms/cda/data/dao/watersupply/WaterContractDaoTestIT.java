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
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.Location;
import cwms.cda.data.dto.LookupType;
import cwms.cda.data.dto.watersupply.PumpType;
import cwms.cda.data.dto.watersupply.WaterSupplyPump;
import cwms.cda.data.dto.watersupply.WaterUser;
import cwms.cda.data.dto.watersupply.WaterUserContract;
import cwms.cda.data.dto.watersupply.WaterUserContractTest;
import fixtures.CwmsDataApiSetupCallback;
import mil.army.usace.hec.test.database.CwmsDatabaseContainer;
import org.jooq.DSLContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static cwms.cda.data.dao.JooqDao.getDslContext;
import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class WaterContractDaoTestIT extends DataApiTestIT {
    private static final String OFFICE_ID = "SPK";
    private static final String DELETE_ACTION = "DELETE ALL";

    @BeforeAll
    public static void setup() {


    }

    @AfterAll
    public static void tearDown() {

    }

    @Test
    void testRetrieveWaterUserList() {

    }

    @Test
    void testStoreAndRetrieveWaterUser() throws Exception {
        CwmsId projectLocation = new CwmsId.Builder()
                .withName("Test Location name")
                .withOfficeId(OFFICE_ID)
                .build();
        String entityName = "Test Entity";
        WaterUser testUser = new WaterUser(entityName, projectLocation,
            "Test Water Right");

        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection(c -> {
            DSLContext ctx = getDslContext(c, OFFICE_ID);
            WaterContractDao dao = new WaterContractDao(ctx);
            dao.storeWaterUser(testUser, true);
            WaterUser retrievedUser = dao.getWaterUser(projectLocation, entityName);
            WaterUserContractTest.assertSame(testUser, retrievedUser);
        });
        cleanupUserRoutine(testUser);
    }

    @Test
    void testRetrieveWaterUserContractList() {

    }

    @Test
    void testStoreWaterContract() throws Exception {
        WaterUserContract contract = buildTestWaterContract();
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection(c -> {
            DSLContext ctx = getDslContext(c, OFFICE_ID);
            WaterContractDao dao = new WaterContractDao(ctx);
            dao.storeWaterContract(contract, true, false);
        });
        cleanupContractRoutine(contract);
    }

    @Test
    void testRetrieveWaterUserContract() {

    }

    @Test
    void testRetrieveWaterContractList() {

    }

    @Test
    void testStoreWaterContractType() throws Exception {
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
            dao.storeWaterContractTypes(contractType, true);
            List<LookupType> resultTypes = dao.getAllWaterContractTypes(OFFICE_ID);
            LookupType result = resultTypes.get(0);
            assertEquals(contractType.get(0).getDisplayValue(), result.getDisplayValue());
            assertEquals(contractType.get(0).getTooltip(), result.getTooltip());
            assertEquals(contractType.get(0).getActive(), result.getActive());
        });
        // cleanup??
    }

    @Test
    void testRenameWaterUser() {

    }

    @Test
    void testRenameWaterContract() {

    }

    @Test
    void testDeleteWaterUser() throws Exception {
        WaterUser newUser = buildTestWaterUser();
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection(c -> {
            DSLContext ctx = getDslContext(c, OFFICE_ID);
            WaterContractDao dao = new WaterContractDao(ctx);
            dao.storeWaterUser(newUser, true);
            dao.deleteWaterUser(newUser.getParentLocationRef(), newUser.getEntityName(), DELETE_ACTION);
            assertThrows(Exception.class, () -> dao.getWaterUser(newUser.getParentLocationRef(), newUser.getEntityName()));
        });
    }

    @Test
    void testDeleteWaterContract() throws Exception {
        WaterUserContract contract = buildTestWaterContract();
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection(c -> {
            DSLContext ctx = getDslContext(c, OFFICE_ID);
            WaterContractDao dao = new WaterContractDao(ctx);
            dao.storeWaterContract(contract, true, false);
            dao.deleteWaterContract(contract, DELETE_ACTION);
            assertThrows(Exception.class, () -> dao.getAllWaterContracts(contract.getWaterUser().getParentLocationRef(), contract.getWaterUser().getEntityName()));
        });
    }

    @Test
    void testRemovePumpFromContract() throws Exception {
        WaterUserContract contract = buildTestWaterContract();
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection(c -> {
            DSLContext ctx = getDslContext(c, OFFICE_ID);
            WaterContractDao dao = new WaterContractDao(ctx);
            dao.storeWaterContract(contract, true, false);
            dao.removePumpFromContract(contract, contract.getPumpInLocation().getPumpId().getName(), "", false);
            List<WaterUserContract> retrievedContract = dao.getAllWaterContracts(contract.getWaterUser().getParentLocationRef(), contract.getWaterUser().getEntityName());
            assertNull(retrievedContract.get(0).getPumpInLocation());
        });
    }

    private void cleanupUserRoutine(WaterUser user) throws Exception {
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection(c -> {
            DSLContext ctx = getDslContext(c, OFFICE_ID);
            WaterContractDao dao = new WaterContractDao(ctx);
            dao.deleteWaterUser(user.getParentLocationRef(), user.getEntityName(), DELETE_ACTION);
        });
    }

    private void cleanupContractRoutine(WaterUserContract contract) throws Exception {
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection(c -> {
            DSLContext ctx = getDslContext(c, OFFICE_ID);
            WaterContractDao dao = new WaterContractDao(ctx);
            dao.deleteWaterContract(contract, DELETE_ACTION);
        });
    }

    private WaterUser buildTestWaterUser() {
        return new WaterUser("Test Entity", new CwmsId.Builder()
                                .withName("Test Location name")
                                .withOfficeId(OFFICE_ID)
                                .build(),
                        "Test Water Right");
    }

    private WaterUserContract buildTestWaterContract() {
        return new WaterUserContract.Builder()
                .withWaterContract(new LookupType.Builder()
                        .withTooltip("Test Tooltip")
                        .withActive(true)
                        .withDisplayValue("Test Display Value")
                        .withOfficeId(OFFICE_ID).build())
                .withStorageUnitsId("%")
                .withFutureUseAllocation(158900.6)
                .withContractedStorage(1589005.6)
                .withInitialUseAllocation(1500.2)
                .withContractExpirationDate(new Date(1979252516))
                .withContractEffectiveDate(new Date(1766652851))
                .withTotalAllocPercentActivated(55.1)
                .withFutureUsePercentActivated(35.7)
                .withWaterUser(new WaterUser("Test Entity", new CwmsId.Builder()
                                .withName("Test Location name")
                                .withOfficeId(OFFICE_ID)
                                .build(),
                        "Test Water Right"))
                .withPumpInLocation(new WaterSupplyPump(buildTestLocation(), PumpType.PUMP_IN,
                        new CwmsId.Builder().withName("Test Pump").withOfficeId(OFFICE_ID).build()))
                .withPumpOutLocation(new WaterSupplyPump(buildTestLocation(), PumpType.PUMP_OUT, new CwmsId.Builder()
                        .withName("Test Pump").withOfficeId(OFFICE_ID).build()))
                .withPumpOutBelowLocation(new WaterSupplyPump(buildTestLocation(), PumpType.PUMP_OUT_BELOW,
                        new CwmsId.Builder().withOfficeId(OFFICE_ID).withName("Test Pump").build()))
                .build();
    }

    private Location buildTestLocation() {
        return new Location.Builder(OFFICE_ID, "Test Location Name")
                .withBoundingOfficeId(OFFICE_ID)
                .withMapLabel("Test Map Label")
                .withElevation(150.6)
                .withNation(Nation.US)
                .withStateInitial("CA")
                .withLatitude(35.6)
                .withLongitude(-120.6)
                .withPublishedLatitude(35.6)
                .withPublishedLongitude(-120.6)
                .withActive(true)
                .withLocationType("Test Location Type")
                .withLocationKind("Test Location Kind")
                .withName("Test Location Name")
                .build();
    }


}
