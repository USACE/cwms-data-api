package cwms.cda.data.dao.basin;

import static cwms.cda.data.dao.JooqDao.getDslContext;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import cwms.cda.api.DataApiTestIT;
import cwms.cda.api.enums.Nation;
import cwms.cda.api.errors.NotFoundException;
import cwms.cda.data.dao.DeleteRule;
import cwms.cda.data.dao.LocationsDao;
import cwms.cda.data.dao.LocationsDaoImpl;
import cwms.cda.data.dao.StreamDao;
import cwms.cda.data.dto.Location;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.basin.Basin;
import cwms.cda.data.dto.basin.BasinTest;
import cwms.cda.data.dto.stream.Stream;
import fixtures.CwmsDataApiSetupCallback;
import fixtures.TestAccounts;
import mil.army.usace.hec.test.database.CwmsDatabaseContainer;
import org.jooq.DSLContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


class BasinDaoTest extends DataApiTestIT {
    private static final String OFFICE_ID = TestAccounts.KeyUser.SWT_NORMAL.getOperatingOffice();
    private static final String UNIT_SYSTEM = "SI";
    private static final String UNITS = "km2";
    final String DELETE_ACTION = "DELETE ALL";
    private static final ArrayList<Location> locationsCreated = new ArrayList<>();
    private static final Basin parentBasin = new Basin.Builder()
            .withBasinId(new CwmsId.Builder()
                    .withName("TEST_PARENT_BASIN")
                    .withOfficeId(OFFICE_ID)
                    .build())
            .withAreaUnit(UNITS)
            .withPrimaryStreamId(new CwmsId.Builder()
                    .withName("TEST_PAR_PRIM_STREAM")
                    .withOfficeId(OFFICE_ID)
                    .build())
            .withTotalDrainageArea(100.0)
            .withSortOrder(1.0)
            .build();
    private static Stream testStream;

    @BeforeAll
    static void setup() {
        try {
            testStream = new Stream.Builder()
                    .withStartsDownstream(true)
                    .withId(new CwmsId.Builder()
                            .withName("TEST_PAR_PRIM_STREAM")
                            .withOfficeId(OFFICE_ID)
                            .build())
                    .withComment("TESTING TESTING")
                    .withSlopeUnits("%")
                    .withAverageSlope(0.01)
                    .withLengthUnits("km")
                    .withLength(100.0)
                    .withFlowsIntoStreamNode(null)
                    .withDivertsFromStreamNode(null)
                    .build();

            createTestLocation("TEST_PAR_PRIM_STREAM", "STREAM");

            createTestLocation("TEST_PARENT_BASIN", "BASIN");
            for (int i = 1; i < 7; i++) {
                createTestLocation("TEST_BASIN" + i, "BASIN");
            }

            CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
            db.connection(c -> {
                DSLContext ctx = getDslContext(c, OFFICE_ID);
                try {
                    StreamDao streamDao = new StreamDao(ctx);
                    BasinDao parentBasinDao = new BasinDao(ctx);
                    streamDao.storeStream(testStream, false);
                    parentBasinDao.storeBasin(parentBasin);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @AfterAll
    public static void tearDownStream() {
        try{
            CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
            db.connection(c -> {
                StreamDao streamDao = new StreamDao(getDslContext(c, OFFICE_ID));
                streamDao.deleteStream(testStream.getId().getOfficeId(), testStream.getId().getName(), DeleteRule.DELETE_ALL);
                LocationsDao locationsDao = new LocationsDaoImpl(getDslContext(c, OFFICE_ID));
                locationsDao.deleteLocation(testStream.getId().getName(), testStream.getId().getOfficeId(), true);
            });
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @AfterAll
    public static void tearDownLocations() {
        Iterator<Location> it = locationsCreated.iterator();
        while(it.hasNext()) {
            try {
                Location location = it.next();
                CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
                db.connection(c -> {
                    LocationsDao locationsDao = new LocationsDaoImpl(getDslContext(c, OFFICE_ID));
                    locationsDao.deleteLocation(location.getName(), location.getOfficeId(), true);
                });
                it.remove();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    private static void createTestLocation(String locationName, String kind) throws SQLException{
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        Location location = new Location.Builder(locationName,
                kind,
                ZoneId.of("UTC"),
                0.0,
                0.0,
                "WGS84",
                OFFICE_ID)
                .withActive(true)
                .withCountyName("Douglas")
                .withNation(Nation.US)
                .withStateInitial("CO")
                .build();
        if(locationsCreated.contains(location)){
            return;
        }
        db.connection(c -> {
            try {
                LocationsDaoImpl locationsDao = new LocationsDaoImpl(getDslContext(c, OFFICE_ID));
                locationsDao.storeLocation(location);
                if (!kind.equalsIgnoreCase("STREAM")) {
                    locationsCreated.add(location);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    void testGetBasin() throws Exception {
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        Basin basin = buildTestBasin(1);
        db.connection(c -> {
            DSLContext ctx = getDslContext(c, OFFICE_ID);
            try {
                BasinDao basinDao = new BasinDao(ctx);
                assertNotNull(basin);
                basinDao.storeBasin(basin);
                Basin retrievedBasin = basinDao.getBasin(basin.getBasinId(), UNITS);
                BasinTest.assertSame(basin, retrievedBasin);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        cleanUpRoutine(basin);
    }

    @Test
    void testGetBasins() throws Exception {
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        Basin basin = buildTestBasin(2);
        Basin basin1 = buildTestBasin(3);
        db.connection(c -> {
            DSLContext ctx = getDslContext(c, OFFICE_ID);
            try {
                List<Basin> basins = new ArrayList<>();
                BasinDao basinDao = new BasinDao(ctx);
                assertNotNull(basin);
                basinDao.storeBasin(basin);
                basins.add(basin);
                assertNotNull(basin1);
                basinDao.storeBasin(basin1);
                basins.add(basin1);
                List<Basin> retrievedBasins = basinDao.getAllBasins(UNIT_SYSTEM, OFFICE_ID);

                assertNotNull(retrievedBasins);
                for (int i = 0; i < basins.size(); i++) {
                    BasinTest.assertSame(basins.get(i), retrievedBasins.get(i));
                }
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        });
        cleanUpRoutine(basin);
        cleanUpRoutine(basin1);
    }

    @Test
    void testStoreBasin() throws Exception {
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        Basin basin = buildTestBasin(4);
        db.connection(c -> {
            DSLContext ctx = getDslContext(c, OFFICE_ID);
            try {
                assertNotNull(basin);
                BasinDao basinDao = new BasinDao(ctx);
                basinDao.storeBasin(basin);
                Basin retrievedBasin = basinDao.getBasin(basin.getBasinId(), UNIT_SYSTEM);
                BasinTest.assertSame(basin, retrievedBasin);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        cleanUpRoutine(basin);
    }

    @Test
    void testRenameBasin() throws Exception {
        Basin basin = buildTestBasin(5);
        assertNotNull(basin);
        Basin renamedBasin = new Basin.Builder()
                .withBasinId(new CwmsId.Builder()
                        .withName("RENAMED_TEST_BASIN2")
                        .withOfficeId(OFFICE_ID)
                        .build())
                .withAreaUnit(UNITS)
                .withTotalDrainageArea(100.0)
                .withParentBasinId(parentBasin.getBasinId())
                .withSortOrder(1.0)
                .withContributingDrainageArea(50.0)
                .withPrimaryStreamId(new CwmsId.Builder()
                        .withName("TEST_PAR_PRIM_STREAM")
                        .withOfficeId(OFFICE_ID)
                        .build())
                .build();
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection(c -> {
            DSLContext ctx = getDslContext(c, OFFICE_ID);
            BasinDao basinDao = new BasinDao(ctx);
            try {
                basinDao.storeBasin(basin);
                basinDao.renameBasin(basin.getBasinId(), renamedBasin.getBasinId());
                Basin retrievedBasin = basinDao.getBasin(renamedBasin.getBasinId(), UNIT_SYSTEM);
                assertNotNull(retrievedBasin);
                BasinTest.assertSame(renamedBasin, retrievedBasin);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        cleanUpRoutine(renamedBasin);
    }

    @Test
    void testDeleteBasin() throws Exception {
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection(c -> {
            DSLContext ctx = getDslContext(c, OFFICE_ID);
            try {
                Basin basin = buildTestBasin(6);
                assertNotNull(basin);
                BasinDao basinDao = new BasinDao(ctx);
                basinDao.storeBasin(basin);
                basinDao.deleteBasin(basin.getBasinId(), DELETE_ACTION);
                CwmsId basinId = basin.getBasinId();
                assertThrows(NotFoundException.class, () -> basinDao.getBasin(basinId, UNIT_SYSTEM));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    private void cleanUpRoutine(Basin basin) throws Exception {
        assertNotNull(basin);
        CwmsDatabaseContainer<?> db = CwmsDataApiSetupCallback.getDatabaseLink();
        db.connection(c -> {
            DSLContext ctx = getDslContext(c, OFFICE_ID);
            try {
                BasinDao basinDao = new BasinDao(ctx);
                basinDao.deleteBasin(basin.getBasinId(), DELETE_ACTION);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    private Basin buildTestBasin(int num) {
        return new Basin.Builder()
                .withBasinId(new CwmsId.Builder()
                        .withName("TEST_BASIN" + num)
                        .withOfficeId(OFFICE_ID)
                        .build())
                .withAreaUnit(UNITS)
                .withTotalDrainageArea(100.0)
                .withParentBasinId(parentBasin.getBasinId())
                .withSortOrder(1.0)
                .withContributingDrainageArea(50.0)
                .withPrimaryStreamId(new CwmsId.Builder()
                        .withName("TEST_PAR_PRIM_STREAM")
                        .withOfficeId(OFFICE_ID)
                        .build())
                .build();
    }
}
