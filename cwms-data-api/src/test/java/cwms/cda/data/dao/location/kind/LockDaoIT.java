/*
 * MIT License
 *
 * Copyright (c) 2024 Hydrologic Engineering Center
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
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
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package cwms.cda.data.dao.location.kind;

import static cwms.cda.data.dao.DaoTest.getDslContext;
import static org.junit.jupiter.api.Assertions.*;

import cwms.cda.api.enums.Nation;
import cwms.cda.api.enums.UnitSystem;
import cwms.cda.api.errors.NotFoundException;
import cwms.cda.data.dao.DeleteRule;
import cwms.cda.data.dao.LocationLevelsDao;
import cwms.cda.data.dao.LocationLevelsDaoImpl;
import cwms.cda.data.dao.LocationsDaoImpl;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.Location;
import cwms.cda.data.dto.LocationLevel;
import cwms.cda.data.dto.LookupType;
import cwms.cda.data.dto.location.kind.Lock;
import cwms.cda.data.dto.location.kind.LockLocationLevelRef;
import cwms.cda.helpers.DTOMatch;
import fixtures.CwmsDataApiSetupCallback;
import java.io.IOException;
import java.sql.SQLException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import mil.army.usace.hec.test.database.CwmsDatabaseContainer;
import org.jooq.DSLContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

@Tag("integration")
@TestInstance(Lifecycle.PER_CLASS)
final class LockDaoIT extends ProjectStructureIT {
    private static final String LOCK_KIND = "SITE";
    private static final Location LOCK_LOC1 = buildProjectStructureLocation("LOCK_LOC1_IT", LOCK_KIND);
    private static final Location LOCK_LOC2 = buildProjectStructureLocation("LOCK_LOC2_IT", LOCK_KIND);
    private static final Location LOCK_LOC3 = buildProjectStructureLocation("LOCK_LOC3_IT", LOCK_KIND);
    private static final Logger LOGGER = Logger.getLogger(LockDaoIT.class.getName());
    private List<CwmsId> locksToCleanup = new ArrayList<>();
    private List<LocationLevel> locationLevelsToCleanup = new ArrayList<>();

    @BeforeAll
    public void setup() throws Exception {
        setupProject();
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
                DSLContext context = getDslContext(c, OFFICE_ID);
                LocationsDaoImpl locationsDao = new LocationsDaoImpl(context);
                try {
                    locationsDao.storeLocation(LOCK_LOC1);
                    locationsDao.storeLocation(LOCK_LOC2);
                    locationsDao.storeLocation(LOCK_LOC3);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            },
            CwmsDataApiSetupCallback.getWebUser());
    }

    @AfterAll
    public void tearDown() throws Exception {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
                DSLContext context = getDslContext(c, OFFICE_ID);
                LockDao lockDao = new LockDao(context);
                LocationsDaoImpl locationsDao = new LocationsDaoImpl(context);
                LocationLevelsDao locationLevelsDao = new LocationLevelsDaoImpl(context);
                try {
                    locationsDao.deleteLocation(LOCK_LOC1.getName(), OFFICE_ID, true);
                } catch (NotFoundException ex) {
                    /* only an error within the tests below. */
                    LOGGER.log(Level.CONFIG, "Error deleting location - location does not exist", ex);
                }
                try {
                    locationsDao.deleteLocation(LOCK_LOC2.getName(), OFFICE_ID, true);
                } catch (NotFoundException ex) {
                    /* only an error within the tests below. */
                    LOGGER.log(Level.CONFIG, "Error deleting location - location does not exist", ex);
                }
                try {
                    locationsDao.deleteLocation(LOCK_LOC3.getName(), OFFICE_ID, true);
                } catch (NotFoundException ex) {
                    /* only an error within the tests below. */
                    LOGGER.log(Level.CONFIG, "Error deleting location - location does not exist", ex);
                }
                try{
                    locationsDao.deleteLocation(LOCK_LOC1.getName() + "New", OFFICE_ID, true);
                } catch (NotFoundException ex) {
                    /* only an error within the tests below. */
                    LOGGER.log(Level.CONFIG, "Error deleting location - location does not exist", ex);
                }
                for (CwmsId lock : locksToCleanup) {
                    try {
                        lockDao.deleteLock(lock, DeleteRule.DELETE_ALL);
                    } catch (NotFoundException ex) {
                        /* only an error within the tests below. */
                        LOGGER.log(Level.CONFIG, "Error deleting lock - does not exist", ex);
                    }
                }
                locksToCleanup.clear();
                for (LocationLevel locationLevel : locationLevelsToCleanup) {
                    try {
                        locationLevelsDao.deleteLocationLevel(locationLevel.getLocationLevelId(), locationLevel.getLevelDate(), locationLevel.getOfficeId(), true);
                    } catch (NotFoundException ex) {
                        LOGGER.log(Level.CONFIG, "Error deleting location level - does not exist", ex);
                    }
                }
                locationLevelsToCleanup.clear();
            },
            CwmsDataApiSetupCallback.getWebUser());
    }

    @AfterEach
    void cleanup() throws Exception
    {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c ->
        {
            DSLContext context = getDslContext(c, OFFICE_ID);
            LocationLevelsDao locationLevelsDao = new LocationLevelsDaoImpl(context);
            LockDao lockDao = new LockDao(context);
            for (CwmsId lock : locksToCleanup) {
                try {
                    lockDao.deleteLock(lock, DeleteRule.DELETE_ALL);
                } catch (NotFoundException ex) {
                    /* only an error within the tests below. */
                    LOGGER.log(Level.CONFIG, "Error deleting lock - does not exist", ex);
                }
            }
            locksToCleanup.clear();
            for (LocationLevel locationLevel : locationLevelsToCleanup) {
                try {
                    locationLevelsDao.deleteLocationLevel(locationLevel.getLocationLevelId(), locationLevel.getLevelDate(), locationLevel.getOfficeId(), true);
                } catch (NotFoundException ex) {
                    LOGGER.log(Level.CONFIG, "Error deleting location level - does not exist", ex);
                }
            }
            locationLevelsToCleanup.clear();
        });
    }

    @Test
    void testRoundTrip() throws Exception {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        Lock lockWithLevels = buildTestLock(LOCK_LOC1, PROJECT_LOC.getName());
        Lock lock = storeLocLevelsAndBuildStorableLock(lockWithLevels);
        databaseLink.connection(c -> {
                DSLContext context = getDslContext(c, OFFICE_ID);
                LockDao lockDao = new LockDao(context);
                lockDao.storeLock(lock, false);
                String lockId = lock.getLocation().getName();
                String lockOfficeId = lock.getLocation().getOfficeId();
                CwmsId cwmsId = CwmsId.buildCwmsId(lockOfficeId, lockId);
                locksToCleanup.add(cwmsId);
                Lock retrievedLock = lockDao.retrieveLock(cwmsId, UnitSystem.EN);
                 DTOMatch.assertMatch(lockWithLevels, retrievedLock, true);
                lockDao.deleteLock(cwmsId, DeleteRule.DELETE_ALL);
                assertThrows(NotFoundException.class, () -> lockDao.retrieveLock(cwmsId, UnitSystem.SI));
                locksToCleanup.remove(cwmsId);
            },
            CwmsDataApiSetupCallback.getWebUser());
    }

    @Test
    void testRoundTripMulti() throws Exception {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        Lock lock1 = storeLocLevelsAndBuildStorableLock(buildTestLock(LOCK_LOC1, PROJECT_LOC.getName()));
        Lock lock2 = storeLocLevelsAndBuildStorableLock(buildTestLock(LOCK_LOC2, PROJECT_LOC.getName()));
        Lock lock3 = storeLocLevelsAndBuildStorableLock(buildTestLock(LOCK_LOC3, PROJECT_LOC2.getName()));
        databaseLink.connection(c -> {
                DSLContext context = getDslContext(c, OFFICE_ID);
                LockDao lockDao = new LockDao(context);
                lockDao.storeLock(lock1, false);
                locksToCleanup.add(CwmsId.buildCwmsId(lock1.getLocation().getOfficeId(), lock1.getLocation().getName()));
                lockDao.storeLock(lock2, false);
                locksToCleanup.add(CwmsId.buildCwmsId(lock2.getLocation().getOfficeId(), lock2.getLocation().getName()));
                lockDao.storeLock(lock2, false);
                locksToCleanup.add(CwmsId.buildCwmsId(lock3.getLocation().getOfficeId(), lock3.getLocation().getName()));
                String lockId = lock2.getLocation().getName();
                String lockOfficeId = lock2.getLocation().getOfficeId();
                CwmsId projectId = CwmsId.buildCwmsId(lock1.getProjectId().getOfficeId(), lock1.getProjectId().getName());
                List<Lock> retrievedLock = lockDao.retrieveLockCatalog(projectId);
                assertEquals(2, retrievedLock.size());
                assertTrue(retrievedLock.stream()
                    .anyMatch(e -> e.getLocation().getName().equalsIgnoreCase(lock1.getLocation().getName())));
                assertTrue(retrievedLock.stream()
                    .anyMatch(e -> e.getLocation().getName().equalsIgnoreCase(lock2.getLocation().getName())));
                assertFalse(retrievedLock.stream()
                    .anyMatch(e -> e.getLocation().getName().equalsIgnoreCase(lock3.getLocation().getName())));
                CwmsId cwmsId = CwmsId.buildCwmsId(lockOfficeId, lockId);
                lockDao.deleteLock(cwmsId, DeleteRule.DELETE_ALL);
                assertThrows(NotFoundException.class, () -> lockDao.retrieveLock(cwmsId, UnitSystem.SI));
                locksToCleanup.remove(CwmsId.buildCwmsId(lock2.getLocation().getOfficeId(), lock2.getLocation().getName()));
            },
            CwmsDataApiSetupCallback.getWebUser());
    }

    @Test
    void testRename() throws Exception {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        Lock lock = storeLocLevelsAndBuildStorableLock(buildTestLock(LOCK_LOC1, PROJECT_LOC.getName()));
        databaseLink.connection(c -> {
                DSLContext context = getDslContext(c, OFFICE_ID);
                LockDao lockDao = new LockDao(context);
                lockDao.storeLock(lock, false);
                String originalId = lock.getLocation().getName();
                String office = lock.getLocation().getOfficeId();
                String newId = lock.getLocation().getName() + "New";
                CwmsId newCwmsId = CwmsId.buildCwmsId(office, newId);
                CwmsId cwmsId = CwmsId.buildCwmsId(office, originalId);
                locksToCleanup.add(cwmsId);
                lockDao.renameLock(cwmsId, newId);
                assertThrows(NotFoundException.class, () -> lockDao.retrieveLock(cwmsId, UnitSystem.SI));
                Lock retrievedLock = lockDao.retrieveLock(newCwmsId, UnitSystem.SI);
                assertNotNull(retrievedLock.getLocation());
                assertEquals(newId, retrievedLock.getLocation().getName());
                lockDao.deleteLock(newCwmsId, DeleteRule.DELETE_ALL);
                locksToCleanup.remove(cwmsId);
            },
            CwmsDataApiSetupCallback.getWebUser());
    }

    @Test
    void testUnitConvertToEN() throws Exception {
        Lock lock = buildTestLockSI();
        Lock expected = buildTestLock();
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c ->
        {
            DSLContext context = getDslContext(c, OFFICE_ID);
            LockDao dao = new LockDao(context);
            Lock lockEN = dao.unitConvertToEN(lock);
            DTOMatch.assertMatch(expected, lockEN, false);
        });
    }

    private static Lock buildTestLock(Location location, String projectId) {
        return new Lock.Builder()
            .withLocation(location)
            .withProjectId(new CwmsId.Builder()
                .withName(projectId)
                .withOfficeId(PROJECT_LOC.getOfficeId())
                .build())
            .withLockWidth(100.0)
            .withLockLength(100.0)
            .withNormalLockLift(10.0)
            .withMaximumLockLift(20.0)
            .withVolumePerLockage(100.0)
            .withMinimumDraft(5.0)
            .withLengthUnits("ft")
            .withVolumeUnits("ft3")
            .withElevationUnits("ft")
            .withHighWaterLowerPoolWarningLevel(3.0) // will not be stored when lock is stored, must save as location level
            .withHighWaterUpperPoolWarningLevel(3.0) // will not be stored when lock is stored, must save as location level
            .withChamberType(new LookupType.Builder().withOfficeId("CWMS").withActive(true)
                .withTooltip("The main chamber on the land side of the lock").withDisplayValue("Land Side Main").build())
            .withHighWaterLowerPoolLocationLevel(
                new LockLocationLevelRef(String.format("/locks/%s.Elev-Closure.Inst.0.High Water Lower Pool?office=SPK", location.getName()), 4.5))
            .withHighWaterUpperPoolLocationLevel(
                new LockLocationLevelRef(String.format("/locks/%s.Elev-Closure.Inst.0.High Water Upper Pool?office=SPK", location.getName()), 5.5))
            .withLowWaterLowerPoolLocationLevel(
                new LockLocationLevelRef(String.format("/locks/%s.Elev-Closure.Inst.0.Low Water Lower Pool?office=SPK", location.getName()), 8.14))
            .withLowWaterUpperPoolLocationLevel(
                new LockLocationLevelRef(String.format("/locks/%s.Elev-Closure.Inst.0.Low Water Upper Pool?office=SPK", location.getName()), 6.5))
            .build();
    }

    private Lock storeLocLevelsAndBuildStorableLock(Lock lock) throws SQLException {
        // returns storable Lock object
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c ->
        {
            DSLContext context = getDslContext(c, OFFICE_ID);
            LocationLevelsDao locDao = new LocationLevelsDaoImpl(context);
            List<LocationLevel> locLevelList = createLocationLevelList(lock);
            locationLevelsToCleanup.addAll(locLevelList);
            for (LocationLevel locLevel : locLevelList) {
                locDao.storeLocationLevel(locLevel);
            }
        }, CwmsDataApiSetupCallback.getWebUser());
        return new Lock.Builder()
            .withLocation(lock.getLocation())
            .withProjectId(lock.getProjectId())
            .withLockWidth(lock.getLockWidth())
            .withLockLength(lock.getLockLength())
            .withNormalLockLift(lock.getNormalLockLift())
            .withMaximumLockLift(lock.getMaximumLockLift())
            .withVolumePerLockage(lock.getVolumePerLockage())
            .withMinimumDraft(lock.getMinimumDraft())
            .withLengthUnits(lock.getLengthUnits())
            .withElevationUnits(lock.getElevationUnits())
            .withVolumeUnits(lock.getVolumeUnits())
            .withHighWaterLowerPoolWarningLevel(lock.getHighWaterLowerPoolWarningLevel())
            .withHighWaterUpperPoolWarningLevel(lock.getHighWaterUpperPoolWarningLevel())
            .withChamberType(lock.getChamberType())
            .build();
    }

    private List<LocationLevel> createLocationLevelList(Lock lock) {
        List<LocationLevel> retVal = new ArrayList<>();
        LocationLevel lowLowerLevel = new LocationLevel.Builder(lock.getLowWaterLowerPoolLocationLevel().getLevelId(), ZonedDateTime.now())
            .withLevelUnitsId(lock.getElevationUnits())
            .withConstantValue(lock.getLowWaterLowerPoolLocationLevel().getLevelValue())
            .withOfficeId(lock.getLowWaterLowerPoolLocationLevel().getOfficeId())
            .withSpecifiedLevelId(lock.getLowWaterLowerPoolLocationLevel().getSpecifiedLevelId())
            .build();
        retVal.add(lowLowerLevel);
        LocationLevel lowUpperLevel = new LocationLevel.Builder(lock.getLowWaterUpperPoolLocationLevel().getLevelId(), ZonedDateTime.now())
            .withLevelUnitsId(lock.getElevationUnits())
            .withConstantValue(lock.getLowWaterUpperPoolLocationLevel().getLevelValue())
            .withOfficeId(lock.getLowWaterUpperPoolLocationLevel().getOfficeId())
            .withSpecifiedLevelId(lock.getLowWaterUpperPoolLocationLevel().getSpecifiedLevelId())
            .build();
        retVal.add(lowUpperLevel);
        LocationLevel highLowerLevel = new LocationLevel.Builder(lock.getHighWaterLowerPoolLocationLevel().getLevelId(), ZonedDateTime.now())
            .withLevelUnitsId(lock.getElevationUnits())
            .withConstantValue(lock.getHighWaterLowerPoolLocationLevel().getLevelValue())
            .withOfficeId(lock.getHighWaterLowerPoolLocationLevel().getOfficeId())
            .withSpecifiedLevelId(lock.getHighWaterLowerPoolLocationLevel().getSpecifiedLevelId())
            .build();
        retVal.add(highLowerLevel);
        LocationLevel highUpperLevel = new LocationLevel.Builder(lock.getHighWaterUpperPoolLocationLevel().getLevelId(), ZonedDateTime.now())
            .withLevelUnitsId(lock.getElevationUnits())
            .withConstantValue(lock.getHighWaterUpperPoolLocationLevel().getLevelValue())
            .withOfficeId(lock.getHighWaterUpperPoolLocationLevel().getOfficeId())
            .withSpecifiedLevelId(lock.getHighWaterUpperPoolLocationLevel().getSpecifiedLevelId())
            .build();
        retVal.add(highUpperLevel);
        LocationLevel warningBuffer = new LocationLevel.Builder(String.format("%s.Elev-Closure.Inst.0.Warning Buffer", lock.getLocation().getName()), ZonedDateTime.now())
                .withLevelUnitsId(lock.getElevationUnits())
                .withConstantValue(lock.getHighWaterLowerPoolWarningLevel())
                .withOfficeId(lock.getLocation().getOfficeId())
                .withSpecifiedLevelId("Warning Buffer")
                .build();
        retVal.add(warningBuffer);
        return retVal;
    }

    private Lock buildTestLock() {
        return new Lock.Builder()
            .withLocation(buildTestLocation())
            .withProjectId(new CwmsId.Builder()
                    .withName("PROJECT")
                    .withOfficeId("SWT")
                    .build())
            .withLockLength(328.084)
            .withMaximumLockLift(65.6168)
            .withVolumeUnits("ft3")
            .withLengthUnits("ft")
            .withElevationUnits("ft")
            .withChamberType(new LookupType.Builder().withOfficeId("CWMS").withActive(true)
                    .withDisplayValue("LOCK").withTooltip("Land Side Main").build())
            .withLockWidth(328.084)
            .withNormalLockLift(32.8084)
            .withVolumePerLockage(3531.46667)
            .withMinimumDraft(16.4042)
            .withHighWaterLowerPoolLocationLevel(
                    new LockLocationLevelRef("/locks/TEST_LOCATION2.Elev-Closure.Inst.0.High Water Lower Pool?office=LRL", 60.69554))
            .withHighWaterUpperPoolLocationLevel(
                    new LockLocationLevelRef("/locks/TEST_LOCATION2.Elev-Closure.Inst.0.High Water Upper Pool?office=LRL", 67.25722))
            .withLowWaterLowerPoolLocationLevel(
                    new LockLocationLevelRef("/locks/TEST_LOCATION2.Elev-Closure.Inst.0.Low Water Lower Pool?office=LRL", 102.1654))
            .withLowWaterUpperPoolLocationLevel(
                    new LockLocationLevelRef("/locks/TEST_LOCATION2.Elev-Closure.Inst.0.Low Water Upper Pool?office=LRL", 54.13386))
            .withHighWaterLowerPoolWarningLevel(32.8084)
            .withHighWaterUpperPoolWarningLevel(32.8084)
            .build();
    }

    private Lock buildTestLockSI() {
        return new Lock.Builder()
                .withLocation(buildTestLocation())
                .withProjectId(new CwmsId.Builder()
                        .withName("PROJECT")
                        .withOfficeId("SWT")
                        .build())
                .withLockLength(100.0)
                .withMaximumLockLift(20.0)
                .withVolumeUnits("m3")
                .withLengthUnits("m")
                .withElevationUnits("m")
                .withChamberType(new LookupType.Builder().withOfficeId("CWMS").withActive(true)
                        .withDisplayValue("LOCK").withTooltip("Land Side Main").build())
                .withLockWidth(100.0)
                .withNormalLockLift(10.0)
                .withVolumePerLockage(100.0)
                .withMinimumDraft(5.0)
                .withHighWaterLowerPoolLocationLevel(
                        new LockLocationLevelRef("/locks/TEST_LOCATION2.Elev-Closure.Inst.0.High Water Lower Pool?office=LRL", 18.5))
                .withHighWaterUpperPoolLocationLevel(
                        new LockLocationLevelRef("/locks/TEST_LOCATION2.Elev-Closure.Inst.0.High Water Upper Pool?office=LRL", 20.5))
                .withLowWaterLowerPoolLocationLevel(
                        new LockLocationLevelRef("/locks/TEST_LOCATION2.Elev-Closure.Inst.0.Low Water Lower Pool?office=LRL", 31.14))
                .withLowWaterUpperPoolLocationLevel(
                        new LockLocationLevelRef("/locks/TEST_LOCATION2.Elev-Closure.Inst.0.Low Water Upper Pool?office=LRL", 16.5))
                .withHighWaterLowerPoolWarningLevel(10.0)
                .withHighWaterUpperPoolWarningLevel(10.0)
                .build();
    }

    private Location buildTestLocation() {
        return new Location.Builder("TEST_LOCATION2", "LOCK", ZoneId.of("UTC"),
            50.0, 50.0, "NVGD29", "SPK")
            .withElevation(10.0)
            .withElevationUnits("ft")
            .withLocationType("SITE")
            .withCountyName("Sacramento")
            .withNation(Nation.US)
            .withActive(true)
            .withStateInitial("CA")
            .withBoundingOfficeId("SPK")
            .withLongName("TEST_LOCATION")
            .withPublishedLatitude(50.0)
            .withPublishedLongitude(50.0)
            .withDescription("for testing")
            .withNearestCity("Davis")
            .build();
    }
}
