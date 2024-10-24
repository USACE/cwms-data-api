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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import cwms.cda.api.enums.UnitSystem;
import cwms.cda.api.errors.NotFoundException;
import cwms.cda.data.dao.DeleteRule;
import cwms.cda.data.dao.LocationsDaoImpl;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.Location;
import cwms.cda.data.dto.LocationLevel;
import cwms.cda.data.dto.LookupType;
import cwms.cda.data.dto.location.kind.Lock;
import cwms.cda.helpers.DTOMatch;
import fixtures.CwmsDataApiSetupCallback;
import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import mil.army.usace.hec.test.database.CwmsDatabaseContainer;
import org.jooq.DSLContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

@Tag("integration")
@TestInstance(Lifecycle.PER_CLASS)
final class LockDaoIT extends ProjectStructureIT {
    private static final String LOCK_KIND = "LOCK";
    private static final Location LOCK_LOC1 = buildProjectStructureLocation("LOCK_LOC1_IT", LOCK_KIND);
    private static final Location LOCK_LOC2 = buildProjectStructureLocation("LOCK_LOC2_IT", LOCK_KIND);
    private static final Location LOCK_LOC3 = buildProjectStructureLocation("LOCK_LOC3_IT", LOCK_KIND);
    private static final Logger LOGGER = Logger.getLogger(LockDaoIT.class.getName());
    private List<Lock> locksToCleanup = new ArrayList<>();

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
        tearDownProject();
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
                DSLContext context = getDslContext(c, OFFICE_ID);
                LockDao lockDao = new LockDao(context);
                LocationsDaoImpl locationsDao = new LocationsDaoImpl(context);
                try {
                    locationsDao.deleteLocation(LOCK_LOC1.getName(), OFFICE_ID, true);
                    locationsDao.deleteLocation(LOCK_LOC2.getName(), OFFICE_ID, true);
                    locationsDao.deleteLocation(LOCK_LOC3.getName(), OFFICE_ID, true);
                    for (Lock lock : locksToCleanup) {
                        CwmsId cwmsId = CwmsId.buildCwmsId(lock.getLocation().getOfficeId(), lock.getLocation().getName());
                        lockDao.deleteLock(cwmsId, DeleteRule.DELETE_ALL);
                    }
                    locksToCleanup.clear();
                } catch (NotFoundException ex) {
                    /* only an error within the tests below. */
                    LOGGER.log(Level.CONFIG, "Error deleting location - location does not exist", ex);
                }
            },
            CwmsDataApiSetupCallback.getWebUser());
    }

    @Test
    void testRoundTrip() throws Exception {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
                DSLContext context = getDslContext(c, OFFICE_ID);
                LockDao lockDao = new LockDao(context);
                Lock lock = buildTestLock(LOCK_LOC1, PROJECT_LOC.getName());
                lockDao.storeLock(lock, false);
                locksToCleanup.add(lock);
                String lockId = lock.getLocation().getName();
                String lockOfficeId = lock.getLocation().getOfficeId();
                CwmsId cwmsId = CwmsId.buildCwmsId(lockOfficeId, lockId);
                Lock retrievedLock = lockDao.retrieveLock(cwmsId, UnitSystem.EN);
                DTOMatch.assertMatch(lock, retrievedLock);
                lockDao.deleteLock(cwmsId, DeleteRule.DELETE_ALL);
                assertThrows(NotFoundException.class, () -> lockDao.retrieveLock(cwmsId, UnitSystem.EN));
                locksToCleanup.remove(lock);
            },
            CwmsDataApiSetupCallback.getWebUser());
    }

    @Test
    void testRoundTripMulti() throws Exception {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
                DSLContext context = getDslContext(c, OFFICE_ID);
                LockDao lockDao = new LockDao(context);
                Lock lock1 = buildTestLock(LOCK_LOC1, PROJECT_LOC.getName());
                lockDao.storeLock(lock1, false);
                locksToCleanup.add(lock1);
                Lock lock2 = buildTestLock(LOCK_LOC2, PROJECT_LOC.getName());
                lockDao.storeLock(lock2, false);
                locksToCleanup.add(lock2);
                Lock lock3 = buildTestLock(LOCK_LOC3, PROJECT_LOC2.getName());
                lockDao.storeLock(lock2, false);
                locksToCleanup.add(lock3);
                String lockId = lock2.getLocation().getName();
                String lockOfficeId = lock2.getLocation().getOfficeId();
                CwmsId projectId = CwmsId.buildCwmsId(lock1.getProjectId().getOfficeId(), lock1.getProjectId().getName());
                List<CwmsId> retrievedLock = lockDao.retrieveLockIds(projectId, null);
                assertEquals(2, retrievedLock.size());
                assertTrue(retrievedLock.stream()
                    .anyMatch(e -> e.getName().split("-")[1].equalsIgnoreCase(lock1.getLocation().getName())));
                assertTrue(retrievedLock.stream()
                    .anyMatch(e -> e.getName().split("-")[1].equalsIgnoreCase(lock2.getLocation().getName())));
                assertFalse(retrievedLock.stream()
                    .anyMatch(e -> e.getName().split("-")[1].equalsIgnoreCase(lock3.getLocation().getName())));
                CwmsId cwmsId = CwmsId.buildCwmsId(lockOfficeId, lockId);
                lockDao.deleteLock(cwmsId, DeleteRule.DELETE_ALL);
                assertThrows(NotFoundException.class, () -> lockDao.retrieveLock(cwmsId, null));
                locksToCleanup.remove(lock2);
            },
            CwmsDataApiSetupCallback.getWebUser());
    }

    @Test
    void testRename() throws Exception {
        CwmsDatabaseContainer<?> databaseLink = CwmsDataApiSetupCallback.getDatabaseLink();
        databaseLink.connection(c -> {
                DSLContext context = getDslContext(c, OFFICE_ID);
                LockDao lockDao = new LockDao(context);
                Lock lock = buildTestLock(LOCK_LOC1, PROJECT_LOC.getName());
                lockDao.storeLock(lock, false);
                locksToCleanup.add(lock);
                String originalId = lock.getLocation().getName();
                String office = lock.getLocation().getOfficeId();
                String newId = lock.getLocation().getName() + "New";
                CwmsId cwmsId = CwmsId.buildCwmsId(office, originalId);
                lockDao.renameLock(cwmsId, newId);
                assertThrows(NotFoundException.class, () -> lockDao.retrieveLock(cwmsId, UnitSystem.EN));
                CwmsId newCwmsId = CwmsId.buildCwmsId(office, newId);
                Lock retrievedLock = lockDao.retrieveLock(newCwmsId, UnitSystem.EN);
                assertEquals(newId, retrievedLock.getLocation().getName());
                lockDao.deleteLock(newCwmsId, DeleteRule.DELETE_ALL);
                locksToCleanup.remove(lock);
            },
            CwmsDataApiSetupCallback.getWebUser());
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
            .withUnits("ft")
            .withVolumeUnits("ft3")
            .withHighWaterLowerPoolWarningLevel(2)
            .withHighWaterUpperPoolWarningLevel(2)
            .withChamberType(new LookupType.Builder().withOfficeId("LRD").withActive(true)
                    .withTooltip("CHAMBER").withDisplayValue("Land Side Main").build())
            .withHighWaterLowerPoolLocationLevel(new LocationLevel.Builder("HIGH_WATER_LOWER",
                    ZonedDateTime.parse("2024-09-15T00:00:00Z"))
                .withLevelComment("High Water Lower Pool Location Level")
                .withLevelUnitsId("ft")
                .withParameterId("Elev")
                .withConstantValue(1.5)
                .withOfficeId("SPK")
                .build())
            .withHighWaterUpperPoolLocationLevel(new LocationLevel.Builder("HIGH_WATER_UPPER",
                    ZonedDateTime.parse("2024-09-16T00:00:00Z"))
                .withLevelComment("High Water Upper Pool Location Level")
                .withLevelUnitsId("ft")
                .withParameterId("Elev")
                .withConstantValue(2.5)
                .withOfficeId("SPK")
                .build())
            .withLowWaterLowerPoolLocationLevel(new LocationLevel.Builder("LOW_WATER_LOWER",
                    ZonedDateTime.parse("2024-09-17T00:00:00Z"))
                .withLevelComment("Low Water Lower Pool Location Level")
                .withLevelUnitsId("ft")
                .withParameterId("Elev")
                .withConstantValue(3.14)
                .withOfficeId("SPK")
                .build())
            .withLowWaterUpperPoolLocationLevel(new LocationLevel.Builder("LOW_WATER_UPPER",
                    ZonedDateTime.parse("2024-09-18T00:00:00Z"))
                .withLevelComment("Low Water Upper Pool Location Level")
                .withLevelUnitsId("ft")
                .withParameterId("Elev")
                .withConstantValue(6.5)
                .withOfficeId("SPK")
                .build())
            .build();
    }
}
