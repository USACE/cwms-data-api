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

package cwms.cda.data.dao.project;

import static cwms.cda.data.dao.DaoTest.getDslContext;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.google.common.flogger.FluentLogger;
import cwms.cda.api.errors.NotFoundException;
import cwms.cda.data.dao.DeleteRule;
import cwms.cda.data.dto.Location;
import cwms.cda.data.dto.project.LockRevokerRights;
import cwms.cda.data.dto.project.Project;
import cwms.cda.data.dto.project.ProjectLock;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.logging.Level;
import org.jooq.DSLContext;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Disabled("Preferring to maintain IT")
class ProjectLockDaoTest {
    FluentLogger logger = FluentLogger.forEnclosingClass();
    public static final String USER_ID = "L2WEBTEST";
    public static  final String OFFICE = "SPK";


    @Test
    void test_revoke_needs_rights() throws SQLException {
        // The structure of the test is to
        // Create a project.
        // Then create a lock
        // then try to revoke the lock - should fail
        // then get revoke perms
        // then revoke the lock and it should work
        // then cleanup lock and project

        DSLContext dsl = getDslContext(OFFICE);
        ProjectDao prjDao = new ProjectDao(dsl);
        ProjectLockDao lockDao = new ProjectLockDao(dsl);

        String projId = "needsRevoke";
        String appId = "needsRevoke_test";
        Project testProject = buildTestProject(OFFICE, projId);
        prjDao.create(testProject, true);

        try {
            lockDao.removeAllLockRevokerRights(OFFICE, appId, USER_ID); // reset

            int revokeTimeout = 10;
            boolean revokeExisting = false;

            ProjectLock req1 = new ProjectLock.Builder(OFFICE, projId, appId).build();
            String lockId = lockDao.requestLock(req1, revokeExisting, revokeTimeout);
            try {
                assertNotNull(lockId);
                assertTrue(lockId.length() > 8);

                try {
                    lockDao.revokeLock(OFFICE, projId, appId, 0);
                    fail("Should have thrown an exception");
                } catch (Exception e) {
                    logger.at(Level.INFO).log("Expected exception: %s", e.getMessage());
                }

            } finally {
                lockDao.updateLockRevokerRights(OFFICE, projId, appId, USER_ID, true);
                lockDao.revokeLock(OFFICE, projId, appId, 0);
            }
        } finally {
            deleteProject(prjDao, projId, lockDao, appId);
        }

    }

    @Test
    void test_has_rights_must_exist() throws SQLException {
        DSLContext dsl = getDslContext(OFFICE);
        ProjectLockDao lockDao = new ProjectLockDao(dsl);

        String projId = "ANonProject";
        String appId = "dont_exist";
        try {
            lockDao.hasLockRevokerRights(OFFICE, projId, appId, USER_ID);
            fail("Should have thrown an exception");
        } catch (NotFoundException e) {
            logger.at(Level.INFO).log("Expected exception: %s", e.getMessage());
        }
    }

    @Test
    void test_has_rights() throws SQLException {

        DSLContext dsl = getDslContext(OFFICE);
        ProjectDao prjDao = new ProjectDao(dsl);
        ProjectLockDao lockDao = new ProjectLockDao(dsl);

        String projId = "hasRights";
        String appId = "unitest";

        Project testProject = buildTestProject(OFFICE, projId);
        prjDao.create(testProject, true);

        try {
            assertFalse(lockDao.hasLockRevokerRights(OFFICE, projId, appId, USER_ID));
            lockDao.updateLockRevokerRights(OFFICE, projId, appId, USER_ID, true);
            assertTrue(lockDao.hasLockRevokerRights(OFFICE, projId, appId, USER_ID));
            lockDao.updateLockRevokerRights(OFFICE, projId, appId, USER_ID, false);
            assertFalse(lockDao.hasLockRevokerRights(OFFICE, projId, appId, USER_ID));

        } finally {
            lockDao.removeAllLockRevokerRights(OFFICE, appId, USER_ID);
            deleteProject(prjDao, projId, lockDao, appId);
        }
    }

    @Test
    void test_can_update_rights_for_project_that_dont_exist() throws SQLException {
        DSLContext dsl = getDslContext(OFFICE);
        ProjectLockDao lockDao = new ProjectLockDao(dsl);

        String projId = "AAnoProj";
        String appId = "dont_exist";
        try {
            // verify that this doesn't throw an exception
            lockDao.updateLockRevokerRights(OFFICE, projId, appId, USER_ID, true);
            lockDao.updateLockRevokerRights(OFFICE, projId, appId, USER_ID, false);

            List<LockRevokerRights> lockRevokerRights = lockDao.catLockRevokerRights(OFFICE, projId, appId);
            assertNotNull(lockRevokerRights);
            assertTrue(lockRevokerRights.isEmpty());
        } finally {
            lockDao.removeAllLockRevokerRights(OFFICE, appId, USER_ID);
        }
    }

    @Test
    void test_cat_lock_revoke_rights() throws SQLException {
        DSLContext dsl = getDslContext(OFFICE);
        ProjectLockDao lockDao = new ProjectLockDao(dsl);
        ProjectDao prjDao = new ProjectDao(dsl);

        String projId = "catRights";
        String appId = "test_catRights";
        String officeMask = OFFICE;

        Project testProject = buildTestProject(OFFICE, projId);
        prjDao.create(testProject, true);

        try {
            lockDao.removeAllLockRevokerRights(OFFICE, appId, USER_ID); // start fresh

            lockDao.updateLockRevokerRights(OFFICE, projId, appId, USER_ID, true);

            List<LockRevokerRights> lockRevokerRights = lockDao.catLockRevokerRights(officeMask, projId, appId);
            assertNotNull(lockRevokerRights);
            assertFalse(lockRevokerRights.isEmpty());

            // Now deny
            lockDao.updateLockRevokerRights(OFFICE, projId, appId, USER_ID, false);
            lockRevokerRights = lockDao.catLockRevokerRights(officeMask, projId, appId);
            assertNotNull(lockRevokerRights);
            assertTrue(lockRevokerRights.isEmpty());


        } finally {
            lockDao.removeAllLockRevokerRights(OFFICE, appId, USER_ID);
            deleteProject(prjDao, projId, lockDao, appId);
        }
    }

        @Test
    void test_cannot_get_off_deny_list() throws SQLException {

        // There is an allow list (allow = t) and a deny list (allow = f)
        // The allow list is checked first then the deny list is checked
        // If the user is on the deny list, they cannot revoke a lock
        // doesn't matter if they are on the allow list.
        // Note: the only way to get off the deny list is to deny revoke rights for all projects

        DSLContext dsl = getDslContext(OFFICE);
        ProjectDao prjDao = new ProjectDao(dsl);
        ProjectLockDao lockDao = new ProjectLockDao(dsl);

        String projId = "canUnset";
        String appId = "revoke_test";
        Project testProject = buildTestProject(OFFICE, projId);
        prjDao.create(testProject, true);

        try {
            int revokeTimeout = 10;
            boolean revokeExisting = false;

            ProjectLock req1 = new ProjectLock.Builder(OFFICE, projId, appId).build();
            String lockId = lockDao.requestLock(req1, revokeExisting, revokeTimeout);
            try {
                assertNotNull(lockId);
                assertTrue(lockId.length() > 8);

                try {
                    lockDao.revokeLock(OFFICE, projId, appId, 0);  // delete/kill.
                    fail("Should have thrown an exception");
                } catch (Exception e) {
                    logger.at(Level.INFO).log("Expected exception: %s", e.getMessage());
                }

                lockDao.updateLockRevokerRights(OFFICE, projId, appId, USER_ID, true);
                // Normally we'd be able to revoke the lock now, but we want to get on the deny list too
                lockDao.updateLockRevokerRights(OFFICE, projId, appId, USER_ID, false);
                // Now on the allow AND deny list.

                try {
                    lockDao.revokeLock(OFFICE, projId, appId, 0);
                    fail("Should have thrown an exception");
                } catch (Exception e) {
                    //expected
                    logger.at(Level.INFO).log("Expected exception: %s", e.getMessage());
                }

                lockDao.updateLockRevokerRights(OFFICE, projId, appId, USER_ID, true);
                // still expect this to fail
                try {
                    lockDao.revokeLock(OFFICE, projId, appId, 0);
                    fail("Should have thrown an exception");
                } catch (Exception e) {
                    //expected
                    logger.at(Level.INFO).log("Expected exception: %s", e.getMessage());
                }

            } finally {
                // cleanup
                lockDao.removeAllLockRevokerRights(OFFICE, appId, USER_ID);
                lockDao.updateLockRevokerRights(OFFICE, projId, appId, USER_ID, true);
                lockDao.revokeLock(OFFICE, projId, appId, 0);
            }
        } finally {
            lockDao.removeAllLockRevokerRights(OFFICE, appId, USER_ID);
            deleteProject(prjDao, projId, lockDao, appId);
        }

    }

    private void deleteProject(ProjectDao prjDao, String projId, ProjectLockDao lockDao, String appId) {
        try {
            prjDao.delete(OFFICE, projId, DeleteRule.DELETE_ALL);
        } catch (Exception e) {
            logger.at(Level.WARNING).withCause(e).log("Failed to delete project: %s", projId);
            List<ProjectLock> locks = lockDao.retrieveLocks(OFFICE, projId, appId);
            locks.forEach(lock -> {
                logger.atFine().log("Remaining Locks: %s %s %s %s %s %s %s",
                       lock.getProjectId(), lock.getApplicationId(), lock.getAcquireTime(),
                       lock.getSessionUser(), lock.getOsUser(), lock.getSessionProgram(), lock.getSessionMachine());
            });
        }
    }

    @Test
    void test_isLocked() throws SQLException {

        DSLContext dsl = getDslContext(OFFICE);
        ProjectDao prjDao = new ProjectDao(dsl);
        ProjectLockDao lockDao = new ProjectLockDao(dsl);

        String projId = "isLockd";
        String appId = "isLocked_test";
        Project testProject = buildTestProject(OFFICE, projId);
        prjDao.create(testProject, true);
        String lockId;
        try {
            int revokeTimeout = 10;
            boolean revokeExisting = true;

            lockDao.updateLockRevokerRights(OFFICE, projId, appId, USER_ID, true);
            ProjectLock req1 = new ProjectLock.Builder(OFFICE, projId, appId).build();
            lockId = lockDao.requestLock(req1, revokeExisting, revokeTimeout);

            assertNotNull(lockId);
            assertTrue(lockId.length() > 8);  // FYI its 32 hex chars

            boolean locked = lockDao.isLocked(OFFICE, projId, appId);
            assertTrue(locked);

            lockDao.releaseLock(OFFICE, lockId);  // throws if ther isn't a current lock
            locked = lockDao.isLocked(OFFICE, projId, appId);
            assertFalse(locked);

        } finally {

            try {
                lockDao.revokeLock(OFFICE, projId, appId, 0);  // delete/kill.
            } catch (Exception e) {
                logger.at(Level.WARNING).withCause(e).log("Failed to revoke lock: %s", appId);
            }
            deleteProject(prjDao, projId, lockDao, appId);
        }
    }


    @Test
    void test_retrieveLocks() throws SQLException {

        DSLContext dsl = getDslContext(OFFICE);
        ProjectDao prjDao = new ProjectDao(dsl);
        ProjectLockDao lockDao = new ProjectLockDao(dsl);

        String projId = "catLocks";
        String projId1 = "catLocks1";
        String projId2 = "catLocks2";
        String appId = "catLocks_test";

        Project testProject = buildTestProject(OFFICE, projId1);
        prjDao.create(testProject);
        Project testProject2 = buildTestProject(OFFICE, projId2);
        prjDao.create(testProject2);
        try {
            int revokeTimeout = 10;

            lockDao.updateLockRevokerRights(OFFICE, projId1, appId, USER_ID, true);
            lockDao.updateLockRevokerRights(OFFICE, projId2, appId, USER_ID, true);

            ProjectLock req1 = new ProjectLock.Builder(OFFICE, projId1, appId).build();
            String lock1 = lockDao.requestLock(req1, true, revokeTimeout);
            assertTrue(lock1.length() > 8);

            ProjectLock req2 = new ProjectLock.Builder(OFFICE, projId2, appId).build();
            String lock2 = lockDao.requestLock(req2, false, revokeTimeout);
            assertTrue(lock2.length() > 8);
            assertNotEquals(lock1, lock2);

            List<ProjectLock> locks = lockDao.retrieveLocks(OFFICE, projId + "*", appId);
            assertNotNull(locks);
            assertFalse(locks.isEmpty());

            lockDao.releaseLock(OFFICE, lock1);
            lockDao.releaseLock(OFFICE, lock2);

            lockDao.updateLockRevokerRights(OFFICE, projId1, appId, USER_ID, true);
            lockDao.updateLockRevokerRights(OFFICE, projId2, appId, USER_ID, true);

        } finally {
            try {
                lockDao.revokeLock(OFFICE, projId1, appId, 0);
            } catch (Exception e) {
                logger.at(Level.WARNING).withCause(e).log("Failed to revoke lock: %s", projId1);
            }
            try {
                 lockDao.revokeLock(OFFICE, projId2, appId, 0);
            } catch (Exception e) {
                logger.at(Level.WARNING).withCause(e).log("Failed to revoke lock: %s", projId2);
            }

            deleteProject(prjDao, projId1, lockDao, appId);
            deleteProject(prjDao, projId2, lockDao, appId);
        }
    }


    private static Project buildTestProject(String office, String prjId) {
        Location pbLoc = new Location.Builder(office,prjId + "-PB")
                .withTimeZoneName(ZoneId.of("UTC"))
                .withActive(null)
                .build();
        Location ngLoc = new Location.Builder(office,prjId + "-NG")
                .withTimeZoneName(ZoneId.of("UTC"))
                .withActive(null)
                .build();

        Location prjLoc = new Location.Builder(office, prjId)
                .withTimeZoneName(ZoneId.of("UTC"))
                .withActive(null)
                .build();

        return new Project.Builder()
                .withLocation(prjLoc)
                .withProjectOwner("Project Owner")
                .withAuthorizingLaw("Authorizing Law")
                .withFederalCost(BigDecimal.valueOf(100.0))
                .withNonFederalCost(BigDecimal.valueOf(50.0))
                .withFederalOAndMCost(BigDecimal.valueOf(10.0))
                .withNonFederalOAndMCost(BigDecimal.valueOf(5.0))
                .withCostYear(Instant.now())
                .withCostUnit("$")
                .withYieldTimeFrameEnd(Instant.now())
                .withYieldTimeFrameStart(Instant.now())
                .withFederalOAndMCost(BigDecimal.valueOf(10.0))
                .withNonFederalOAndMCost(BigDecimal.valueOf(5.0))
                .withProjectRemarks("Remarks")
                .withPumpBackLocation(pbLoc)
                .withNearGageLocation(ngLoc)
                .withBankFullCapacityDesc("Bank Full Capacity Description")
                .withDownstreamUrbanDesc("Downstream Urban Description")
                .withHydropowerDesc("Hydropower Description")
                .withSedimentationDesc("Sedimentation Description")
                .build();

    }


}