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

package cwms.cda.api.project;

import com.google.common.flogger.FluentLogger;
import cwms.cda.data.dao.DeleteRule;
import cwms.cda.data.dao.project.ProjectDao;
import cwms.cda.data.dao.project.ProjectLockDao;
import cwms.cda.data.dto.Location;
import cwms.cda.data.dto.project.Project;
import cwms.cda.data.dto.project.ProjectLock;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.logging.Level;
import org.jooq.DSLContext;

public class ProjectLockHandlerUtil {
    private static final FluentLogger logger = FluentLogger.forEnclosingClass();

    public static Project buildTestProject(String office, String prjId) {
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


    public static void deleteProject(DSLContext dsl, String projId, String office, String appId) {
        ProjectDao prjDao = new ProjectDao(dsl);
        try {
            prjDao.delete(office, projId, DeleteRule.DELETE_ALL);
        } catch (Exception e) {
            ProjectLockDao lockDao = new ProjectLockDao(dsl);
            logger.at(Level.WARNING).withCause(e).log("Failed to delete project: %s", projId);
            List<ProjectLock> locks = lockDao.retrieveLocks(office, projId, appId);
            locks.forEach(lock -> {
                logger.atFine().log("Remaining Locks: " + lock.getProjectId() + " " +
                        lock.getApplicationId() + " " + lock.getAcquireTime() + " " +
                        lock.getSessionUser() + " " + lock.getOsUser() + " " +
                        lock.getSessionProgram() + " " + lock.getSessionMachine());
            });
        }
    }

    public static void revokeLock(DSLContext dsl, String office, String projId, String appId) {
        ProjectLockDao lockDao = new ProjectLockDao(dsl);
        try {
            lockDao.revokeLock(office, projId, appId, 0);
        } catch (Exception e) {
            logger.at(Level.WARNING).withCause(e).log("Failed to revoke lock: %s", appId);
        }
    }

    public static void releaseLock(DSLContext dsl, String office, String[] lockId2) {
        if(lockId2 != null ) {
            for (String lockId : lockId2) {
                releaseLock(dsl, office, lockId);
            }
        }

    }
    public static void releaseLock(DSLContext dsl,  String office, String lockId) {
        if (lockId != null) {
            ProjectLockDao lockDao = new ProjectLockDao(dsl);
            try {
                lockDao.releaseLock(office, lockId);
            } catch (Exception e) {
                // don't care
            }
        }
    }


}
