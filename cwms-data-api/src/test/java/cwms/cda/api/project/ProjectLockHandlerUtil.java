/*
 * MIT License
 *
 * Copyright (c) 2026 Hydrologic Engineering Center
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
import cwms.cda.data.dao.project.ProjectLockDaoV1;
import cwms.cda.data.dao.project.ProjectLockDaoV2;
import cwms.cda.data.dto.CwmsId;
import cwms.cda.data.dto.Location;
import cwms.cda.data.dto.project.Project;
import cwms.cda.data.dto.project.ProjectLock;
import cwms.cda.data.dto.project.ProjectLockV1;
import cwms.cda.data.dto.project.ProjectLockV2;
import cwms.cda.formatters.Formats;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.jooq.DSLContext;
import org.junit.jupiter.params.provider.Arguments;

public class ProjectLockHandlerUtil {
    public static final String METHOD_SOURCE = "cwms.cda.api.project.ProjectLockHandlerUtil#fixturesAndFormats";
    private static final FluentLogger logger = FluentLogger.forEnclosingClass();

    public static Stream<Fixture<? extends ProjectLock>> fixtures() {
        return Stream.of(V1, V2);
    }

    public static Stream<Arguments> fixturesAndFormats() {
        return fixtures()
                .flatMap(fixture -> Stream.of(Formats.JSON, Formats.DEFAULT)
                        .map(format -> Arguments.of(fixture, format)));
    }

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
            ProjectLockDaoV1 lockDao = new ProjectLockDaoV1(dsl);
            logger.atWarning().withCause(e).log("Failed to delete project: %s", projId);
            List<ProjectLockV1> locks = lockDao.retrieveLocks(office, projId, appId);
            locks.forEach(lock -> {
                logger.atFine().log("Remaining Locks: " + lock.getProjectId() + " " +
                        lock.getApplicationId() + " " + lock.getAcquireTime() + " " +
                        lock.getSessionUser() + " " + lock.getOsUser() + " " +
                        lock.getSessionProgram() + " " + lock.getSessionMachine());
            });
        }
    }

    public static void revokeLock(DSLContext dsl, String office, String projId, String appId) {
        ProjectLockDaoV1 lockDao = new ProjectLockDaoV1(dsl);
        try {
            lockDao.revokeLock(office, projId, appId, 0);
        } catch (Exception e) {
            // don't care
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
            ProjectLockDaoV1 lockDao = new ProjectLockDaoV1(dsl);
            try {
                lockDao.releaseLock(office, lockId);
            } catch (Exception e) {
                // don't care
            }
        }
    }

    public interface Fixture<T extends ProjectLock> {
        ProjectLockDao<T> newDao(DSLContext dsl);

        T minimal(String office, String name, String applicationId);

        T full(String office, String name, String applicationId, String osUser, String sessionProgram,
               String sessionMachine, String sessionUser);

        String requestPath(String office);

        String catalogPath(String office);

        String getOnePath(String office, String name);

        String releasePath(String office);

        String denyPath(String office);

        boolean officeAsQueryParam();

        String officeOf(Map<String, ?> lockJson);

        String nameOf(Map<String, ?> lockJson);
    }

    public static final Fixture<ProjectLockV1> V1 = new Fixture<>() {
        @Override
        public ProjectLockDaoV1 newDao(DSLContext dsl) {
            return new ProjectLockDaoV1(dsl);
        }

        @Override
        public ProjectLockV1 minimal(String office, String name, String applicationId) {
            return new ProjectLockV1.Builder(office, name, applicationId).build();
        }

        @Override
        public ProjectLockV1 full(String office, String name, String applicationId, String osUser,
                                  String sessionProgram, String sessionMachine, String sessionUser) {
            return new ProjectLockV1.Builder()
                    .withOfficeId(office)
                    .withProjectId(name)
                    .withApplicationId(applicationId)
                    .withOsUser(osUser)
                    .withSessionProgram(sessionProgram)
                    .withSessionMachine(sessionMachine)
                    .withSessionUser(sessionUser)
                    .build();
        }

        @Override
        public String requestPath(String office) {
            return "/project-locks/";
        }

        @Override
        public String catalogPath(String office) {
            return "/project-locks/";
        }

        @Override
        public String getOnePath(String office, String name) {
            return "/project-locks/" + name;
        }

        @Override
        public String releasePath(String office) {
            return "/project-locks/release";
        }

        @Override
        public String denyPath(String office) {
            return "/project-locks/deny";
        }

        @Override
        public boolean officeAsQueryParam() {
            return true;
        }

        @Override
        public String officeOf(Map<String, ?> lockJson) {
            return (String) lockJson.get("office-id");
        }

        @Override
        public String nameOf(Map<String, ?> lockJson) {
            return (String) lockJson.get("project-id");
        }

        @Override
        public String toString() {
            return "ProjectLockV1";
        }
    };

    public static final Fixture<ProjectLockV2> V2 = new Fixture<>() {
        @Override
        public ProjectLockDaoV2 newDao(DSLContext dsl) {
            return new ProjectLockDaoV2(dsl);
        }

        @Override
        public ProjectLockV2 minimal(String office, String name, String applicationId) {
            return new ProjectLockV2.Builder(CwmsId.buildCwmsId(office, name), applicationId).build();
        }

        @Override
        public ProjectLockV2 full(String office, String name, String applicationId, String osUser,
                                  String sessionProgram, String sessionMachine, String sessionUser) {
            return new ProjectLockV2.Builder(CwmsId.buildCwmsId(office, name), applicationId)
                    .withOsUser(osUser)
                    .withSessionProgram(sessionProgram)
                    .withSessionMachine(sessionMachine)
                    .withSessionUser(sessionUser)
                    .build();
        }

        @Override
        public String requestPath(String office) {
            return "/v2/project-locks/" + office + "/";
        }

        @Override
        public String catalogPath(String office) {
            return "/v2/project-locks/" + office + "/";
        }

        @Override
        public String getOnePath(String office, String name) {
            return "/v2/project-locks/" + office + "/" + name;
        }

        @Override
        public String releasePath(String office) {
            return "/v2/project-locks/" + office + "/release";
        }

        @Override
        public String denyPath(String office) {
            return "/v2/project-locks/" + office + "/deny";
        }

        @Override
        public boolean officeAsQueryParam() {
            return false;
        }

        @Override
        public String officeOf(Map<String, ?> lockJson) {
            Object idObj = lockJson.get("id");
            if (!(idObj instanceof Map)) {
                return null;
            }
            return (String) ((Map<?, ?>) idObj).get("office-id");
        }

        @Override
        public String nameOf(Map<String, ?> lockJson) {
            Object idObj = lockJson.get("id");
            if (!(idObj instanceof Map)) {
                return null;
            }
            return (String) ((Map<?, ?>) idObj).get("name");
        }

        @Override
        public String toString() {
            return "ProjectLockV2";
        }
    };


}
