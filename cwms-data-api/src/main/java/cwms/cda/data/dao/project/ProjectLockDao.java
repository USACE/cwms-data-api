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

import static cwms.cda.data.dao.project.ProjectDao.toBigInteger;

import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dto.project.Lock;
import cwms.cda.data.dto.project.LockRevokerRights;
import java.math.BigInteger;
import java.util.List;
import java.util.TimeZone;
import java.util.logging.Logger;
import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import usace.cwms.db.dao.util.OracleTypeMap;
import usace.cwms.db.jooq.codegen.packages.CWMS_PROJECT_PACKAGE;

public class ProjectLockDao extends JooqDao<Lock> {
    private static final Logger logger = Logger.getLogger(ProjectLockDao.class.getName());
    public static final String OFFICE_ID = "OFFICE_ID";
    public static final String PROJECT_ID = "PROJECT_ID";
    public static final String APPLICATION_ID = "APPLICATION_ID";
    public static final String ACQUIRE_TIME = "ACQUIRE_TIME";
    public static final String SESSION_USER = "SESSION_USER";
    public static final String OS_USER = "OS_USER";
    public static final String SESSION_PROGRAM = "SESSION_PROGRAM";
    public static final String SESSION_MACHINE = "SESSION_MACHINE";
    public static final String USER_ID = "USER_ID";

    public ProjectLockDao(DSLContext dsl) {
        super(dsl);
    }

    public String requestLock(String office, String projectId, String appId,
                              boolean revokeExisting, int revokeTimeout) {
        BigInteger revokeTimeoutBI = toBigInteger(revokeTimeout);
        String pRevokeExisting = OracleTypeMap.formatBool(revokeExisting);
        return connectionResult(dsl,
                c -> CWMS_PROJECT_PACKAGE.call_REQUEST_LOCK(getDslContext(c, office).configuration(),
                projectId, appId, pRevokeExisting, revokeTimeoutBI, office)
        );
    }

    public boolean isLocked(String office, String projectId, String appId) {
        String s = connectionResult(dsl,
                c -> CWMS_PROJECT_PACKAGE.call_IS_LOCKED(getDslContext(c, office).configuration(),
                projectId, appId, office));
        return OracleTypeMap.parseBool(s);
    }


    public List<Lock> catLocks(String projMask, String appMask, TimeZone tz, String officeMask) {
        return CWMS_PROJECT_PACKAGE.call_CAT_LOCKS(dsl.configuration(),
                projMask, appMask, tz.getID(), officeMask)
                .map(ProjectLockDao::buildLockFromCatLocksRecord);
    }

    private static @NotNull Lock buildLockFromCatLocksRecord(Record catRecord) {
        String officeId = catRecord.getValue(OFFICE_ID, String.class);
        String projectId = catRecord.getValue(PROJECT_ID, String.class);
        String applicationId = catRecord.getValue(APPLICATION_ID, String.class);

        return new Lock.Builder(officeId, projectId, applicationId)
                .withAcquireTime(catRecord.getValue(ACQUIRE_TIME, String.class))
                .withSessionUser(catRecord.getValue(SESSION_USER, String.class))
                .withOsUser(catRecord.getValue(OS_USER, String.class))
                .withSessionProgram(catRecord.getValue(SESSION_PROGRAM, String.class))
                .withSessionMachine(catRecord.getValue(SESSION_MACHINE, String.class))
                .build()
                ;
    }

    public void releaseLock(String lockId) {
        connection(dsl, c -> CWMS_PROJECT_PACKAGE.call_RELEASE_LOCK(
                DSL.using(c, SQLDialect.ORACLE18C).configuration(),
                lockId));
    }

    public void revokeLock(String office, String projId, String appId,
                           int revokeTimeout) {
        BigInteger revokeTimeoutBI = toBigInteger(revokeTimeout);
        connection(dsl, c ->
                CWMS_PROJECT_PACKAGE.call_REVOKE_LOCK(getDslContext(c, office).configuration(),
                        projId, appId, revokeTimeoutBI, office));
    }


    public void denyLockRevocation(String lockId) {
        connection(dsl, c ->
                CWMS_PROJECT_PACKAGE.call_DENY_LOCK_REVOCATION(
                        DSL.using(c, SQLDialect.ORACLE18C).configuration(),
                        lockId));
    }

    /**
     * Either adds the user to allow list or the deny list
     * @param office the office to use in the session
     * @param userId the user to add
     * @param projectMask the project mask
     * @param applicationMask the application mask
     * @param officeMask the office mask
     * @param allow true to add to allow list, false to add to deny list
     */
    public void updateLockRevokerRights(String office, String userId, String projectMask,
                                        String applicationMask, String officeMask, boolean allow) {
        String pAllow = OracleTypeMap.formatBool(allow);

        connection(dsl, c -> {
            DSLContext context = getDslContext(c, office);
            CWMS_PROJECT_PACKAGE.call_UPDATE_LOCK_REVOKER_RIGHTS(
                        context.configuration(),
                userId, projectMask, pAllow, applicationMask, officeMask);
                }
        );
    }


    public void allowLockRevokerRights(String office, String userId, String projectMask,
                                        String applicationMask, String officeMask) {
        updateLockRevokerRights(office, userId, projectMask, applicationMask, officeMask, true);
    }

    public void denyLockRevokerRights(String office,String userId, String projectMask,
                                       String applicationMask, String officeMask) {
        updateLockRevokerRights(office,  userId, projectMask, applicationMask, officeMask, false);
    }

    /**
     * This method is a convenience method for the special case where project_ids='*" and p_allow = 'F'.
     * The pl/sql removes from the allow and deny list in this special case.
     *
     * @param userId the user for which to remove all rights
     * @param applicationMask the application mask
     * @param officeMask the office mask
     */
    public void removeAllLockRevokerRights(String office, String userId, String applicationMask, String officeMask) {

        updateLockRevokerRights(office, userId, "*", applicationMask, officeMask, false);
    }


    public List<LockRevokerRights> catLockRevokerRights(String projectMask,
                                                        String applicationMask, String officeMask) {
        return connectionResult(dsl,
                c -> CWMS_PROJECT_PACKAGE.call_CAT_LOCK_REVOKER_RIGHTS(
                        DSL.using(c, SQLDialect.ORACLE18C).configuration(),
                        projectMask, applicationMask, officeMask)
                        .map(ProjectLockDao::buildLockRevokerRightsFromCatRightsRecord));
    }

    private static @NotNull LockRevokerRights buildLockRevokerRightsFromCatRightsRecord(Record r) {
        String officeId = r.getValue(OFFICE_ID, String.class);
        String projectId = r.getValue(PROJECT_ID, String.class);
        String applicationId = r.getValue(APPLICATION_ID, String.class);
        String userId = r.getValue(USER_ID, String.class);

        return new LockRevokerRights.Builder(officeId, projectId, applicationId, userId).build();
    }

    public boolean hasLockRevokerRights(String office, String userId, String projectId, String applicationId) {

        String s = connectionResult(dsl,
                c -> CWMS_PROJECT_PACKAGE.call_HAS_REVOKER_RIGHTS(getDslContext(c, office).configuration(),
                projectId, applicationId, userId, office, null));
        return OracleTypeMap.parseBool(s);
    }

}
