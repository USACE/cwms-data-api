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

import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dto.project.LockRevokerRights;
import cwms.cda.data.dto.project.ProjectLock;
import java.math.BigInteger;
import java.sql.CallableStatement;
import java.time.Instant;
import java.util.List;
import org.jetbrains.annotations.NotNull;
import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.SQLDialect;
import org.jooq.exception.TooManyRowsException;
import org.jooq.impl.DSL;
import usace.cwms.db.dao.util.OracleTypeMap;
import usace.cwms.db.jooq.codegen.packages.CWMS_PROJECT_PACKAGE;

public class ProjectLockDao extends JooqDao<ProjectLock> {
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

    /**
     * Requests a lock for a project.
     *
     * @param request        The ProjectLock object representing the lock request
     * @param revokeExisting True if existing locks should be revoked, false otherwise
     * @param revokeTimeout  The time in seconds to wait for existing locks to be revoked
     * @return The lockId if the request was successful, null otherwise
     */
    public String requestLock(ProjectLock request, boolean revokeExisting, int revokeTimeout) {

        String office = request.getOfficeId();
        return connectionResult(dsl, c -> {
            setOffice(c, office);
            int i = 1;
            try (CallableStatement stmt = c.prepareCall("{? = call CWMS_PROJECT.REQUEST_LOCK(?,?,?,?,?,?,?,?,?)}")) {
                stmt.registerOutParameter(i++, java.sql.Types.VARCHAR);
                stmt.setString(i++, request.getProjectId());
                stmt.setString(i++, request.getApplicationId());
                stmt.setString(i++, OracleTypeMap.formatBool(revokeExisting));
                stmt.setObject(i++, BigInteger.valueOf(revokeTimeout));
                stmt.setString(i++, office);
                stmt.setString(i++, request.getSessionUser());
                stmt.setString(i++, request.getOsUser());
                stmt.setString(i++, request.getSessionProgram());
                stmt.setString(i++, request.getSessionMachine());
                stmt.execute();
                return stmt.getString(1);
            }
        });
    }


    /**
     * Determines if a project lock is active for the given parameters.
     *
     * @param office    the office associated with the lock
     * @param projectId the ID of the project
     * @param appId     the ID of the application
     * @return true if a lock is active, false otherwise
     */
    public boolean isLocked(String office, String projectId, String appId) {
        String s = connectionResult(dsl, c -> {
                    Configuration conf = getDslContext(c, office).configuration();
                    return CWMS_PROJECT_PACKAGE.call_IS_LOCKED(conf,
                    projectId, appId, office);
                });
        return OracleTypeMap.parseBool(s);
    }

    /**
     * Returns the list of project locks based on the given office mask, project mask, and application mask.
     *
     * @param officeMask the office mask
     * @param projMask the project mask
     * @param appMask the application mask
     * @return the list of project locks
     */
    public List<ProjectLock> retrieveLocks(String officeMask, String projMask, String appMask) {
        return CWMS_PROJECT_PACKAGE.call_CAT_LOCKS(dsl.configuration(),
                        projMask, appMask, "UTC", officeMask)
                .map(ProjectLockDao::buildLockFromCatLocksRecord);
    }

    /**
     * Returns the requested lock based on given office, project, and application.
     *
     * @param office the office
     * @param projectName the project
     * @param applicationName the application
     * @return the matching project lock or null
     * @throws TooManyRowsException if the provided arguments match more then one row.
     */
    public ProjectLock retrieveLock(String office, String projectName, String applicationName) {
        ProjectLock retval = null;

        List<ProjectLock> locks = CWMS_PROJECT_PACKAGE.call_CAT_LOCKS(dsl.configuration(),
                        projectName, applicationName, "UTC", office)
                .map(ProjectLockDao::buildLockFromCatLocksRecord);
        if (locks.size() > 1) {
            throw new TooManyRowsException("Provided arguments matched " + locks.size() + " rows");
        } else if (locks.size() == 1) {
            retval = locks.get(0);
        }

        return retval;
    }

    private static @NotNull ProjectLock buildLockFromCatLocksRecord(Record catRecord) {
        String officeId = catRecord.getValue(OFFICE_ID, String.class);
        String projectId = catRecord.getValue(PROJECT_ID, String.class);
        String applicationId = catRecord.getValue(APPLICATION_ID, String.class);

        String acquireStr = catRecord.getValue(ACQUIRE_TIME, String.class);
        Instant acquireTime = acquireStr != null ? Instant.parse(acquireStr) : null;

        return new ProjectLock.Builder(officeId, projectId, applicationId)
                .withAcquireTime(acquireTime)
                .withSessionUser(catRecord.getValue(SESSION_USER, String.class))
                .withOsUser(catRecord.getValue(OS_USER, String.class))
                .withSessionProgram(catRecord.getValue(SESSION_PROGRAM, String.class))
                .withSessionMachine(catRecord.getValue(SESSION_MACHINE, String.class))
                .build()
                ;
    }

    /**
     * Releases a lock for a given office and lock ID.
     *
     * @param sessionOffice  the office to be used for the session.
     * @param lockId  the ID of the lock to release
     */
    public void releaseLock(String sessionOffice, String lockId) {
        connection(dsl, c -> {
            Configuration conf = getDslContext(c, sessionOffice).configuration();
            CWMS_PROJECT_PACKAGE.call_RELEASE_LOCK(conf, lockId);
        });
    }

    /**
     * Revokes a project lock, if successful the lock is deleted.
     *
     * @param office       the office associated with the lock
     * @param projId       the ID of the project
     * @param appId        the ID of the application
     * @param revokeTimeout the time in seconds to wait for existing lock to be revoked
     */
    public void revokeLock(String office, String projId, String appId,
                           int revokeTimeout) {
        BigInteger revokeTimeoutBI = BigInteger.valueOf(revokeTimeout);
        connection(dsl, c -> {
            Configuration conf = getDslContext(c, office).configuration();
            CWMS_PROJECT_PACKAGE.call_REVOKE_LOCK(conf,
                    projId, appId, revokeTimeoutBI, office);
        });
    }


    /**
     * Denies the revocation of a lock.
     *
     * @param lockId the ID of the lock to deny revocation for
     */
    public void denyLockRevocation(String lockId) {
        connection(dsl, c -> {
            Configuration conf = DSL.using(c, SQLDialect.ORACLE18C).configuration();
            CWMS_PROJECT_PACKAGE.call_DENY_LOCK_REVOCATION(conf, lockId);
        });
    }

    /**
     * Either adds the user to allow list or the deny list.
     *
     * @param sessionOffice          the office to use in the session
     * @param projectMask     the project mask
     * @param applicationId the application id
     * @param userId          the user to add
     * @param allow           true to add to allow list, false to add to deny list
     */
    public void updateLockRevokerRights(String sessionOffice, String projectMask,
                                        String applicationId, String userId, boolean allow) {

        connection(dsl, c -> {
            Configuration conf = getDslContext(c, sessionOffice).configuration();
            CWMS_PROJECT_PACKAGE.call_UPDATE_LOCK_REVOKER_RIGHTS(conf,
                userId, projectMask, OracleTypeMap.formatBool(allow), applicationId, null);
        });
    }

    /**
     * Adds a user to the LockRevokeRights allow list. Equivalent to calling updateLockRevokerRights with allow=true.
     *
     * @param sessionOffice          the office to use in the session
     * @param projectMask     the project mask
     * @param applicationId  the application id
     * @param userId          the user to add
     */
    public void allowLockRevokerRights(String sessionOffice, String projectMask,
                                       String applicationId, String userId) {
        updateLockRevokerRights(sessionOffice, projectMask, applicationId, userId, true);
    }

    /**
     * Adds a user to the LockRevokeRights deny list. Equivalent to calling updateLockRevokerRights with allow=false.
     *
     * @param sessionOffice          the office to use in the session
     * @param projectMask     the project mask
     * @param applicationId   the application id
     * @param userId          the user to add
     */
    public void denyLockRevokerRights(String sessionOffice, String projectMask,
                                      String applicationId, String userId) {
        updateLockRevokerRights(sessionOffice, projectMask, applicationId, userId, false);
    }

    /**
     * Removes a user from the allow and deny list.  Equivalent to calling updateLockRevokerRights
     * with project_ids='*" and p_allow = 'F'.
     * The pl/sql treats those two option in combination as a special case and removes from the
     * allow and deny list.
     *
     * @param sessionOffice          the office to use in the session
     * @param applicationId   the application id
     * @param userId          the user for which to remove all rights
     */
    public void removeAllLockRevokerRights(String sessionOffice, String applicationId, String userId) {

        updateLockRevokerRights(sessionOffice, "*", applicationId, userId, false);
    }

    /**
     * Retrieves a list of LockRevokerRights based on the given office, project, and application masks.
     *
     * @param officeMask     the office mask
     * @param projectMask    the project mask
     * @param applicationMask the application mask
     * @return the list of LockRevokerRights
     */
    public List<LockRevokerRights> catLockRevokerRights(String officeMask, String projectMask, String applicationMask) {
        return connectionResult(dsl, c -> {
                    Configuration conf = DSL.using(c, SQLDialect.ORACLE18C).configuration();
                    return CWMS_PROJECT_PACKAGE.call_CAT_LOCK_REVOKER_RIGHTS(conf,
                            projectMask, applicationMask, officeMask)
                            .map(ProjectLockDao::buildLockRevokerRightsFromCatRightsRecord);
                });
    }

    private static @NotNull LockRevokerRights buildLockRevokerRightsFromCatRightsRecord(Record r) {
        String officeId = r.getValue(OFFICE_ID, String.class);
        String projectId = r.getValue(PROJECT_ID, String.class);
        String applicationId = r.getValue(APPLICATION_ID, String.class);
        String userId = r.getValue(USER_ID, String.class);

        return new LockRevokerRights.Builder(officeId, projectId, applicationId, userId).build();
    }

    /**
     * Determines if the specified user has lock revoker rights for a project.
     *
     * @param office        the office associated with the lock
     * @param projectId    the ID of the project
     * @param applicationId the ID of the application
     * @param userId        the ID of the user
     * @return true if the user has lock revoker rights, false otherwise
     */
    public boolean hasLockRevokerRights(String office, String projectId, String applicationId, String userId) {

        String s = connectionResult(dsl, c -> {
                    Configuration conf = getDslContext(c, office).configuration();
                    return CWMS_PROJECT_PACKAGE.call_HAS_REVOKER_RIGHTS(conf,
                    projectId, applicationId, userId, office, null);
                });
        return OracleTypeMap.parseBool(s);
    }

}
