package cwms.cda.data.dao.project;

import static cwms.cda.data.dao.project.ProjectDao.toBigInteger;

import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dto.project.Lock;
import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;
import java.util.logging.Logger;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import usace.cwms.db.dao.util.OracleTypeMap;
import usace.cwms.db.jooq.codegen.packages.CWMS_PROJECT_PACKAGE;

public class ProjectLockDao extends JooqDao<Lock> {
    private static final Logger logger = Logger.getLogger(ProjectLockDao.class.getName());

    protected ProjectLockDao(DSLContext dsl) {
        super(dsl);
    }

    public String requestLock(String projectId, String appId,
                              boolean revokeExisting, int revokeTimeout, String office) {
        BigInteger revokeTimeoutBI = toBigInteger(revokeTimeout);
        return connectionResult(dsl,
                c -> CWMS_PROJECT_PACKAGE.call_REQUEST_LOCK(getDslContext(c, office).configuration(),
                projectId, appId, OracleTypeMap.formatBool(revokeExisting), revokeTimeoutBI,
                office));
    }

    public boolean isLocked(String projectId, String appId, String office) {
        String s = connectionResult(dsl,
                c -> CWMS_PROJECT_PACKAGE.call_IS_LOCKED(getDslContext(c, office).configuration(),
                projectId, appId, office));
        return OracleTypeMap.parseBool(s);
    }


    public List<Lock> catLocks(String projMask, String appMask, TimeZone tz, String officeMask) throws SQLException {
        List<Lock> retval = new ArrayList<>();
        try (ResultSet resultSet = CWMS_PROJECT_PACKAGE.call_CAT_LOCKS(dsl.configuration(),
                        projMask, appMask, tz.getID(), officeMask)
                .intoResultSet()) {
            while (resultSet.next()) {
                String officeId = resultSet.getString("office_id");
                String projectId = resultSet.getString("project_id");
                String applicationId = resultSet.getString("application_id");
                String acquireTime = resultSet.getString("acquire_time");
                String sessionUser = resultSet.getString("session_user");
                String osUser = resultSet.getString("os_user");
                String sessionProgram = resultSet.getString("session_program");
                String sessionMachine = resultSet.getString("session_machine");

                Lock lock = new Lock(officeId, projectId, applicationId, acquireTime, sessionUser
                        , osUser, sessionProgram, sessionMachine);
                retval.add(lock);
            }
        }
        return retval;
    }

    public void releaseLock(String lockId) {
        connection(dsl, c -> CWMS_PROJECT_PACKAGE.call_RELEASE_LOCK(
                DSL.using(c, SQLDialect.ORACLE18C).configuration(),
                lockId));
    }

    public void revokeLock(String projId, String appId,
                           int revokeTimeout, String office) {
        BigInteger revokeTimeoutBI = toBigInteger(revokeTimeout);
        connection(dsl, c ->
                CWMS_PROJECT_PACKAGE.call_REVOKE_LOCK(getDslContext(c, office).configuration(),
                        projId,
                        appId, revokeTimeoutBI, office));
    }


    public void denyLockRevocation(String lockId) {
        connection(dsl, c ->
                CWMS_PROJECT_PACKAGE.call_DENY_LOCK_REVOCATION(
                        DSL.using(c, SQLDialect.ORACLE18C).configuration(),
                        lockId));
    }


}
