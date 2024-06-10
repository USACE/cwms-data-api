package cwms.cda.data.dao.project;

import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dto.project.LockRevokerRights;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import usace.cwms.db.dao.util.OracleTypeMap;
import usace.cwms.db.jooq.codegen.packages.CWMS_PROJECT_PACKAGE;

public class ProjectLockRevokerRightsDao extends JooqDao<LockRevokerRights> {
    protected ProjectLockRevokerRightsDao(DSLContext dsl) {
        super(dsl);
    }

    public void updateLockRevokerRights(LockRevokerRights lock, boolean allow) {
        CWMS_PROJECT_PACKAGE.call_UPDATE_LOCK_REVOKER_RIGHTS(dsl.configuration(),
                lock.getUserId(), lock.getProjectId(), OracleTypeMap.formatBool(allow),
                lock.getApplicationId(), lock.getOfficeId());
    }


    public List<LockRevokerRights> catLockRevokerRights(String projectMask,
                                                        String applicationMask, String officeMask) {
        return connectionResult(dsl, c -> {
            List<LockRevokerRights> retval = new ArrayList<>();
            try (ResultSet rs = CWMS_PROJECT_PACKAGE.call_CAT_LOCK_REVOKER_RIGHTS(
                    DSL.using(c, SQLDialect.ORACLE18C).configuration(),
                    projectMask, applicationMask, officeMask).intoResultSet()) {
                while (rs.next()) {
                    String officeId = rs.getString("office_id");
                    String projectId = rs.getString("project_id");
                    String applicationId = rs.getString("application_id");
                    String userId = rs.getString("user_id");

                    LockRevokerRights lock = new LockRevokerRights(officeId, projectId,
                            applicationId, userId);
                    retval.add(lock);
                }

            }
            return retval;
        });
    }
}
