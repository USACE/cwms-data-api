package cwms.radar.datasource;

import java.sql.Connection;
import java.util.logging.Logger;

import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import usace.cwms.db.jooq.codegen.packages.CWMS_ENV_PACKAGE;

public class SessionOfficePreparer implements ConnectionPreparer {
    private static final Logger logger = Logger.getLogger(SessionOfficePreparer.class.getName());

    private final String office;

    public SessionOfficePreparer(String office) {
        this.office = office;
    }

    @Override
    public Connection prepare(Connection conn) {

        if(office != null && !office.isEmpty()) {
            try {
                logger.fine("Setting office to: " + office);
                DSLContext dsl = DSL.using(conn, SQLDialect.ORACLE11G);
                CWMS_ENV_PACKAGE.call_SET_SESSION_OFFICE_ID(dsl.configuration(), office);
            } catch (Exception e) {
                throw new DataAccessException("Unable to set session office id to " + office, e);
            }
        } else {
            logger.fine("Office is null or empty.");
            // Should we call clear_session_privileges ?
        }
        return conn;
    }
}
