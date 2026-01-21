package cwms.cda.datasource;

import java.sql.Connection;

import com.google.common.flogger.FluentLogger;

import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import usace.cwms.db.jooq.codegen.packages.CWMS_ENV_PACKAGE;

public class SessionOfficePreparer implements ConnectionPreparer {
    private static final FluentLogger logger = FluentLogger.forEnclosingClass();

    private final String office;

    public SessionOfficePreparer(String office) {
        this.office = office;
    }

    @Override
    public Connection prepare(Connection conn) {

        if (office != null && !office.isEmpty()) {
            try {
                if (!conn.isReadOnly()) {
                    DSLContext dsl = DSL.using(conn, SQLDialect.ORACLE18C);

                    logger.atFine().log("Setting office to: %s", office);
                    CWMS_ENV_PACKAGE.call_SET_SESSION_OFFICE_ID(dsl.configuration(), office);
                } else {
                    logger.atFiner().atMostEvery(10, java.util.concurrent.TimeUnit.MINUTES)
                            .log("Connection is read-only.  No office session set.");
                }
            } catch (Exception e) {
                throw new DataAccessException("Unable to set session office id to " + office, e);
            }
        } else {
            logger.atFine().log("Office is null or empty.");
            // Should we call clear_session_privileges ?
        }
        return conn;
    }
}
