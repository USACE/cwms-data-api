package cwms.cda.datasource;

import java.sql.Connection;
import java.sql.SQLException;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import usace.cwms.db.jooq.codegen.packages.CWMS_ENV_PACKAGE;

public class SessionUserPreparer implements ConnectionPreparer {
    private final String sessionKey;

    public SessionUserPreparer(String key) {
        this.sessionKey = key;
    }

    @Override
    public Connection prepare(Connection conn) {
        if (sessionKey == null || !sessionKey.startsWith("testing")) {
            try (DSLContext dsl = DSL.using(conn, SQLDialect.ORACLE11G)) {
                CWMS_ENV_PACKAGE.call_SET_SESSION_USER(dsl.configuration(), sessionKey);
            } catch (Exception e) {
                boolean keyNullOrEmpty = sessionKey == null || sessionKey.isEmpty();
                throw new DataAccessException("Unable to set user session.  "
                        + "sessionKey null or empty = " + keyNullOrEmpty, e);
            }
        }

        return conn;
    }
}
