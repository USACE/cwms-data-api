package cwms.radar.datasource;

import java.sql.Connection;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import usace.cwms.db.jooq.codegen.packages.CWMS_ENV_PACKAGE;

public class SessionOfficePreparer implements ConnectionPreparer {
    private final String office;

    public SessionOfficePreparer(String office) {
        this.office = office;
    }

    @Override
    public Connection prepare(Connection conn) {
        try {
            DSLContext dsl = DSL.using(conn, SQLDialect.ORACLE11G);
            CWMS_ENV_PACKAGE.call_SET_SESSION_OFFICE_ID(dsl.configuration(), office);
        } catch (Exception e) {
            throw new DataAccessException("Unable to set session office id to " + office, e);
        }
        return conn;
    }
}
