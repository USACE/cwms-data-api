package cwms.cda.data.dao;

import java.sql.Connection;
import javax.sql.DataSource;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.jooq.impl.DataSourceConnectionProvider;
import usace.cwms.db.jooq.codegen.packages.CWMS_ENV_PACKAGE;

class OfficeSettingConnectionProvider extends DataSourceConnectionProvider {
    private final String officeId;

    public OfficeSettingConnectionProvider(DataSource dataSource, String officeId) {
        super(dataSource);
        this.officeId = officeId;
    }

    @Override
    public Connection acquire() throws DataAccessException {
        Connection conn = super.acquire();
        return getConnection(conn, officeId);
    }

    public static Connection getConnection(Connection conn, String office) {
        try {
            DSLContext dsl = DSL.using(conn, SQLDialect.ORACLE11G);
            CWMS_ENV_PACKAGE.call_SET_SESSION_OFFICE_ID(dsl.configuration(), office);
        } catch (Exception e) {
            throw new DataAccessException("Unable to set session office id to " + office, e);
        }
        return conn;
    }

    // Override release to clear session settings if necessary.
}
