package cwms.cda.data.dao;

import java.sql.Connection;
import java.sql.SQLException;
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
        try {
            DSLContext dsl = DSL.using(conn, SQLDialect.ORACLE18C);
            CWMS_ENV_PACKAGE.call_SET_SESSION_OFFICE_ID(dsl.configuration(), officeId);
            return conn;
        } catch (Exception e) {
            try {
                conn.close();
            } catch (SQLException ex) {
                e.addSuppressed(new DataAccessException("Trying to set the session office id to " + officeId + " caused an exception."
                        + " Attempting to close the connection used in order to return it to the pool also triggered an exception.", ex));
            }
            throw e;
        }
    }



}
