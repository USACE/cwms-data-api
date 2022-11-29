package cwms.radar.datasource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import usace.cwms.db.jooq.codegen.packages.CWMS_ENV_PACKAGE;

public class ApiKeyUserPreparer implements ConnectionPreparer {
    private final String user;

    public ApiKeyUserPreparer(String user) {
        this.user = user;
    }

    @Override
    public Connection prepare(Connection conn) {
        if (user != null) {
            try (DSLContext dsl = DSL.using(conn, SQLDialect.ORACLE11G);
                PreparedStatement setUser = conn.prepareStatement("begin DBMS_SESSION.set_context('CWMS_ENV','CWMS_USER',?); end;")
                ) {
                CWMS_ENV_PACKAGE.call_CLEAR_SESSION_PRIVILEGES(dsl.configuration());
                CWMS_ENV_PACKAGE.call_SET_SESSION_PRIVILEGES(dsl.configuration());
                setUser.setString(1,user);
                setUser.execute();
                CWMS_ENV_PACKAGE.call_SET_SESSION_PRIVILEGES(dsl.configuration());
            } catch (Exception e) {
                boolean keyNullOrEmpty = user == null || user.isEmpty();
                throw new DataAccessException("Unable to set user session.  "
                        + "user null or empty = " + keyNullOrEmpty, e);
            }
        }

        return conn;
    }
}
