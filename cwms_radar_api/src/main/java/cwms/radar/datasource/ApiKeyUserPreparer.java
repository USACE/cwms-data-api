package cwms.radar.datasource;

import java.sql.Connection;
import java.sql.PreparedStatement;import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;


public class ApiKeyUserPreparer implements ConnectionPreparer {
    private final String user;

    public ApiKeyUserPreparer(String user) {
        this.user = user;
    }

    @Override
    public Connection prepare(Connection conn) {
        if (user != null) {
            try (DSLContext dsl = DSL.using(conn, SQLDialect.ORACLE11G);
                PreparedStatement setApiUser = conn.prepareStatement("begin cwms_env.set_session_user_direct(upper('q0hectest_pu')); end;");
                PreparedStatement setUser = conn.prepareStatement("begin CWMS_ENV.set_session_user_direct(?); end;")
                ) {
                setApiUser.execute();
                setUser.setString(1,user);
                setUser.execute();
            } catch (Exception e) {
                boolean keyNullOrEmpty = user == null || user.isEmpty();
                throw new DataAccessException("Unable to set user session.  "
                        + "user null or empty = " + keyNullOrEmpty, e);
            }
        }

        return conn;
    }
}
