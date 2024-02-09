package cwms.cda.datasource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;


public class DirectUserPreparer implements ConnectionPreparer {
    private final String user;

    public DirectUserPreparer(String user) {
        this.user = user;
    }

    @Override
    public Connection prepare(Connection conn) {
        if (user != null) {
            try (
                PreparedStatement setApiUser = conn.prepareStatement("begin cwms_env.set_session_user_direct(upper(?)); end;");
                ) {                
                setApiUser.setString(1,user);
                setApiUser.execute();                
            } catch (Exception e) {
                boolean keyNullOrEmpty = user == null || user.isEmpty();
                throw new DataAccessException("Unable to set user session.  "
                        + "user null or empty = " + keyNullOrEmpty, e);
            }
        }

        return conn;
    }
}
