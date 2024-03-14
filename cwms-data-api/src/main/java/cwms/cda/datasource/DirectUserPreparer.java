package cwms.cda.datasource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import org.jooq.exception.DataAccessException;


public class DirectUserPreparer implements ConnectionPreparer {
    private final String user;

    public DirectUserPreparer(String user) {
        this.user = user;
    }

    @Override
    public Connection prepare(Connection conn) {
        if (user != null) {
            String sql = "begin cwms_env.set_session_user_direct(upper(?)); end;";
            try (PreparedStatement setApiUser = conn.prepareStatement(sql)) {
                setApiUser.setString(1,user);
                setApiUser.execute();                
            } catch (Exception e) {
                throw new DataAccessException("Unable to set user session.  "
                        + "user empty = " + user.isEmpty(), e);
            }
        }

        return conn;
    }
}
