package cwms.cda.datasource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.concurrent.TimeUnit;

import com.google.common.flogger.FluentLogger;
import org.jooq.exception.DataAccessException;


public class DirectUserPreparer implements ConnectionPreparer {
    public static final FluentLogger logger = FluentLogger.forEnclosingClass();
    private final String user;

    public DirectUserPreparer(String user) {
        this.user = user;
    }

    @Override
    public Connection prepare(Connection conn) {
        try {
            if (!conn.isReadOnly()) {
                if (user != null) {
                    String sql = "begin cwms_env.set_session_user_direct(upper(?)); end;";
                    try (PreparedStatement setApiUser = conn.prepareStatement(sql)) {
                        setApiUser.setString(1, user);
                        setApiUser.execute();
                    }
                }
            } else {
                logger.atFiner()
                        .atMostEvery(10, TimeUnit.MINUTES)
                        .log("Connection is read-only.  No user session set.");
            }
        } catch (Exception e) {
            throw new DataAccessException("Unable to set user session.  "
                    + "user empty = " + user.isEmpty(), e);
        }

        return conn;
    }
}
