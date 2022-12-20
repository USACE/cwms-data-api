package cwms.radar.datasource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;


public class ApiKeyUserPreparer implements ConnectionPreparer {
    private static final Logger logger = Logger.getLogger(ApiKeyUserPreparer.class.getName());
    private final String apiKey;

    public ApiKeyUserPreparer(String apiKey) {
        this.apiKey = apiKey;
    }

    @Override
    public Connection prepare(Connection conn) {
        if (apiKey != null) {
            try (DSLContext dsl = DSL.using(conn, SQLDialect.ORACLE11G);
                PreparedStatement setUser = conn.prepareStatement("begin CWMS_ENV.set_session_user_apikey(?); end;");
                PreparedStatement getPrivs = conn.prepareStatement("select NVL(SYS_CONTEXT ('CWMS_ENV', 'CWMS_PRIVILEGE')"
                                                                     + ",'none'),NVL(SYS_CONTEXT ('CWMS_ENV', "
                                                                     + " 'CWMS_USER'),'not set') from dual");
                ) {
                setUser.setString(1,apiKey);
                setUser.execute();
                logger.fine(() -> {
                    try (ResultSet rs = getPrivs.executeQuery()) {
                        rs.next();
                        return "(CWMS_PRIV,USER) set to: " + rs.getString(1) + rs.getString(2);
                    } catch (Exception ex) {
                        return "CWMS_PRIV set to: " + ex.getLocalizedMessage();
                    }
                });                
                
            } catch (Exception e) {
                boolean keyNullOrEmpty = apiKey == null || apiKey.isEmpty();
                throw new DataAccessException("Unable to set user session.  "
                        + "user null or empty = " + keyNullOrEmpty, e);
            }
        }

        return conn;
    }
}
