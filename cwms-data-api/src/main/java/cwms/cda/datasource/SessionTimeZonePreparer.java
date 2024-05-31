package cwms.cda.datasource;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import javax.annotation.Nullable;

public class SessionTimeZonePreparer implements ConnectionPreparer {

    public SessionTimeZonePreparer() {

    }

    private static @Nullable String getSessionTimeZone(Connection connection) throws SQLException {
        String dbTimeZone = null;
        String sql = "SELECT sessiontimezone FROM dual";
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {
            if (resultSet.next()) {
                dbTimeZone = resultSet.getString(1);
            }
        }
        return dbTimeZone;
    }

    private static void setSessionTimeZoneUtc(Connection connection) throws SQLException {
        String sql = "ALTER SESSION SET TIME_ZONE = 'UTC'";
        try (CallableStatement statement = connection.prepareCall(sql)) {
            statement.execute();
        }
    }

    @Override
    public Connection prepare(Connection conn) throws SQLException {
        setSessionTimeZoneUtc(conn);
        return conn;
    }

}