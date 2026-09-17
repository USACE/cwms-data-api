package cwms.cda.datasource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import cwms.cda.api.DataApiTestIT;
import fixtures.CwmsDataApiSetupCallback;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@Tag("integration")
class ReadDeadlineTestIT extends DataApiTestIT {
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void deadlineReleasesPoolSlotAndNextBorrowerWorks(boolean disableOob) throws Exception {
        org.apache.tomcat.jdbc.pool.DataSource source = new org.apache.tomcat.jdbc.pool.DataSource();
        source.setUrl(CwmsDataApiSetupCallback.getDatabaseLink().getJdbcUrl());
        source.setUsername(CwmsDataApiSetupCallback.getWebUser());
        source.setPassword(CwmsDataApiSetupCallback.getDatabaseLink().getPassword());
        source.setDriverClassName("oracle.jdbc.OracleDriver");
        Properties properties = new Properties();
        properties.setProperty("oracle.net.disableOob", Boolean.toString(disableOob));
        source.setDbProperties(properties);
        source.setMaxActive(1);
        source.setInitialSize(1);
        source.setMaxIdle(1);
        source.setMinIdle(0);
        source.setMaxWait(2000);
        source.setTestOnBorrow(true);
        source.setValidationQuery("select 1 from dual");
        source.setValidationInterval(0);
        try {
            String firstSession;
            int originalTimeout;
            try (Connection warm = source.getConnection()) {
                firstSession = sessionId(warm);
                originalTimeout = warm.getNetworkTimeout();
            }
            long started = System.nanoTime();
            SQLException failure = assertThrows(SQLException.class, () -> {
                try (Connection guarded = new ReadDeadline(1000).wrap(source).getConnection();
                     PreparedStatement sleep = guarded.prepareStatement("begin dbms_session.sleep(?); end;")) {
                    sleep.setInt(1, 10);
                    sleep.execute();
                }
            });
            long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
            System.out.println("Oracle deadline: disableOob=" + disableOob + ", elapsedMs=" + elapsed
                    + ", errorCode=" + failure.getErrorCode());
            assertTrue(elapsed < 5000, "Ten-second query returned after " + elapsed + " ms");
            assertEquals(0, source.getActive(), "Timed-out request must release its pool slot");
            try (Connection next = source.getConnection()) {
                String nextSession = sessionId(next);
                if (failure.getErrorCode() == 1013) {
                    assertEquals(firstSession, nextSession, "Clean cancellation should preserve the session");
                    assertEquals(originalTimeout, next.getNetworkTimeout());
                } else {
                    assertNotEquals(firstSession, nextSession, "Broken session must not be handed out again");
                }
            }
            assertEquals(0, source.getActive());
        } finally {
            source.close();
        }
    }

    private static String sessionId(Connection connection) throws SQLException {
        try (PreparedStatement query = connection.prepareStatement("select dbms_session.unique_session_id from dual");
             ResultSet result = query.executeQuery()) {
            assertTrue(result.next());
            return result.getString(1);
        }
    }
}
