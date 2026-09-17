package cwms.cda.datasource;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;

class ReadDeadlineTest {
    @Test
    void connectionReturnWaitsForInFlightCancellation() throws Exception {
        DataSource source = mock(DataSource.class);
        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        CountDownLatch cancelling = new CountDownLatch(1);
        CountDownLatch allowCancellationToFinish = new CountDownLatch(1);
        when(source.getConnection()).thenReturn(connection);
        when(connection.prepareStatement("select 1")).thenReturn(statement);
        doAnswer(call -> {
            cancelling.countDown();
            assertTrue(allowCancellationToFinish.await(3, TimeUnit.SECONDS));
            return null;
        }).when(statement).cancel();
        Connection guarded = new ReadDeadline(300).wrap(source).getConnection();
        guarded.prepareStatement("select 1");
        java.util.concurrent.ExecutorService closer = java.util.concurrent.Executors.newSingleThreadExecutor();
        try {
            assertTrue(cancelling.await(3, TimeUnit.SECONDS));
            java.util.concurrent.Future<?> closed = closer.submit(() -> {
                try {
                    guarded.close();
                } catch (SQLException ex) {
                    throw new IllegalStateException(ex);
                }
            });
            assertThrows(java.util.concurrent.TimeoutException.class,
                    () -> closed.get(100, TimeUnit.MILLISECONDS));
            verify(connection, never()).close();
            allowCancellationToFinish.countDown();
            closed.get(3, TimeUnit.SECONDS);
            verify(connection).close();
        } finally {
            allowCancellationToFinish.countDown();
            closer.shutdownNow();
        }
    }

    @Test
    void cancelsRunningJdbcAndRestoresConnectionBeforeReturn() throws Exception {
        DataSource source = mock(DataSource.class);
        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        CountDownLatch cancelled = new CountDownLatch(1);
        when(source.getConnection()).thenReturn(connection);
        when(connection.getNetworkTimeout()).thenReturn(12345);
        when(connection.prepareStatement("select 1")).thenReturn(statement);
        doAnswer(call -> {
            cancelled.countDown();
            return null;
        }).when(statement).cancel();
        when(statement.executeQuery()).thenAnswer(call -> {
            assertTrue(cancelled.await(3, TimeUnit.SECONDS));
            throw new java.sql.SQLTimeoutException("cancelled");
        });
        ReadDeadline deadline = new ReadDeadline(300);
        try (Connection guarded = deadline.wrap(source).getConnection();
             PreparedStatement query = guarded.prepareStatement("select 1")) {
            assertThrows(java.sql.SQLTimeoutException.class, query::executeQuery);
        }
        verify(statement).cancel();
        verify(connection).setNetworkTimeout(any(), org.mockito.ArgumentMatchers.eq(12345));
        verify(connection).close();
    }

    @Test
    void closedStatementCannotCancelLaterBorrower() throws Exception {
        DataSource source = mock(DataSource.class);
        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        when(source.getConnection()).thenReturn(connection);
        when(connection.prepareStatement("select 1")).thenReturn(statement);
        ReadDeadline deadline = new ReadDeadline(300);
        try (Connection guarded = deadline.wrap(source).getConnection()) {
            guarded.prepareStatement("select 1").close();
        }
        Thread.sleep(450);
        verify(statement, never()).cancel();
        verify(connection).close();
    }

    @Test
    void checksDeadlineDuringCursorFetch() throws Exception {
        DataSource source = mock(DataSource.class);
        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        ResultSet rows = mock(ResultSet.class);
        when(source.getConnection()).thenReturn(connection);
        when(connection.prepareStatement("select 1")).thenReturn(statement);
        when(statement.executeQuery()).thenReturn(rows);
        ReadDeadline deadline = new ReadDeadline(300);
        try (Connection guarded = deadline.wrap(source).getConnection();
             PreparedStatement query = guarded.prepareStatement("select 1");
             ResultSet result = query.executeQuery()) {
            Thread.sleep(450);
            assertThrows(ReadDeadline.Expired.class, result::next);
            verify(rows, never()).next();
        }
    }

    @Test
    void closesCheckoutWhenDeadlineExpiresWaitingForPool() throws Exception {
        DataSource source = mock(DataSource.class);
        Connection connection = mock(Connection.class);
        when(source.getConnection()).thenAnswer(call -> {
            Thread.sleep(150);
            return connection;
        });
        ReadDeadline deadline = new ReadDeadline(50);
        assertThrows(ReadDeadline.Expired.class, () -> deadline.wrap(source).getConnection());
        verify(connection).close();
    }

    @Test
    void invalidTimeoutDoesNotStartWork() {
        assertThrows(IllegalArgumentException.class, () -> new ReadDeadline(0));
        assertFalse(ReadDeadline.isTimeout(new SQLException("unrelated")));
        assertTrue(ReadDeadline.isTimeout(new RuntimeException(new java.sql.SQLTimeoutException())));
    }
}
