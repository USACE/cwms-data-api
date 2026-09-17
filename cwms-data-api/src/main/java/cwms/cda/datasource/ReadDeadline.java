package cwms.cda.datasource;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;

/** Request-owned JDBC deadline. Cancellation completes before a connection can return to its pool. */
public final class ReadDeadline {
    private static final ScheduledThreadPoolExecutor CANCELLATIONS = cancellationExecutor();
    private final long expires;

    /**
     * Starts a deadline with a positive timeout of at most five minutes.
     * @throws IllegalArgumentException if the configured timeout is outside the allowed range
     */
    public ReadDeadline(long timeoutMillis) {
        if (timeoutMillis < 1 || timeoutMillis > 300_000) {
            throw new IllegalArgumentException("Read timeout must be between 1 and 300000 milliseconds");
        }
        expires = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
    }

    private static ScheduledThreadPoolExecutor cancellationExecutor() {
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(8, task -> {
            Thread thread = new Thread(task, "cda-read-cancellation");
            thread.setDaemon(true);
            return thread;
        });
        executor.setRemoveOnCancelPolicy(true);
        executor.setKeepAliveTime(10, TimeUnit.SECONDS);
        executor.allowCoreThreadTimeOut(true);
        return executor;
    }

    /** Returns whether the monotonic deadline has elapsed. */
    public boolean expired() {
        return System.nanoTime() >= expires;
    }

    /**
     * Stops request-owned work once its deadline or thread interruption is observed.
     * @throws Expired if the deadline elapsed or the request thread was interrupted
     */
    public void check() {
        if (expired() || Thread.currentThread().isInterrupted()) {
            throw new Expired();
        }
    }

    /** Returns the remaining positive timeout or throws when it has elapsed. */
    public int remainingMillis() {
        check();
        return (int) Math.max(1, TimeUnit.NANOSECONDS.toMillis(expires - System.nanoTime()));
    }

    /** Applies this request's deadline to connections obtained from the supplied source. */
    public DataSource wrap(DataSource source) {
        return new DelegatingDataSource(source) {
            @Override
            public Connection getConnection() throws SQLException {
                check();
                return wrapConnection(super.getConnection());
            }

            @Override
            public Connection getConnection(String user, String password) throws SQLException {
                check();
                return wrapConnection(super.getConnection(user, password));
            }
        };
    }

    private Connection wrapConnection(Connection connection) throws SQLException {
        final int previousTimeout;
        try {
            check();
            previousTimeout = connection.getNetworkTimeout();
            int timeout = remainingMillis() + 1000;
            connection.setNetworkTimeout(Runnable::run,
                    previousTimeout > 0 ? Math.min(previousTimeout, timeout) : timeout);
        } catch (SQLException | RuntimeException ex) {
            try {
                connection.close();
            } catch (SQLException closeError) {
                ex.addSuppressed(closeError);
            }
            throw ex;
        }
        List<CancellableStatement> statements = new ArrayList<>();
        return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(),
                new Class<?>[]{Connection.class}, (proxy, method, args) -> {
                    String name = method.getName();
                    if ("close".equals(name)) {
                        // Do not permit a late cancellation to reach a subsequent pool borrower.
                        for (CancellableStatement statement : statements) {
                            statement.finish();
                        }
                        try {
                            if (!connection.isClosed()) {
                                connection.setNetworkTimeout(Runnable::run, previousTimeout);
                            }
                        } finally {
                            connection.close();
                        }
                        return null;
                    }
                    if ("prepareStatement".equals(name) || "prepareCall".equals(name)
                            || "createStatement".equals(name)) {
                        check();
                        Statement raw = (Statement) invoke(connection, method, args);
                        CancellableStatement tracked = new CancellableStatement(raw);
                        statements.add(tracked);
                        return tracked.proxy((Connection) proxy);
                    }
                    return invoke(connection, method, args);
                });
    }

    private final class CancellableStatement {
        private final Statement statement;
        private final ScheduledFuture<?> cancellation;
        private boolean finished;

        private CancellableStatement(Statement statement) throws SQLException {
            this.statement = statement;
            try {
                setTimeout();
            } catch (SQLException | RuntimeException ex) {
                try {
                    statement.close();
                } catch (SQLException closeError) {
                    ex.addSuppressed(closeError);
                }
                throw ex;
            }
            cancellation = CANCELLATIONS.schedule(this::cancel,
                    Math.max(0, expires - System.nanoTime()), TimeUnit.NANOSECONDS);
        }

        private void setTimeout() throws SQLException {
            statement.setQueryTimeout(Math.max(1, (remainingMillis() + 999) / 1000));
        }

        private synchronized void cancel() {
            if (!finished) {
                try {
                    statement.cancel();
                } catch (SQLException ignored) {
                    // The socket deadline remains the backstop for a broken connection.
                }
            }
        }

        private synchronized void finish() {
            finished = true;
            cancellation.cancel(false);
        }

        private Statement proxy(Connection connection) {
            Class<?> type = statement instanceof CallableStatement ? CallableStatement.class
                    : statement instanceof PreparedStatement ? PreparedStatement.class : Statement.class;
            return (Statement) Proxy.newProxyInstance(Statement.class.getClassLoader(),
                    new Class<?>[]{type}, (proxy, method, args) -> {
                        String name = method.getName();
                        if ("close".equals(name)) {
                            finish();
                            return invoke(statement, method, args);
                        }
                        if ("getConnection".equals(name)) {
                            return connection;
                        }
                        if (name.startsWith("execute")) {
                            check();
                            setTimeout();
                        }
                        Object value = invoke(statement, method, args);
                        if (value instanceof ResultSet) {
                            return wrapRows((ResultSet) value);
                        }
                        return value;
                    });
        }

        private ResultSet wrapRows(ResultSet rows) {
            return (ResultSet) Proxy.newProxyInstance(ResultSet.class.getClassLoader(),
                    new Class<?>[]{ResultSet.class}, (proxy, method, args) -> {
                        if ("next".equals(method.getName())) {
                            check();
                        }
                        return invoke(rows, method, args);
                    });
        }
    }

    private static Object invoke(Object target, Method method, Object[] args) throws Throwable {
        try {
            return method.invoke(target, args);
        } catch (InvocationTargetException ex) {
            throw ex.getCause();
        }
    }

    /** Recognizes deadline failures even when the SQL layer wraps their cause. */
    public static boolean isTimeout(Throwable error) {
        for (Throwable cause = error; cause != null; cause = cause.getCause()) {
            if (cause instanceof Expired || cause instanceof SQLTimeoutException) {
                return true;
            }
        }
        return false;
    }

    public static final class Expired extends RuntimeException {
        public Expired() {
            super("Time-series read deadline exceeded");
        }
    }
}
