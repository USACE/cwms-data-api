package cwms.messaging;

import oracle.jdbc.driver.OracleConnection;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.logging.Logger;


/**
 * This class is a wrapper around a DataSource that delegates all calls to the
 * wrapped DataSource.  It is intended to be extended by classes that need to
 * override DataSource methods.
 */
public class DataSourceWrapper implements DataSource {


    private DataSource delegate;

    /**
     * Create a new DelegatingDataSource.
     * @param delegate the target DataSource
     */
    public DataSourceWrapper(DataSource delegate) {
        //wrapped DelegatingDataSource is used because internally AQJMS casts the returned connection
        //as an OracleConnection, but the JNDI pool is returning us a proxy, so unwrap it
        this.delegate = delegate;
    }

    /**
     * Return the target DataSource that this DataSource should delegate to.
     */

    public DataSource getDelegate() {
        return this.delegate;
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return getDelegate().getLogWriter();
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
        getDelegate().setLogWriter(out);
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return getDelegate().getLoginTimeout();
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        getDelegate().setLoginTimeout(seconds);
    }



    @Override
    @SuppressWarnings("unchecked")
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) {
            return (T) this;
        }
        return getDelegate().unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return (iface.isInstance(this) || getDelegate().isWrapperFor(iface));
    }


    @Override
    public Logger getParentLogger() {
        return Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
    }

    @Override
    public Connection getConnection() throws SQLException {
        return getDelegate().getConnection().unwrap(OracleConnection.class);
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        return getDelegate().getConnection(username, password).unwrap(OracleConnection.class);
    }
}