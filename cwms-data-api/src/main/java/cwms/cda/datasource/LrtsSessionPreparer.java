package cwms.cda.datasource;

import static cwms.cda.data.dao.Dao.formatBool;
import static org.jooq.SQLDialect.ORACLE18C;

import com.google.common.flogger.FluentLogger;
import cwms.cda.data.dao.JooqDao;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import usace.cwms.db.jooq.codegen.packages.CWMS_UTIL_PACKAGE;


/**
 * Prepares a connection by setting the LRTS session flag.
 */
public class LrtsSessionPreparer implements ConnectionPreparer {

    private static final FluentLogger logger = FluentLogger.forEnclosingClass();

    private final Boolean isNewLrts;
    private final boolean clearOnClose;

    public LrtsSessionPreparer(Boolean isNewLrts) {
        this(isNewLrts, false);
    }

    public LrtsSessionPreparer(Boolean isNewLrts, boolean clear) {
        this.isNewLrts = isNewLrts;
        clearOnClose = clear;
    }

    @Override
    public Connection prepare(Connection connection) {
        if (isNewLrts == null) {
            return connection;
        }

        DSLContext dsl = DSL.using(connection, ORACLE18C);

        // Can also get current value and remember it and then reset to that in the close
        // if setting with null,null doesn't work.
        //            GET_SESSION_INFO sessionInfo = CWMS_UTIL_PACKAGE.call_GET_SESSION_INFO(dslContext.configuration()
        //                    , SESSION_USE_LRTS_ID_FORMAT);

        String requireBool = formatBool(isNewLrts);
        int requireIntValue = isNewLrts ? JooqDao.REQUIRE_NEW_LRTS_ID_FORMAT
                : JooqDao.REQUIRE_OLD_LRTS_ID_FORMAT;
        CWMS_UTIL_PACKAGE.call_SET_SESSION_INFO(dsl.configuration(),
                JooqDao.SESSION_USE_LRTS_ID_FORMAT, requireBool, requireIntValue);
        logger.atFine().log("Set LRTS session flag to %s (%d) for connection %s",
                requireBool, requireIntValue, connection);

        if (clearOnClose) {
            // Return a proxy that will unset the flag on close()
            return (Connection) Proxy.newProxyInstance(
                    connection.getClass().getClassLoader(),
                    new Class<?>[]{Connection.class},
                    new CloseUnsettingHandler(connection));
        } else {
            return connection;
        }

    }

    private static class CloseUnsettingHandler implements InvocationHandler {
        private final Connection delegate;
        private volatile boolean closed = false;

        CloseUnsettingHandler(Connection delegate) {
            this.delegate = delegate;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            String name = method.getName();
            if ("close".equals(name)) {
                if (!closed) {
                    try {
                        DSLContext dsl = DSL.using(delegate, ORACLE18C);
                        // Passing null values to clear the session setting
                        CWMS_UTIL_PACKAGE.call_SET_SESSION_INFO(dsl.configuration(),
                                JooqDao.SESSION_USE_LRTS_ID_FORMAT, null, null);
                        logger.atFine().log("Cleared LRTS session flag for connection %s", delegate);
                    } catch (RuntimeException ex) {
                        logger.atWarning().withCause(ex)
                                .log("Failed to clear LRTS session flag on connection close");
                    } finally {
                        closed = true;
                    }
                }
                return method.invoke(delegate, args);
            }
            return method.invoke(delegate, args);
        }
    }
}
