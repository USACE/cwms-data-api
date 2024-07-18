/*
 * MIT License
 *
 * Copyright (c) 2024 Hydrologic Engineering Center
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package cwms.cda.data.dao;

import static org.jooq.SQLDialect.ORACLE;

import com.google.common.flogger.FluentLogger;
import com.google.common.flogger.StackSize;
import cwms.cda.ApiServlet;
import cwms.cda.api.errors.AlreadyExists;
import cwms.cda.api.errors.InvalidItemException;
import cwms.cda.api.errors.NotFoundException;
import cwms.cda.datasource.ConnectionPreparingDataSource;
import cwms.cda.security.CwmsAuthException;
import io.javalin.http.Context;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import javax.servlet.http.HttpServletResponse;
import javax.sql.DataSource;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jooq.Condition;
import org.jooq.ConnectionCallable;
import org.jooq.ConnectionRunnable;
import org.jooq.DSLContext;
import org.jooq.ExecuteListener;
import org.jooq.Field;
import org.jooq.SQLDialect;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.CustomCondition;
import org.jooq.impl.DSL;
import org.jooq.impl.DefaultExecuteListenerProvider;
import org.owasp.html.HtmlPolicyBuilder;
import org.owasp.html.PolicyFactory;
import usace.cwms.db.jooq.codegen.packages.CWMS_ENV_PACKAGE;


public abstract class JooqDao<T> extends Dao<T> {
    protected static final int ORACLE_CURSOR_TYPE = -10;
    private static final FluentLogger logger = FluentLogger.forEnclosingClass();

    static ExecuteListener listener = new ExceptionWrappingListener();

    public enum DeleteMethod {
        DELETE_ALL(DeleteRule.DELETE_ALL),
        DELETE_KEY(DeleteRule.DELETE_KEY),
        DELETE_DATA(DeleteRule.DELETE_DATA);

        private final DeleteRule rule;

        DeleteMethod(DeleteRule rule) {
            this.rule = rule;
        }

        public DeleteRule getRule() {
            return rule;
        }
    }

    protected JooqDao(DSLContext dsl) {
        super(dsl);
    }

    /**
     * This method is used by the ApiServlet to get a jOOQ DSLContext given
     * the current request context.  ApiServlet places certain attributes into
     * the context and this method uses those attributes to create a DSLContext.
     * An ExecuteListener is also added to the DSLContext to wrap certain
     * recognized SQLExceptions in more specific CDA exception types.  This
     * enables ApiServlet to handle the exception specialization in a more
     * generic way.
     *
     * @param ctx The current request context.
     * @return A DSLContext for the current request.
     */
    public static DSLContext getDslContext(Context ctx) {
        DSLContext retVal;
        final String officeId = ctx.attribute(ApiServlet.OFFICE_ID);
        final DataSource dataSource = ctx.attribute(ApiServlet.DATA_SOURCE);
        if (dataSource != null) {
            DataSource wrappedDataSource = new ConnectionPreparingDataSource(connection ->
                    setClientInfo(ctx, connection), dataSource);
            retVal = DSL.using(wrappedDataSource, SQLDialect.ORACLE18C);
        } else {
            // Some tests still use this method
            logger.atFine().withStackTrace(StackSize.FULL)
                  .log("System still using old context method.");
            Connection database = ctx.attribute(ApiServlet.DATABASE);
            retVal = getDslContext(database, officeId);
        }

        retVal.configuration().set(new DefaultExecuteListenerProvider(listener));

        return retVal;
    }

    public static DSLContext getDslContext(Connection connection, String officeId) {
        // Because this dsl is constructed with a connection, jOOQ will reuse the provided
        // connection and not get new connections from a DataSource.  See:
        //   https://www.jooq.org/doc/latest/manual/sql-building/dsl-context/connection-vs-datasource/
        // This also means
        // that jOOQ will not automatically return the connection to the pool for each statement.
        // So it is safe for us to set the session office id.
        // Everything that uses the returned dsl after this method will reuse this connection.
        // This method should probably be called from within a connection{  } block and jOOQ
        // code within the block should use the returned DSLContext or the connection.
        DSLContext dsl = DSL.using(connection, SQLDialect.ORACLE18C);
        CWMS_ENV_PACKAGE.call_SET_SESSION_OFFICE_ID(dsl.configuration(), officeId);

        return dsl;
    }

    private static Connection setClientInfo(Context ctx, Connection connection) {
        try {
            connection.setClientInfo("OCSID.ECID", ApiServlet.APPLICATION_TITLE + " " + ApiServlet.VERSION);
            connection.setClientInfo("OCSID.MODULE", ctx.endpointHandlerPath());
            connection.setClientInfo("OCSID.ACTION", ctx.method());
            connection.setClientInfo("OCSID.CLIENTID", ctx.url().replace(ctx.path(), "") + ctx.contextPath());
        } catch (SQLClientInfoException ex) {
            logger.atWarning()
                    .withCause(ex)
                    .log("Unable to set client info on connection.");
        }
        return connection;
    }

    @Override
    public List<T> getAll(String officeId) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Optional<T> getByUniqueName(String uniqueName, String officeId) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    protected static Double toDouble(BigDecimal bigDecimal) {
        Double retVal = null;
        if (bigDecimal != null) {
            retVal = bigDecimal.doubleValue();
        }

        return retVal;
    }

    /**
     * Oracle supports case insensitive regexp search but the syntax for calling it is a
     * bit weird.  This method lets Dao classes add a case-insensitive regexp search in
     * an easy to read manner without having to worry about the syntax.
     */
    public static Condition caseInsensitiveLikeRegex(Field<String> field, String regex) {
        return new CustomCondition() {
            @Override
            public void accept(org.jooq.Context<?> ctx) {
                if (ctx.family() == ORACLE) {
                    ctx.visit(DSL.condition("{regexp_like}({0}, {1}, 'i')", field, DSL.val(regex)));
                } else {
                    ctx.visit(DSL.upper(field).likeRegex(regex.toUpperCase()));
                }
            }
        };
    }

    protected static Condition filterExact(Field<String> field, String filter) {
        if (filter == null) {
            return DSL.noCondition();
        } else {
            return field.eq(filter);
        }
    }

    /**
     * Oracle supports case insensitive regexp search but the syntax for calling it is a
     * bit weird.  This method lets Dao classes add a case-insensitive regexp search in
     * an easy to read manner without having to worry about the syntax.
     * <p/>
     * A null regex will return a condition that always evaluates to true
     */
    public static Condition caseInsensitiveLikeRegexNullTrue(Field<String> field, String regex) {
        if (regex == null) {
            return DSL.noCondition();
        }
        return caseInsensitiveLikeRegex(field, regex);
    }

    /**
     * This method tries to determine if the given RuntimeException
     * is one of several types of exception (e.q NotFound,
     * AlreadyExists, NullArg) that can be specially handled by ApiServlet
     * by returning specific HTTP codes or error messages.
     * @param input the observed exception
     * @return An exception, possibly wrapped
     */
    public static RuntimeException wrapException(RuntimeException input) {
        RuntimeException retVal = input;

        // Add specializations as needed.
        if (isNotFound(input)) {
            retVal = buildNotFound(input);
        } else if (isAlreadyExists(input)) {
            retVal = buildAlreadyExists(input);
        } else if (isNullArgument(input)) {
            retVal = buildNullArgument(input);
        } else if (isInvalidItem(input)) {
            retVal = buildInvalidItem(input);
        } else if (isCantSetSessionNoPermissions(input)) {
            retVal = buildNotAuthorizedForOffice(input);
        } else if (isInvalidUnits(input)) {
            retVal = buildInvalidUnits(input);
        }

        return retVal;
    }

    public static Optional<SQLException> getSqlException(Throwable input) {
        Throwable cause = input;

        if (input instanceof DataAccessException) {
            DataAccessException dae = (DataAccessException) input;
            cause = dae.getCause();
        }

        if (cause instanceof SQLException) {
            return Optional.of((SQLException) cause);
        } else if (cause instanceof DataAccessException) {
            return getSqlException(cause); // There might be nested DataAccessExceptions
        } else {
            return Optional.empty();
        }
    }

    private static boolean hasCodeOrMessage(SQLException sqlException,
                                            List<Integer> codes, List<String> segments) {
        final String localizedMessage = sqlException.getLocalizedMessage();

        return codes.contains(sqlException.getErrorCode())
                || segments.stream().anyMatch(localizedMessage::contains);
    }

    private static boolean hasCodeAndMessage(SQLException sqlException,
                                            List<Integer> codes, List<String> segments) {
        final String localizedMessage = sqlException.getLocalizedMessage();

        return codes.contains(sqlException.getErrorCode())
                && segments.stream().anyMatch(localizedMessage::contains);
    }


    // See link for a more complete list of CWMS Error codes:
    // https://bitbucket.hecdev.net/projects/CWMS/repos/cwms_database_origin_teamcity_work/browse/src/buildSqlScripts.py#4866

    public static boolean isNotFound(RuntimeException input) {
        boolean retVal = false;

        Optional<SQLException> optional = getSqlException(input);
        if (optional.isPresent()) {
            SQLException sqlException = optional.get();

            List<Integer> codes = Arrays.asList(20001, 20025, 20034);
            List<String> segments = Arrays.asList("_DOES_NOT_EXIST", "_NOT_FOUND",
                    " does not exist.");

            retVal = hasCodeOrMessage(sqlException, codes, segments);

            if(!retVal)
            {
                segments = Collections.singletonList("does not exist as a stream location");
                retVal = hasCodeAndMessage(sqlException, Collections.singletonList(20998), segments);
            }
        }
        return retVal;
    }    

    public static boolean isInvalidItem(RuntimeException input) {
        boolean retVal = false;

        Optional<SQLException> optional = getSqlException(input);
        if (optional.isPresent()) {
            SQLException sqlException = optional.get();

            List<Integer> codes = Collections.singletonList(20019);
            List<String> segments = Collections.singletonList("INVALID_ITEM");

            retVal = hasCodeOrMessage(sqlException, codes, segments);
        }
        return retVal;
    }

    @NotNull
    static NotFoundException buildNotFound(RuntimeException input) {
        // The cause can be kinda long and include all the line numbers from the pl/sql.

        Throwable cause = input;
        if (input instanceof DataAccessException) {
            DataAccessException dae = (DataAccessException) input;
            cause = dae.getCause();
        }

        NotFoundException exception = new NotFoundException(cause);

        String localizedMessage = cause.getLocalizedMessage();
        if (localizedMessage != null) {
            String[] parts = localizedMessage.split("\n");
            if (parts.length > 1) {
                exception = new NotFoundException(parts[0], cause);
            }
        }
        return exception;
    }

    public static boolean isCantSetSessionNoPermissions(RuntimeException input) {
        boolean retVal = false;
        Optional<SQLException> optional = getSqlException(input);
        if (optional.isPresent()) {
            SQLException sqlException = optional.get();

            // 20998 is the code we're getting but that is the generic error code.
            // We'll need to use hasCode_AND_Message or this will trigger on other errors.
            List<Integer> codes = Collections.singletonList(20998);
            List<String> segments = Collections.singletonList("does not have any assigned privileges");

            retVal = hasCodeAndMessage(sqlException, codes, segments);  // _AND_
        }
        return retVal;
    }

    @NotNull
    static CwmsAuthException buildNotAuthorizedForOffice(RuntimeException input) {
        // The cause can be kinda long and include all the line numbers from the pl/sql.

        Throwable cause = input;
        if (input instanceof DataAccessException) {
            DataAccessException dae = (DataAccessException) input;
            cause = dae.getCause();
        }

        return new CwmsAuthException("User not authorized for this office.", cause,
                                            HttpServletResponse.SC_UNAUTHORIZED, false);
    }

    public static boolean isAlreadyExists(RuntimeException input) {
        boolean retVal = false;

        Optional<SQLException> optional = getSqlException(input);
        if (optional.isPresent()) {
            SQLException sqlException = optional.get();
            List<Integer> codes = Arrays.asList(20003, 20020, 20026);
            List<String> segments = Arrays.asList("ALREADY_EXISTS", " already exists.");

            retVal = hasCodeOrMessage(sqlException, codes, segments);

        }
        return retVal;
    }


    private static RuntimeException buildAlreadyExists(RuntimeException input) {
        Throwable cause = input;
        if (input instanceof DataAccessException) {
            DataAccessException dae = (DataAccessException) input;
            cause = dae.getCause();
        }

        AlreadyExists exception = new AlreadyExists(cause);

        String localizedMessage = cause.getLocalizedMessage();
        if (localizedMessage != null) {
            String[] parts = localizedMessage.split("\n");
            if (parts.length > 1) {
                exception = new AlreadyExists(parts[0], cause);
            }
        }
        return exception;
    }

    private static boolean isNullArgument(RuntimeException input) {
        boolean retVal = false;

        Optional<SQLException> optional = getSqlException(input);
        if (optional.isPresent()) {
            SQLException sqlException = optional.get();
            List<Integer> codes = Collections.singletonList(20244);
            List<String> segments = Arrays.asList("NULL_ARGUMENT", " already exists.");

            retVal = hasCodeOrMessage(sqlException, codes, segments);

        }
        return retVal;
    }

    private static RuntimeException buildNullArgument(RuntimeException input) {
        Throwable cause = input;
        if (input instanceof DataAccessException) {
            DataAccessException dae = (DataAccessException) input;
            cause = dae.getCause();
        }

        RuntimeException exception = new IllegalArgumentException(cause);

        String localizedMessage = cause.getLocalizedMessage();
        if (localizedMessage != null) {
            String[] parts = localizedMessage.split("\n");
            if (parts.length > 1) {
                exception = new IllegalArgumentException(parts[0], cause);
            }
        }
        return exception;
    }

    private static InvalidItemException buildInvalidItem(RuntimeException input) {
        // The cause can be kinda long and include all the line numbers from the pl/sql.

        Throwable cause = input;
        if (input instanceof DataAccessException) {
            DataAccessException dae = (DataAccessException) input;
            cause = dae.getCause();
        }

        String message = "Invalid Item.";

        String localizedMessage = cause.getLocalizedMessage();
        if (localizedMessage != null) {
            String[] parts = localizedMessage.split("\n");
            if (parts.length > 1) {
                message = parts[0];
            }
        }
        return new InvalidItemException(message, cause);
    }

    public static boolean isInvalidUnits(RuntimeException input) {
        boolean retVal = false;

        Optional<SQLException> optional = getSqlException(input);
        if (optional.isPresent()) {
            SQLException sqlException = optional.get();
            String message = sqlException.getLocalizedMessage();
            int errorCode = sqlException.getErrorCode();

            retVal = errorCode == 20998
                    && message.contains("ORA-20102: The unit")
                    && message.contains("is not a recognized CWMS Database unit for the")
                ;
        }
        return retVal;
    }

    private static InvalidItemException buildInvalidUnits(RuntimeException input) {

        Throwable cause = input;
        if (input instanceof DataAccessException) {
            DataAccessException dae = (DataAccessException) input;
            cause = dae.getCause();
        }

        String localizedMessage = cause.getLocalizedMessage();
        if (localizedMessage != null) {
            // skip ahead in localizedMessage to "ORA-20102:"
            String searchFor = "ORA-20102:";
            int start = localizedMessage.indexOf(searchFor);
            if (start >= 0) {
                localizedMessage = localizedMessage.substring(start + searchFor.length());
                String[] parts = localizedMessage.split("\n");
                if (parts.length >= 1) {
                    localizedMessage = parts[0];
                }
            }
        }

        localizedMessage = sanitizeOrNull(localizedMessage);

        if (localizedMessage == null || localizedMessage.isEmpty()) {
            localizedMessage = "Invalid Units.";
        }

        return new InvalidItemException(localizedMessage, cause);
    }

    private static @Nullable String sanitizeOrNull(@Nullable String localizedMessage) {
        if (localizedMessage != null && !localizedMessage.isEmpty()) {
            int length = localizedMessage.length();
            PolicyFactory sanitizer = new HtmlPolicyBuilder().disallowElements("<script>").toFactory();
            localizedMessage = sanitizer.sanitize(localizedMessage);
            if (localizedMessage.length() != length) {
                // The message was sanitized, it crops everything after the bad input.
                // If the message was "The unit: BADUNIT is not a recognized...."  and the sanitizer
                // decides it doesn't like the word "BADUNIT" then the message will be cropped to
                // "The unit: ".  Which is weird to return.  Just return null.
                localizedMessage = null;
            }
        }
        return localizedMessage;
    }


    /**
     * JooqDao provides its own connection method because the DSL.connection
     * method does not cause thrown exception to be wrapped.
     * @param dslContext the DSLContext to use
     * @param cr the ConnectionRunnable to run with the connection
     */
    protected static void connection(DSLContext dslContext, ConnectionRunnable cr) {
        try {
            dslContext.connection(cr);
        } catch (RuntimeException e) {
            throw wrapException(e);
        }
    }

    /**
     * Like DSL.connection the DSL.connectionResult method does not cause thrown
     * exceptions to be wrapped.  This method delegates to DSL.connectionResult
     * but will wrap exceptions into more specific exception types were possible.
     * @param dslContext the DSLContext to use
     * @param var1 the ConnectionCallable to run with the connection
     */
    protected static <R> R connectionResult(DSLContext dslContext, ConnectionCallable<R> var1) {
        try {
            return dslContext.connectionResult(var1);
        } catch (RuntimeException e) {
            throw wrapException(e);
        }
    }

}
