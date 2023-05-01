package cwms.radar.data.dao;

import static org.jooq.SQLDialect.ORACLE;

import cwms.radar.ApiServlet;
import cwms.radar.api.errors.AlreadyExists;
import cwms.radar.api.errors.NotFoundException;
import io.javalin.http.Context;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import javax.sql.DataSource;
import org.jetbrains.annotations.NotNull;
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
import usace.cwms.db.jooq.codegen.packages.CWMS_ENV_PACKAGE;

public abstract class JooqDao<T> extends Dao<T> {
    static ExecuteListener listener = new ExceptionWrappingListener();

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
        DSLContext retval;
        final String officeId = ctx.attribute(ApiServlet.OFFICE_ID);
        final DataSource dataSource = ctx.attribute(ApiServlet.DATA_SOURCE);
        if (dataSource != null) {
            retval = DSL.using(dataSource, SQLDialect.ORACLE11G);
        } else {
            // Some tests still use this method
            Connection database = ctx.attribute(ApiServlet.DATABASE);
            retval = getDslContext(database, officeId);
        }

        retval.configuration().set(new DefaultExecuteListenerProvider(listener));

        return retval;
    }

    public static DSLContext getDslContext(Connection database, String officeId) {
        DSLContext dsl = DSL.using(database, SQLDialect.ORACLE11G);
        CWMS_ENV_PACKAGE.call_SET_SESSION_OFFICE_ID(dsl.configuration(), officeId);

        return dsl;
    }

    @Override
    public List<T> getAll(Optional<String> limitToOffice) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Optional<T> getByUniqueName(String uniqueName, Optional<String> limitToOffice) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    static Double toDouble(BigDecimal bigDecimal) {
        Double retval = null;
        if (bigDecimal != null) {
            retval = bigDecimal.doubleValue();
        }

        return retval;
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
                    ctx.visit(field.upper().likeRegex(regex.toUpperCase()));
                }
            }
        };
    }

    /**
     * This method tries to determine if the given RuntimeException
     * is one of several types of exception (e.q NotFound,
     * AlreadyExists, NullArg) that can be specially handled by ApiServlet
     * by returning specific HTTP codes or error messages.
     * @param input
     * @return
     */
    public static RuntimeException wrapException(RuntimeException input) {
        RuntimeException retval = input;

        // Add specializations as needed.
        if (isNotFound(input)) {
            retval = buildNotFound(input);
        } else if (isAlreadyExists(input)) {
            retval = buildAlreadyExists(input);
        } else if (isNullArgument(input)) {
            retval = buildNullArgument(input);
        }

        return retval;
    }

    public static Optional<SQLException> getSqlException(RuntimeException input) {
        Throwable cause = input;

        if (input instanceof DataAccessException) {
            DataAccessException dae = (DataAccessException) input;
            cause = dae.getCause();
        }

        if (cause instanceof SQLException) {
            return Optional.of((SQLException) cause);
        } else {
            return Optional.empty();
        }
    }

    private static boolean matches(SQLException sqlException,
                                   List<Integer> codes, List<String> segments) {
        final String localizedMessage = sqlException.getLocalizedMessage();

        return codes.contains(sqlException.getErrorCode())
                || segments.stream().anyMatch(localizedMessage::contains);
    }


    // See link for a more complete list of CWMS Error codes:
    // https://bitbucket.hecdev.net/projects/CWMS/repos/cwms_database_origin_teamcity_work/browse/src/buildSqlScripts.py#4866

    public static boolean isNotFound(RuntimeException input) {
        boolean retval = false;

        Optional<SQLException> optional = getSqlException(input);
        if (optional.isPresent()) {
            SQLException sqlException = optional.get();

            List<Integer> codes = Arrays.asList(20001, 20025, 20034);
            List<String> segments = Arrays.asList("_DOES_NOT_EXIST", "_NOT_FOUND",
                    " does not exist.");

            retval = matches(sqlException, codes, segments);
        }
        return retval;
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
                exception = new NotFoundException(parts[0]);
            }
        }
        return exception;
    }

    public static boolean isAlreadyExists(RuntimeException input) {
        boolean retval = false;

        Optional<SQLException> optional = getSqlException(input);
        if (optional.isPresent()) {
            SQLException sqlException = optional.get();
            List<Integer> codes = Arrays.asList(20003, 20020, 20026);
            List<String> segments = Arrays.asList("ALREADY_EXISTS", " already exists.");

            retval = matches(sqlException, codes, segments);

        }
        return retval;
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
                exception = new AlreadyExists(parts[0]);
            }
        }
        return exception;
    }

    private static boolean isNullArgument(RuntimeException input) {
        boolean retval = false;

        Optional<SQLException> optional = getSqlException(input);
        if (optional.isPresent()) {
            SQLException sqlException = optional.get();
            List<Integer> codes = Arrays.asList(20244);
            List<String> segments = Arrays.asList("NULL_ARGUMENT", " already exists.");

            retval = matches(sqlException, codes, segments);

        }
        return retval;
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
                exception = new IllegalArgumentException(parts[0]);
            }
        }
        return exception;
    }


    /**
     * JooqDao provides its own connection method because the DSL.connection
     * method does not cause thrown exception to be wrapped.
     * @param dslContext the DSLContext to use
     * @param cr the ConnectionRunnable to run with the connection
     */
    void connection(DSLContext dslContext, ConnectionRunnable cr) {
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
    <R> R connectionResult(DSLContext dslContext, ConnectionCallable<R> var1) {
        try {
            return dslContext.connectionResult(var1);
        } catch (RuntimeException e) {
            throw wrapException(e);
        }
    }

}
