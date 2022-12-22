package cwms.radar.data.dao;

import static org.jooq.SQLDialect.ORACLE;

import cwms.radar.ApiServlet;
import cwms.radar.api.NotFoundException;
import cwms.radar.datasource.ConnectionPreparer;
import cwms.radar.datasource.ConnectionPreparingDataSource;
import cwms.radar.datasource.SessionOfficePreparer;
import io.javalin.http.Context;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
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

    public static DSLContext getDslContext(Context ctx) {
        DSLContext retval;
        final String officeId = ctx.attribute(ApiServlet.OFFICE_ID);
        final DataSource dataSource = ctx.attribute(ApiServlet.DATA_SOURCE);
        if (dataSource != null) {
            ConnectionPreparer officeSetter = new SessionOfficePreparer(officeId);
            DataSource ds = new ConnectionPreparingDataSource(officeSetter, dataSource);
            retval = DSL.using(ds, SQLDialect.ORACLE11G);
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


    public static RuntimeException wrapException(RuntimeException input) {
        RuntimeException retval = input;

        // Can add specializations as needed.
        if (isNotFound(input)) {
            retval = buildNotFound(input);
        }

        return retval;
    }

    public static boolean isNotFound(RuntimeException input) {
        boolean retval = false;

        Throwable cause = input;
        if (input instanceof DataAccessException) {
            DataAccessException dae = (DataAccessException) input;
            cause = dae.getCause();
        }

        if (cause instanceof SQLException) {
            SQLException sqlException = (SQLException) cause;
            final String localizedMessage = cause.getLocalizedMessage();

            // See link for a more complete list of CWMS Error codes:
            // https://bitbucket.hecdev.net/projects/CWMS/repos/cwms_database_origin_teamcity_work/browse/src/buildSqlScripts.py#4866

            int errorCode = sqlException.getErrorCode();
            if (errorCode == 20001 || errorCode == 20025 || errorCode == 20034
                    || localizedMessage.contains("_DOES_NOT_EXIST")
                    || localizedMessage.contains("_NOT_FOUND")
                    || localizedMessage.contains(" does not exist.")
            ) {
                retval = true;
            }
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


    // ExecuteListeners aren't called by DSL.connection blocks...
    void connection(DSLContext dslContext, ConnectionRunnable cr) {
        try {
            dslContext.connection(cr);
        } catch (RuntimeException e) {
            throw wrapException(e);
        }
    }

    <T> T connectionResult(DSLContext dslContext, ConnectionCallable<T> var1){
        try {
            return dslContext.connectionResult(var1);
        } catch (RuntimeException e) {
            throw wrapException(e);
        }
    }

}
