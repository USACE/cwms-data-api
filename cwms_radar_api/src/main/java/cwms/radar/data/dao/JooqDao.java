package cwms.radar.data.dao;

import static org.jooq.SQLDialect.ORACLE;

import cwms.radar.ApiServlet;
import cwms.radar.datasource.ConnectionPreparer;
import cwms.radar.datasource.ConnectionPreparingDataSource;
import cwms.radar.datasource.SessionOfficePreparer;
import io.javalin.http.Context;
import java.math.BigDecimal;
import java.sql.Connection;
import java.util.List;
import java.util.Optional;
import javax.sql.DataSource;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.SQLDialect;
import org.jooq.impl.CustomCondition;
import org.jooq.impl.DSL;
import usace.cwms.db.jooq.codegen.packages.CWMS_ENV_PACKAGE;

public abstract class JooqDao<T> extends Dao<T> {

    protected JooqDao(DSLContext dsl) {
        super(dsl);
    }

    public static DSLContext getDslContext(Context ctx) {
        final String officeId = ctx.attribute(ApiServlet.OFFICE_ID);
        final DataSource dataSource = ctx.attribute(ApiServlet.DATA_SOURCE);
        if (dataSource != null) {
            ConnectionPreparer officeSetter = new SessionOfficePreparer(officeId);
            DataSource ds = new ConnectionPreparingDataSource(officeSetter, dataSource);

            return DSL.using(ds, SQLDialect.ORACLE11G);
        } else {
            // Some tests still use this method
            Connection database = (Connection) ctx.attribute(ApiServlet.DATABASE);
            return getDslContext(database, officeId);
        }
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

}
