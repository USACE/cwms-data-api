package cwms.cda.data.dao;

import static usace.cwms.db.jooq.codegen.tables.AV_DB_CHANGE_LOG.AV_DB_CHANGE_LOG;

import cwms.cda.data.dto.CwmsDTO;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import usace.cwms.db.dao.ifc.env.CwmsDbEnv;
import usace.cwms.db.dao.util.services.CwmsDbServiceLookup;

public abstract class Dao<T> {
    public static final int CWMS_18_1_8 = 180108;
    public static final int CWMS_21_1_1 = 210101;
    public static final int CWMS_23_03_16 = 230316;

    private int cwmsDbVersion;
    @SuppressWarnings("unused")
    protected DSLContext dsl;

    public Dao(DSLContext dsl) {
        this.dsl = dsl;
        String version = dsl.connectionResult(c -> DSL.using(c,SQLDialect.ORACLE18C).select(AV_DB_CHANGE_LOG.VERSION)
            .from(AV_DB_CHANGE_LOG)
            .orderBy(AV_DB_CHANGE_LOG.VERSION_DATE.desc())
            .limit(1)
            .fetchOne().component1());
        String[] parts = version.split("\\.");
        cwmsDbVersion =
            Integer.parseInt(parts[0]) * 10000
            + Integer.parseInt(parts[1]) * 100
            + Integer.parseInt(parts[2]);
    }

    public int getDbVersion() {
        return cwmsDbVersion;
    }


    /**
     * Sets session office on specific connection.
     * @param c opened connection
     * @param object Data containing a valid CWMS office
     * @throws SQLException if the underlying database throws an exception
     */
    protected void setOffice(Connection c, CwmsDTO object) throws SQLException {
        this.setOffice(c,object.getOfficeId());
    }

    protected void setOffice(Connection c, String office) throws SQLException {
        CwmsDbEnv db = CwmsDbServiceLookup.buildCwmsDb(CwmsDbEnv.class, c);
        db.setSessionOfficeId(c,office);
    }


    public abstract List<T> getAll(String office);

    public abstract Optional<T> getByUniqueName(String uniqueName, String office);

}
