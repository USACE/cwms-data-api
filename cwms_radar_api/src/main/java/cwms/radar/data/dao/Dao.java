package cwms.radar.data.dao;

import java.util.List;
import java.util.Optional;

import org.jooq.DSLContext;

import cwms.radar.data.dto.CwmsDTO;
import usace.cwms.db.jooq.codegen.packages.CWMS_ENV_PACKAGE;

import static usace.cwms.db.jooq.codegen.tables.AV_DB_CHANGE_LOG.AV_DB_CHANGE_LOG;;

public abstract class Dao<T> {
    public static final int CWMS_18_1_8 = 180108;
    public static final int CWMS_21_1_1 = 210101;

    private int cwmsDbVersion = 0;
    @SuppressWarnings("unused")
    protected DSLContext dsl = null;

    public Dao(DSLContext dsl) {
        this.dsl = dsl;
        String version = dsl.select(AV_DB_CHANGE_LOG.VERSION)
           .from(AV_DB_CHANGE_LOG)
           .orderBy(AV_DB_CHANGE_LOG.VERSION_DATE.desc())
           .limit(1)
           .fetchOne().component1();
        String parts[] = version.split("\\.");
        cwmsDbVersion =
            Integer.parseInt(parts[0])*10000
            +Integer.parseInt(parts[1])*100
            +Integer.parseInt(parts[2]);
    }

    public int getDbVersion(){
        return cwmsDbVersion;
    }

    /**
     * This should be called before attempting to write an object to the database.
     * @param object Object Owned by an office id. 
     */
    protected void setOffice(CwmsDTO object) {
        CWMS_ENV_PACKAGE.call_SET_SESSION_OFFICE_ID(dsl.configuration(), object.getOfficeId());
    }

    /**
     * Same as setOffice with DSL, however Office is known and no DTO provided.
     * E.g. DELETE
     * @param office
     */
    protected void setOffice(String office) {
        CWMS_ENV_PACKAGE.call_SET_SESSION_OFFICE_ID(dsl.configuration(), office);
    }

    public abstract List<T> getAll(Optional<String> limitToOffice);

    public abstract Optional<T> getByUniqueName( String uniqueName, Optional<String> limitToOffice);

}
