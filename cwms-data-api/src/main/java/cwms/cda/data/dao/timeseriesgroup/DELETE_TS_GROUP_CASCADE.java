package cwms.cda.data.dao.timeseriesgroup;

import org.jooq.Parameter;
import org.jooq.impl.AbstractRoutine;
import org.jooq.impl.DSL;
import org.jooq.impl.Internal;
import org.jooq.impl.SQLDataType;
import usace.cwms.db.jooq.codegen.CWMS_20;
import usace.cwms.db.jooq.codegen.packages.CWMS_TS_PACKAGE;

public class DELETE_TS_GROUP_CASCADE extends AbstractRoutine<Void> {
    private static final long serialVersionUID = 1L;
    public static final Parameter<String> P_TS_CATEGORY_ID;
    public static final Parameter<String> P_TS_GROUP_ID;
    public static final Parameter<String> P_CASCADE;
    public static final Parameter<String> P_DB_OFFICE_ID;

    public DELETE_TS_GROUP_CASCADE() {
        super("DELETE_TS_GROUP_CASCADE", CWMS_20.CWMS_20, CWMS_TS_PACKAGE.CWMS_TS);
        this.addInParameter(P_TS_CATEGORY_ID);
        this.addInParameter(P_TS_GROUP_ID);
        this.addInParameter(P_CASCADE);
        this.addInParameter(P_DB_OFFICE_ID);
    }

    public void setP_TS_CATEGORY_ID(String value) {
        this.setValue(P_TS_CATEGORY_ID, value);
    }

    public void setP_TS_GROUP_ID(String value) {
        this.setValue(P_TS_GROUP_ID, value);
    }

    public void setP_CASCADE(String value) {
        this.setValue(P_CASCADE, value);
    }

    public void setP_DB_OFFICE_ID(String value) {
        this.setValue(P_DB_OFFICE_ID, value);
    }

    static {
        P_TS_CATEGORY_ID = Internal.createParameter("P_TS_CATEGORY_ID", SQLDataType.VARCHAR, false, false);
        P_TS_GROUP_ID = Internal.createParameter("P_TS_GROUP_ID", SQLDataType.VARCHAR, false, false);
        P_CASCADE = Internal.createParameter("P_CASCADE", SQLDataType.VARCHAR.defaultValue(DSL.field(DSL.raw("NULL"), SQLDataType.VARCHAR)), true, false);
        P_DB_OFFICE_ID = Internal.createParameter("P_DB_OFFICE_ID", SQLDataType.VARCHAR.defaultValue(DSL.field(DSL.raw("NULL"), SQLDataType.VARCHAR)), true, false);
    }
}