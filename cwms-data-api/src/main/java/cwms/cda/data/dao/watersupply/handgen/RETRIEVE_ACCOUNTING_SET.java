//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package cwms.cda.data.dao.watersupply.handgen;

import java.math.BigInteger;
import java.sql.Timestamp;
import org.jooq.Parameter;
import org.jooq.impl.AbstractRoutine;
import org.jooq.impl.DSL;
import org.jooq.impl.DateAsTimestampBinding;
import org.jooq.impl.Internal;
import org.jooq.impl.SQLDataType;
import usace.cwms.db.jooq.codegen.CWMS_20;
import usace.cwms.db.jooq.codegen.packages.CWMS_WATER_SUPPLY_PACKAGE;
import usace.cwms.db.jooq.codegen.udt.records.WATER_USER_CONTRACT_REF_T;
import cwms.cda.data.dao.watersupply.handgen.records.WAT_USR_CONTRACT_ACCT_TAB_T;

public class RETRIEVE_ACCOUNTING_SET extends AbstractRoutine<Void> {
    private static final long serialVersionUID = 1L;
    public static final Parameter<WAT_USR_CONTRACT_ACCT_TAB_T> P_ACCOUNTING_SET;
    public static final Parameter<WATER_USER_CONTRACT_REF_T> P_CONTRACT_REF;
    public static final Parameter<String> P_UNITS;
    public static final Parameter<Timestamp> P_START_TIME;
    public static final Parameter<Timestamp> P_END_TIME;
    public static final Parameter<String> P_TIME_ZONE;
    public static final Parameter<String> P_START_INCLUSIVE;
    public static final Parameter<String> P_END_INCLUSIVE;
    public static final Parameter<String> P_ASCENDING_FLAG;
    public static final Parameter<BigInteger> P_ROW_LIMIT;
    public static final Parameter<String> P_TRANSFER_TYPE;

    public RETRIEVE_ACCOUNTING_SET() {
        super("RETRIEVE_ACCOUNTING_SET", CWMS_20.CWMS_20, CWMS_WATER_SUPPLY_PACKAGE.CWMS_WATER_SUPPLY);
        this.addOutParameter(P_ACCOUNTING_SET);
        this.addInParameter(P_CONTRACT_REF);
        this.addInParameter(P_UNITS);
        this.addInParameter(P_START_TIME);
        this.addInParameter(P_END_TIME);
        this.addInParameter(P_TIME_ZONE);
        this.addInParameter(P_START_INCLUSIVE);
        this.addInParameter(P_END_INCLUSIVE);
        this.addInParameter(P_ASCENDING_FLAG);
        this.addInParameter(P_ROW_LIMIT);
        this.addInParameter(P_TRANSFER_TYPE);
    }

    public void setP_CONTRACT_REF(WATER_USER_CONTRACT_REF_T value) {
        this.setValue(P_CONTRACT_REF, value);
    }

    public void setP_UNITS(String value) {
        this.setValue(P_UNITS, value);
    }

    public void setP_START_TIME(Timestamp value) {
        this.setValue(P_START_TIME, value);
    }

    public void setP_END_TIME(Timestamp value) {
        this.setValue(P_END_TIME, value);
    }

    public void setP_TIME_ZONE(String value) {
        this.setValue(P_TIME_ZONE, value);
    }

    public void setP_START_INCLUSIVE(String value) {
        this.setValue(P_START_INCLUSIVE, value);
    }

    public void setP_END_INCLUSIVE(String value) {
        this.setValue(P_END_INCLUSIVE, value);
    }

    public void setP_ASCENDING_FLAG(String value) {
        this.setValue(P_ASCENDING_FLAG, value);
    }

    public void setP_ROW_LIMIT(BigInteger value) {
        this.setValue(P_ROW_LIMIT, value);
    }

    public void setP_TRANSFER_TYPE(String value) {
        this.setValue(P_TRANSFER_TYPE, value);
    }

    public WAT_USR_CONTRACT_ACCT_TAB_T getP_ACCOUNTING_SET() {
        return (WAT_USR_CONTRACT_ACCT_TAB_T)this.get(P_ACCOUNTING_SET);
    }

    static {
        P_ACCOUNTING_SET = Internal.createParameter("P_ACCOUNTING_SET", WAT_USR_CONTRACT_ACCT_OBJ_T.WAT_USR_CONTRACT_ACCT_OBJ_T.getDataType().asArrayDataType(WAT_USR_CONTRACT_ACCT_TAB_T.class), false, false);
        P_CONTRACT_REF = Internal.createParameter("P_CONTRACT_REF", usace.cwms.db.jooq.codegen.udt.WATER_USER_CONTRACT_REF_T.WATER_USER_CONTRACT_REF_T.getDataType(), false, false);
        P_UNITS = Internal.createParameter("P_UNITS", SQLDataType.VARCHAR, false, false);
        P_START_TIME = Internal.createParameter("P_START_TIME", SQLDataType.TIMESTAMP(0), false, false, new DateAsTimestampBinding());
        P_END_TIME = Internal.createParameter("P_END_TIME", SQLDataType.TIMESTAMP(0), false, false, new DateAsTimestampBinding());
        P_TIME_ZONE = Internal.createParameter("P_TIME_ZONE", SQLDataType.VARCHAR.defaultValue(DSL.field(DSL.raw("NULL"), SQLDataType.VARCHAR)), true, false);
        P_START_INCLUSIVE = Internal.createParameter("P_START_INCLUSIVE", SQLDataType.VARCHAR.defaultValue(DSL.field(DSL.raw("NULL"), SQLDataType.VARCHAR)), true, false);
        P_END_INCLUSIVE = Internal.createParameter("P_END_INCLUSIVE", SQLDataType.VARCHAR.defaultValue(DSL.field(DSL.raw("NULL"), SQLDataType.VARCHAR)), true, false);
        P_ASCENDING_FLAG = Internal.createParameter("P_ASCENDING_FLAG", SQLDataType.VARCHAR.defaultValue(DSL.field(DSL.raw("NULL"), SQLDataType.VARCHAR)), true, false);
        P_ROW_LIMIT = Internal.createParameter("P_ROW_LIMIT", SQLDataType.DECIMAL_INTEGER(38).defaultValue(DSL.field(DSL.raw("NULL"), SQLDataType.DECIMAL_INTEGER)), true, false);
        P_TRANSFER_TYPE = Internal.createParameter("P_TRANSFER_TYPE", SQLDataType.VARCHAR.defaultValue(DSL.field(DSL.raw("NULL"), SQLDataType.VARCHAR)), true, false);
    }
}
