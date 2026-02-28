//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package cwms.cda.data.dao.watersupply.handgen;

import org.jooq.Parameter;
import org.jooq.impl.AbstractRoutine;
import org.jooq.impl.DSL;
import org.jooq.impl.Internal;
import org.jooq.impl.SQLDataType;
import usace.cwms.db.jooq.codegen.CWMS_20;
import usace.cwms.db.jooq.codegen.packages.CWMS_WATER_SUPPLY_PACKAGE;
import usace.cwms.db.jooq.codegen.udt.LOC_REF_TIME_WINDOW_OBJ_T;
import usace.cwms.db.jooq.codegen.udt.WAT_USR_CONTRACT_ACCT_OBJ_T;
import usace.cwms.db.jooq.codegen.udt.records.LOC_REF_TIME_WINDOW_TAB_T;
import usace.cwms.db.jooq.codegen.udt.records.WATER_USER_CONTRACT_REF_T;
import cwms.cda.data.dao.watersupply.handgen.records.WAT_USR_CONTRACT_ACCT_TAB_T;

public class STORE_ACCOUNTING_SET extends AbstractRoutine<Void> {
    private static final long serialVersionUID = 1L;
    public static final Parameter<WAT_USR_CONTRACT_ACCT_TAB_T> P_ACCOUNTING_TAB;
    public static final Parameter<WATER_USER_CONTRACT_REF_T> P_CONTRACT_REF;
    public static final Parameter<LOC_REF_TIME_WINDOW_TAB_T> P_PUMP_TIME_WINDOW_TAB;
    public static final Parameter<String> P_TIME_ZONE;
    public static final Parameter<String> P_FLOW_UNIT_ID;
    public static final Parameter<String> P_STORE_RULE;
    public static final Parameter<String> P_OVERRIDE_PROT;

    public STORE_ACCOUNTING_SET() {
        super("STORE_ACCOUNTING_SET", CWMS_20.CWMS_20, CWMS_WATER_SUPPLY_PACKAGE.CWMS_WATER_SUPPLY);
        this.addInParameter(P_ACCOUNTING_TAB);
        this.addInParameter(P_CONTRACT_REF);
        this.addInParameter(P_PUMP_TIME_WINDOW_TAB);
        this.addInParameter(P_TIME_ZONE);
        this.addInParameter(P_FLOW_UNIT_ID);
        this.addInParameter(P_STORE_RULE);
        this.addInParameter(P_OVERRIDE_PROT);
    }

    public void setP_ACCOUNTING_TAB(WAT_USR_CONTRACT_ACCT_TAB_T value) {
        this.setValue(P_ACCOUNTING_TAB, value);
    }

    public void setP_CONTRACT_REF(WATER_USER_CONTRACT_REF_T value) {
        this.setValue(P_CONTRACT_REF, value);
    }

    public void setP_PUMP_TIME_WINDOW_TAB(LOC_REF_TIME_WINDOW_TAB_T value) {
        this.setValue(P_PUMP_TIME_WINDOW_TAB, value);
    }

    public void setP_TIME_ZONE(String value) {
        this.setValue(P_TIME_ZONE, value);
    }

    public void setP_FLOW_UNIT_ID(String value) {
        this.setValue(P_FLOW_UNIT_ID, value);
    }

    public void setP_STORE_RULE(String value) {
        this.setValue(P_STORE_RULE, value);
    }

    public void setP_OVERRIDE_PROT(String value) {
        this.setValue(P_OVERRIDE_PROT, value);
    }

    static {
        P_ACCOUNTING_TAB = Internal.createParameter("P_ACCOUNTING_TAB", WAT_USR_CONTRACT_ACCT_OBJ_T.WAT_USR_CONTRACT_ACCT_OBJ_T.getDataType().asArrayDataType(WAT_USR_CONTRACT_ACCT_TAB_T.class), false, false);
        P_CONTRACT_REF = Internal.createParameter("P_CONTRACT_REF", usace.cwms.db.jooq.codegen.udt.WATER_USER_CONTRACT_REF_T.WATER_USER_CONTRACT_REF_T.getDataType(), false, false);
        P_PUMP_TIME_WINDOW_TAB = Internal.createParameter("P_PUMP_TIME_WINDOW_TAB", LOC_REF_TIME_WINDOW_OBJ_T.LOC_REF_TIME_WINDOW_OBJ_T.getDataType().asArrayDataType(LOC_REF_TIME_WINDOW_TAB_T.class), false, false);
        P_TIME_ZONE = Internal.createParameter("P_TIME_ZONE", SQLDataType.VARCHAR.defaultValue(DSL.field(DSL.raw("NULL"), SQLDataType.VARCHAR)), true, false);
        P_FLOW_UNIT_ID = Internal.createParameter("P_FLOW_UNIT_ID", SQLDataType.VARCHAR.defaultValue(DSL.field(DSL.raw("NULL"), SQLDataType.VARCHAR)), true, false);
        P_STORE_RULE = Internal.createParameter("P_STORE_RULE", SQLDataType.VARCHAR.defaultValue(DSL.field(DSL.raw("NULL"), SQLDataType.VARCHAR)), true, false);
        P_OVERRIDE_PROT = Internal.createParameter("P_OVERRIDE_PROT", SQLDataType.VARCHAR.defaultValue(DSL.field(DSL.raw("NULL"), SQLDataType.VARCHAR)), true, false);
    }
}
