package cwms.cda.data.dao.watersupply.handgen;

import cwms.cda.data.dao.watersupply.handgen.records.WAT_USR_CONTRACT_ACCT_TAB_T;
import java.math.BigInteger;
import java.sql.Timestamp;
import org.jooq.Configuration;
import usace.cwms.db.jooq.codegen.udt.records.LOC_REF_TIME_WINDOW_TAB_T;
import usace.cwms.db.jooq.codegen.udt.records.WATER_USER_CONTRACT_REF_T;

public class CWMS_WATER_SUPPLY_PACKAGE {
    public static void call_STORE_ACCOUNTING_SET(Configuration configuration,
                                                 WAT_USR_CONTRACT_ACCT_TAB_T P_ACCOUNTING_TAB,
                                                 WATER_USER_CONTRACT_REF_T P_CONTRACT_REF,
                                                 LOC_REF_TIME_WINDOW_TAB_T P_PUMP_TIME_WINDOW_TAB, String P_TIME_ZONE, String P_FLOW_UNIT_ID, String P_STORE_RULE, String P_OVERRIDE_PROT) {
        STORE_ACCOUNTING_SET p = new STORE_ACCOUNTING_SET();
        p.setP_ACCOUNTING_TAB(P_ACCOUNTING_TAB);
        p.setP_CONTRACT_REF(P_CONTRACT_REF);
        p.setP_PUMP_TIME_WINDOW_TAB(P_PUMP_TIME_WINDOW_TAB);
        p.setP_TIME_ZONE(P_TIME_ZONE);
        p.setP_FLOW_UNIT_ID(P_FLOW_UNIT_ID);
        p.setP_STORE_RULE(P_STORE_RULE);
        p.setP_OVERRIDE_PROT(P_OVERRIDE_PROT);
        p.execute(configuration);
    }

    public static WAT_USR_CONTRACT_ACCT_TAB_T call_RETRIEVE_ACCOUNTING_SET(Configuration configuration,
                                                                           WATER_USER_CONTRACT_REF_T P_CONTRACT_REF, String P_UNITS, Timestamp P_START_TIME, Timestamp P_END_TIME, String P_TIME_ZONE, String P_START_INCLUSIVE, String P_END_INCLUSIVE, String P_ASCENDING_FLAG, BigInteger P_ROW_LIMIT, String P_TRANSFER_TYPE) {
        RETRIEVE_ACCOUNTING_SET p = new RETRIEVE_ACCOUNTING_SET();
        p.setP_CONTRACT_REF(P_CONTRACT_REF);
        p.setP_UNITS(P_UNITS);
        p.setP_START_TIME(P_START_TIME);
        p.setP_END_TIME(P_END_TIME);
        p.setP_TIME_ZONE(P_TIME_ZONE);
        p.setP_START_INCLUSIVE(P_START_INCLUSIVE);
        p.setP_END_INCLUSIVE(P_END_INCLUSIVE);
        p.setP_ASCENDING_FLAG(P_ASCENDING_FLAG);
        p.setP_ROW_LIMIT(P_ROW_LIMIT);
        p.setP_TRANSFER_TYPE(P_TRANSFER_TYPE);
        p.execute(configuration);
        return p.getP_ACCOUNTING_SET();
    }

}
