//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package cwms.cda.data.dao.watersupply.handgen;

import java.sql.Timestamp;
import org.jooq.Package;
import org.jooq.Schema;
import org.jooq.UDTField;
import org.jooq.impl.DSL;
import org.jooq.impl.DateAsTimestampBinding;
import org.jooq.impl.SQLDataType;
import org.jooq.impl.SchemaImpl;
import org.jooq.impl.UDTImpl;
import usace.cwms.db.jooq.codegen.CWMS_20;
import usace.cwms.db.jooq.codegen.udt.records.LOCATION_REF_T;
import usace.cwms.db.jooq.codegen.udt.records.LOOKUP_TYPE_OBJ_T;
import usace.cwms.db.jooq.codegen.udt.records.WATER_USER_CONTRACT_REF_T;

public class WAT_USR_CONTRACT_ACCT_OBJ_T extends UDTImpl<cwms.cda.data.dao.watersupply.handgen.records.WAT_USR_CONTRACT_ACCT_OBJ_T> {
    private static final long serialVersionUID = 1L;
    public static final WAT_USR_CONTRACT_ACCT_OBJ_T WAT_USR_CONTRACT_ACCT_OBJ_T = new WAT_USR_CONTRACT_ACCT_OBJ_T();
    public static final UDTField<cwms.cda.data.dao.watersupply.handgen.records.WAT_USR_CONTRACT_ACCT_OBJ_T, WATER_USER_CONTRACT_REF_T> WATER_USER_CONTRACT_REF;
    public static final UDTField<cwms.cda.data.dao.watersupply.handgen.records.WAT_USR_CONTRACT_ACCT_OBJ_T, LOCATION_REF_T> PUMP_LOCATION_REF;
    public static final UDTField<cwms.cda.data.dao.watersupply.handgen.records.WAT_USR_CONTRACT_ACCT_OBJ_T, LOOKUP_TYPE_OBJ_T> PHYSICAL_TRANSFER_TYPE;
    public static final UDTField<cwms.cda.data.dao.watersupply.handgen.records.WAT_USR_CONTRACT_ACCT_OBJ_T, Double> PUMP_FLOW;
    public static final UDTField<cwms.cda.data.dao.watersupply.handgen.records.WAT_USR_CONTRACT_ACCT_OBJ_T, String> PUMP_FLOW_UNIT;
    public static final UDTField<cwms.cda.data.dao.watersupply.handgen.records.WAT_USR_CONTRACT_ACCT_OBJ_T, Timestamp> TRANSFER_START_DATETIME;
    public static final UDTField<cwms.cda.data.dao.watersupply.handgen.records.WAT_USR_CONTRACT_ACCT_OBJ_T, String> ACCOUNTING_REMARKS;

    public Class<cwms.cda.data.dao.watersupply.handgen.records.WAT_USR_CONTRACT_ACCT_OBJ_T> getRecordType() {
        return cwms.cda.data.dao.watersupply.handgen.records.WAT_USR_CONTRACT_ACCT_OBJ_T.class;
    }

    WAT_USR_CONTRACT_ACCT_OBJ_T() {
        super("WAT_USR_CONTRACT_ACCT_OBJ_T", (Schema)null, (Package)null, false);
    }

    public Schema getSchema() {
        return (Schema)(CWMS_20.CWMS_20 != null ? CWMS_20.CWMS_20 : new SchemaImpl(DSL.name("CWMS_20")));
    }

    static {
        WATER_USER_CONTRACT_REF = createField(DSL.name("WATER_USER_CONTRACT_REF"), usace.cwms.db.jooq.codegen.udt.WATER_USER_CONTRACT_REF_T.WATER_USER_CONTRACT_REF_T.getDataType(), WAT_USR_CONTRACT_ACCT_OBJ_T, "");
        PUMP_LOCATION_REF = createField(DSL.name("PUMP_LOCATION_REF"), usace.cwms.db.jooq.codegen.udt.LOCATION_REF_T.LOCATION_REF_T.getDataType(), WAT_USR_CONTRACT_ACCT_OBJ_T, "");
        PHYSICAL_TRANSFER_TYPE = createField(DSL.name("PHYSICAL_TRANSFER_TYPE"), usace.cwms.db.jooq.codegen.udt.LOOKUP_TYPE_OBJ_T.LOOKUP_TYPE_OBJ_T.getDataType(), WAT_USR_CONTRACT_ACCT_OBJ_T, "");
        PUMP_FLOW = createField(DSL.name("PUMP_FLOW"), SQLDataType.DOUBLE, WAT_USR_CONTRACT_ACCT_OBJ_T, "");
        PUMP_FLOW_UNIT = createField(DSL.name("PUMP_FLOW_UNIT"), SQLDataType.VARCHAR(16), WAT_USR_CONTRACT_ACCT_OBJ_T, "");
        TRANSFER_START_DATETIME = createField(DSL.name("TRANSFER_START_DATETIME"), SQLDataType.TIMESTAMP(0), WAT_USR_CONTRACT_ACCT_OBJ_T, "", new DateAsTimestampBinding());
        ACCOUNTING_REMARKS = createField(DSL.name("ACCOUNTING_REMARKS"), SQLDataType.VARCHAR(255), WAT_USR_CONTRACT_ACCT_OBJ_T, "");
    }
}
