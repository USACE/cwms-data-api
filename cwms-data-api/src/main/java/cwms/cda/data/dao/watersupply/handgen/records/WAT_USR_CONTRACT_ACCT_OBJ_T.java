//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package cwms.cda.data.dao.watersupply.handgen.records;

import java.sql.Timestamp;
import org.jooq.Field;
import org.jooq.Record6;
import org.jooq.Row6;
import org.jooq.impl.UDTRecordImpl;
import usace.cwms.db.jooq.codegen.udt.records.LOCATION_REF_T;
import usace.cwms.db.jooq.codegen.udt.records.LOOKUP_TYPE_OBJ_T;
import usace.cwms.db.jooq.codegen.udt.records.WATER_USER_CONTRACT_REF_T;

public class WAT_USR_CONTRACT_ACCT_OBJ_T extends UDTRecordImpl<cwms.cda.data.dao.watersupply.handgen.records.WAT_USR_CONTRACT_ACCT_OBJ_T>
        implements Record6<WATER_USER_CONTRACT_REF_T, LOCATION_REF_T, LOOKUP_TYPE_OBJ_T, Double, Timestamp, String> {
    private static final long serialVersionUID = 1L;

    public WAT_USR_CONTRACT_ACCT_OBJ_T setWATER_USER_CONTRACT_REF(WATER_USER_CONTRACT_REF_T value) {
        this.set(0, value);
        return this;
    }

    public WATER_USER_CONTRACT_REF_T getWATER_USER_CONTRACT_REF() {
        return (WATER_USER_CONTRACT_REF_T)this.get(0);
    }

    public WAT_USR_CONTRACT_ACCT_OBJ_T setPUMP_LOCATION_REF(LOCATION_REF_T value) {
        this.set(1, value);
        return this;
    }

    public LOCATION_REF_T getPUMP_LOCATION_REF() {
        return (LOCATION_REF_T)this.get(1);
    }

    public WAT_USR_CONTRACT_ACCT_OBJ_T setPHYSICAL_TRANSFER_TYPE(LOOKUP_TYPE_OBJ_T value) {
        this.set(2, value);
        return this;
    }

    public LOOKUP_TYPE_OBJ_T getPHYSICAL_TRANSFER_TYPE() {
        return (LOOKUP_TYPE_OBJ_T)this.get(2);
    }

    public WAT_USR_CONTRACT_ACCT_OBJ_T setPUMP_FLOW(Double value) {
        this.set(3, value);
        return this;
    }

    public Double getPUMP_FLOW() {
        return (Double)this.get(3);
    }

    public WAT_USR_CONTRACT_ACCT_OBJ_T setTRANSFER_START_DATETIME(Timestamp value) {
        this.set(5, value);
        return this;
    }

    public Timestamp getTRANSFER_START_DATETIME() {
        return (Timestamp)this.get(5);
    }

    public WAT_USR_CONTRACT_ACCT_OBJ_T setACCOUNTING_REMARKS(String value) {
        this.set(6, value);
        return this;
    }

    public String getACCOUNTING_REMARKS() {
        return (String)this.get(6);
    }

    public Row6<WATER_USER_CONTRACT_REF_T, LOCATION_REF_T, LOOKUP_TYPE_OBJ_T, Double, Timestamp, String> fieldsRow() {
        return (Row6)super.fieldsRow();
    }

    public Row6<WATER_USER_CONTRACT_REF_T, LOCATION_REF_T, LOOKUP_TYPE_OBJ_T, Double, Timestamp, String> valuesRow() {
        return (Row6)super.valuesRow();
    }

    public Field<WATER_USER_CONTRACT_REF_T> field1() {
        return cwms.cda.data.dao.watersupply.handgen.WAT_USR_CONTRACT_ACCT_OBJ_T.WATER_USER_CONTRACT_REF;
    }

    public Field<LOCATION_REF_T> field2() {
        return cwms.cda.data.dao.watersupply.handgen.WAT_USR_CONTRACT_ACCT_OBJ_T.PUMP_LOCATION_REF;
    }

    public Field<LOOKUP_TYPE_OBJ_T> field3() {
        return cwms.cda.data.dao.watersupply.handgen.WAT_USR_CONTRACT_ACCT_OBJ_T.PHYSICAL_TRANSFER_TYPE;
    }

    public Field<Double> field4() {
        return cwms.cda.data.dao.watersupply.handgen.WAT_USR_CONTRACT_ACCT_OBJ_T.PUMP_FLOW;
    }

    public Field<Timestamp> field5() {
        return cwms.cda.data.dao.watersupply.handgen.WAT_USR_CONTRACT_ACCT_OBJ_T.TRANSFER_START_DATETIME;
    }

    public Field<String> field6() {
        return cwms.cda.data.dao.watersupply.handgen.WAT_USR_CONTRACT_ACCT_OBJ_T.ACCOUNTING_REMARKS;
    }

    public WATER_USER_CONTRACT_REF_T component1() {
        return this.getWATER_USER_CONTRACT_REF();
    }

    public LOCATION_REF_T component2() {
        return this.getPUMP_LOCATION_REF();
    }

    public LOOKUP_TYPE_OBJ_T component3() {
        return this.getPHYSICAL_TRANSFER_TYPE();
    }

    public Double component4() {
        return this.getPUMP_FLOW();
    }

    public Timestamp component5() {
        return this.getTRANSFER_START_DATETIME();
    }

    public String component6() {
        return this.getACCOUNTING_REMARKS();
    }

    public WATER_USER_CONTRACT_REF_T value1() {
        return this.getWATER_USER_CONTRACT_REF();
    }

    public LOCATION_REF_T value2() {
        return this.getPUMP_LOCATION_REF();
    }

    public LOOKUP_TYPE_OBJ_T value3() {
        return this.getPHYSICAL_TRANSFER_TYPE();
    }

    public Double value4() {
        return this.getPUMP_FLOW();
    }

    public Timestamp value5() {
        return this.getTRANSFER_START_DATETIME();
    }

    public String value6() {
        return this.getACCOUNTING_REMARKS();
    }

    public WAT_USR_CONTRACT_ACCT_OBJ_T value1(WATER_USER_CONTRACT_REF_T value) {
        this.setWATER_USER_CONTRACT_REF(value);
        return this;
    }

    public WAT_USR_CONTRACT_ACCT_OBJ_T value2(LOCATION_REF_T value) {
        this.setPUMP_LOCATION_REF(value);
        return this;
    }

    public WAT_USR_CONTRACT_ACCT_OBJ_T value3(LOOKUP_TYPE_OBJ_T value) {
        this.setPHYSICAL_TRANSFER_TYPE(value);
        return this;
    }

    public WAT_USR_CONTRACT_ACCT_OBJ_T value4(Double value) {
        this.setPUMP_FLOW(value);
        return this;
    }

    public WAT_USR_CONTRACT_ACCT_OBJ_T value5(Timestamp value) {
        this.setTRANSFER_START_DATETIME(value);
        return this;
    }

    public WAT_USR_CONTRACT_ACCT_OBJ_T value6(String value) {
        this.setACCOUNTING_REMARKS(value);
        return this;
    }

    public WAT_USR_CONTRACT_ACCT_OBJ_T values(WATER_USER_CONTRACT_REF_T value1, LOCATION_REF_T value2, LOOKUP_TYPE_OBJ_T value3, Double value4, Timestamp value5, String value6) {
        this.value1(value1);
        this.value2(value2);
        this.value3(value3);
        this.value4(value4);
        this.value5(value5);
        this.value6(value6);
        return this;
    }

    public WAT_USR_CONTRACT_ACCT_OBJ_T() {
        super(cwms.cda.data.dao.watersupply.handgen.WAT_USR_CONTRACT_ACCT_OBJ_T.WAT_USR_CONTRACT_ACCT_OBJ_T);
    }

    public WAT_USR_CONTRACT_ACCT_OBJ_T(WATER_USER_CONTRACT_REF_T WATER_USER_CONTRACT_REF, LOCATION_REF_T PUMP_LOCATION_REF, LOOKUP_TYPE_OBJ_T PHYSICAL_TRANSFER_TYPE, Double PUMP_FLOW, Timestamp TRANSFER_START_DATETIME, String ACCOUNTING_REMARKS) {
        super(cwms.cda.data.dao.watersupply.handgen.WAT_USR_CONTRACT_ACCT_OBJ_T.WAT_USR_CONTRACT_ACCT_OBJ_T);
        this.setWATER_USER_CONTRACT_REF(WATER_USER_CONTRACT_REF);
        this.setPUMP_LOCATION_REF(PUMP_LOCATION_REF);
        this.setPHYSICAL_TRANSFER_TYPE(PHYSICAL_TRANSFER_TYPE);
        this.setPUMP_FLOW(PUMP_FLOW);
        this.setTRANSFER_START_DATETIME(TRANSFER_START_DATETIME);
        this.setACCOUNTING_REMARKS(ACCOUNTING_REMARKS);
        this.resetChangedOnNotNull();
    }
}
