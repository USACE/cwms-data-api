//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package cwms.cda.data.dao.watersupply.handgen.records;

import java.util.Arrays;
import java.util.Collection;
import org.jooq.impl.ArrayRecordImpl;
import usace.cwms.db.jooq.codegen.CWMS_20;

public class WAT_USR_CONTRACT_ACCT_TAB_T extends ArrayRecordImpl<WAT_USR_CONTRACT_ACCT_OBJ_T> {
    private static final long serialVersionUID = 1L;

    public WAT_USR_CONTRACT_ACCT_TAB_T() {
        super(CWMS_20.CWMS_20, "WAT_USR_CONTRACT_ACCT_TAB_T", cwms.cda.data.dao.watersupply.handgen.WAT_USR_CONTRACT_ACCT_OBJ_T.WAT_USR_CONTRACT_ACCT_OBJ_T.getDataType());
    }

    public WAT_USR_CONTRACT_ACCT_TAB_T(WAT_USR_CONTRACT_ACCT_OBJ_T... array) {
        this();
        if (array != null) {
            this.addAll(Arrays.asList(array));
        }

    }

    public WAT_USR_CONTRACT_ACCT_TAB_T(Collection<? extends WAT_USR_CONTRACT_ACCT_OBJ_T> collection) {
        this();
        this.addAll(collection);
    }
}
