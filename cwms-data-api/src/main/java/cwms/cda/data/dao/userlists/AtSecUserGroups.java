package cwms.cda.data.dao.userlists;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.name;
import static org.jooq.impl.DSL.table;

import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Table;

public final class AtSecUserGroups {
    public static final Table<Record> AT_SEC_USER_GROUPS = table(
            name("CWMS_20", "AT_SEC_USER_GROUPS"));
    public static final Field<Long> DB_OFFICE_CODE = field(
            name(AT_SEC_USER_GROUPS.getName(), "DB_OFFICE_CODE"), Long.class);
    public static final Field<Long> USER_GROUP_CODE = field(
            name(AT_SEC_USER_GROUPS.getName(), "USER_GROUP_CODE"), Long.class);
    public static final Field<String> USER_GROUP_ID = field(
            name(AT_SEC_USER_GROUPS.getName(), "USER_GROUP_ID"), String.class);

    private AtSecUserGroups() {
    }
}
