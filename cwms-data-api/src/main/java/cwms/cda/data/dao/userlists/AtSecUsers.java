package cwms.cda.data.dao.userlists;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.name;
import static org.jooq.impl.DSL.table;

import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Table;

public final class AtSecUsers {
    public static final Table<Record> AT_SEC_USERS = table(name("CWMS_20", "AT_SEC_USERS"));
    public static final Field<String> USERNAME = field(
            name(AT_SEC_USERS.getName(), "USERNAME"), String.class);
    public static final Field<Long> DB_OFFICE_CODE = field(
            name(AT_SEC_USERS.getName(), "DB_OFFICE_CODE"), Long.class);
    public static final Field<Long> USER_GROUP_CODE = field(
            name(AT_SEC_USERS.getName(), "USER_GROUP_CODE"), Long.class);

    private AtSecUsers() {
    }
}
