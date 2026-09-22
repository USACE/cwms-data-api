package cwms.cda.data.dao.userlists;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.name;
import static org.jooq.impl.DSL.table;

import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Table;

public final class AtUserListMembers {
    public static final Table<Record> AT_USER_LIST_MEMBERS = table(
            name("CWMS_20", "AT_USER_LIST_MEMBERS"));
    public static final Field<Long> DB_OFFICE_CODE = field(
            name(AT_USER_LIST_MEMBERS.getName(), "DB_OFFICE_CODE"), Long.class);
    public static final Field<String> USER_LIST_ID = field(
            name(AT_USER_LIST_MEMBERS.getName(), "USER_LIST_ID"), String.class);
    public static final Field<String> USERID = field(
            name(AT_USER_LIST_MEMBERS.getName(), "USERID"), String.class);
    public static final Field<String> ADDED_BY_USERID = field(
            name(AT_USER_LIST_MEMBERS.getName(), "ADDED_BY_USERID"), String.class);

    private AtUserListMembers() {
    }
}
