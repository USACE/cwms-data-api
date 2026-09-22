package cwms.cda.data.dao.userlists;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.name;
import static org.jooq.impl.DSL.table;

import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Table;

public final class AvUserListMembers {
    public static final Table<Record> AV_USER_LIST_MEMBERS = table(
            name("CWMS_20", "AV_USER_LIST_MEMBERS"));
    public static final Field<String> OFFICE_ID = field(
            name(AV_USER_LIST_MEMBERS.getName(), "OFFICE_ID"), String.class);
    public static final Field<String> USER_LIST_ID = field(
            name(AV_USER_LIST_MEMBERS.getName(), "USER_LIST_ID"), String.class);
    public static final Field<String> USER_ID = field(
            name(AV_USER_LIST_MEMBERS.getName(), "USER_ID"), String.class);
    public static final Field<String> FULL_NAME = field(
            name(AV_USER_LIST_MEMBERS.getName(), "FULL_NAME"), String.class);
    public static final Field<String> EMAIL = field(
            name(AV_USER_LIST_MEMBERS.getName(), "EMAIL"), String.class);

    private AvUserListMembers() {
    }
}
