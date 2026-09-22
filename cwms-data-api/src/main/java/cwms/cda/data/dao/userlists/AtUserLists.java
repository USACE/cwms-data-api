package cwms.cda.data.dao.userlists;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.name;
import static org.jooq.impl.DSL.table;

import java.sql.Timestamp;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Table;

public final class AtUserLists {
    public static final Table<Record> AT_USER_LISTS = table(name("CWMS_20", "AT_USER_LISTS"));
    public static final Field<Long> DB_OFFICE_CODE = field(
            name(AT_USER_LISTS.getName(), "DB_OFFICE_CODE"), Long.class);
    public static final Field<String> USER_LIST_ID = field(
            name(AT_USER_LISTS.getName(), "USER_LIST_ID"), String.class);
    public static final Field<String> USER_LIST_DESC = field(
            name(AT_USER_LISTS.getName(), "USER_LIST_DESC"), String.class);
    public static final Field<String> OWNED_BY_USERID = field(
            name(AT_USER_LISTS.getName(), "OWNED_BY_USERID"), String.class);
    public static final Field<Timestamp> CREATED_AT = field(
            name(AT_USER_LISTS.getName(), "CREATED_AT"), Timestamp.class);
    public static final Field<Timestamp> UPDATED_AT = field(
            name(AT_USER_LISTS.getName(), "UPDATED_AT"), Timestamp.class);

    private AtUserLists() {
    }
}
