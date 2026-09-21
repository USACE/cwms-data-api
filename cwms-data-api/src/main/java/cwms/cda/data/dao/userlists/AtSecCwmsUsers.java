package cwms.cda.data.dao.userlists;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.name;
import static org.jooq.impl.DSL.table;

import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Table;

public final class AtSecCwmsUsers {
    public static final Table<Record> AT_SEC_CWMS_USERS = table(
            name("CWMS_20", "AT_SEC_CWMS_USERS"));
    public static final Field<String> USERID = field(
            name(AT_SEC_CWMS_USERS.getName(), "USERID"), String.class);

    private AtSecCwmsUsers() {
    }
}
