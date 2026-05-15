package cwms.cda.data.dao;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.name;
import static org.jooq.impl.DSL.selectOne;
import static org.jooq.impl.DSL.table;
import static org.jooq.impl.DSL.upper;

import cwms.cda.api.errors.NotFoundException;
import cwms.cda.data.dto.auth.userlists.UserListMember;
import cwms.cda.data.dto.auth.userlists.UserListMembers;
import java.util.List;
import java.util.Optional;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Table;

public final class UserListDao extends Dao<UserListMember> {

    private final Table<?> avUserListMembers = table(name("cwms_20", "av_user_list_members")).as("ulm");
    private final Table<?> atUserLists = table(name("cwms_20", "at_user_lists")).as("ul");
    private final Table<?> cwmsOffice = table(name("cwms_20", "cwms_office")).as("co");

    public UserListDao(DSLContext dsl) {
        super(dsl);
    }

    @Override
    public Optional<UserListMember> getByUniqueName(String uniqueName, String office) {
        return Optional.empty();
    }

    public UserListMembers getMembers(String officeId, String userListId) {
        if (!userListExists(officeId, userListId)) {
            throw new NotFoundException("User list not found: " + officeId + "/" + userListId);
        }

        Field<String> viewOfficeId = field(name(avUserListMembers.getName(), "office_id"), String.class);
        Field<String> viewUserListId = field(name(avUserListMembers.getName(), "user_list_id"), String.class);
        Field<String> viewUserId = field(name(avUserListMembers.getName(), "user_id"), String.class);
        Field<String> viewFullName = field(name(avUserListMembers.getName(), "full_name"), String.class);
        Field<String> viewEmail = field(name(avUserListMembers.getName(), "email"), String.class);

        List<UserListMember> members = dsl.select(viewOfficeId, viewUserListId, viewUserId, viewFullName,
                    viewEmail)
                .from(avUserListMembers)
                .where(ignoreCaseEq(viewOfficeId, officeId))
                .and(ignoreCaseEq(viewUserListId, userListId))
                .orderBy(viewFullName.asc().nullsLast(), viewUserId.asc())
                .fetch(record -> new UserListMember(
                        record.get(viewOfficeId),
                        record.get(viewUserListId),
                        record.get(viewUserId),
                        record.get(viewFullName),
                        record.get(viewEmail)
                ));

        return new UserListMembers(members);
    }

    private boolean userListExists(String officeId, String userListId) {
        Field<Number> listOfficeCode = field(name(atUserLists.getName(), "db_office_code"), Number.class);
        Field<String> listUserListId = field(name(atUserLists.getName(), "user_list_id"), String.class);
        Field<Number> officeCode = field(name(cwmsOffice.getName(), "office_code"), Number.class);
        Field<String> officeName = field(name(cwmsOffice.getName(), "office_id"), String.class);

        return dsl.fetchExists(
                selectOne()
                        .from(atUserLists)
                        .join(cwmsOffice).on(listOfficeCode.eq(officeCode))
                        .where(ignoreCaseEq(officeName, officeId))
                        .and(ignoreCaseEq(listUserListId, userListId))
        );
    }

    private static Condition ignoreCaseEq(Field<String> field, String value) {
        return upper(field).eq(value.toUpperCase());
    }
}
