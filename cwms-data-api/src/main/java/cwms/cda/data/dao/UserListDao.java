package cwms.cda.data.dao;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.name;
import static org.jooq.impl.DSL.selectOne;
import static org.jooq.impl.DSL.table;
import static org.jooq.impl.DSL.upper;

import cwms.cda.api.errors.NotFoundException;
import cwms.cda.data.dto.auth.userlists.UserList;
import cwms.cda.data.dto.auth.userlists.UserListMember;
import cwms.cda.data.dto.auth.userlists.UserListMembers;
import java.sql.Timestamp;
import java.util.List;
import java.util.Optional;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Table;

public final class UserListDao extends Dao<UserListMember> {

    private final Table<?> avUserListMembers = table(name("CWMS_20", "AV_USER_LIST_MEMBERS")).as("ulm");
    private final Table<?> atUserLists = table(name("CWMS_20", "AT_USER_LISTS")).as("ul");
    private final Table<?> cwmsOffice = table(name("CWMS_20", "CWMS_OFFICE")).as("co");

    public UserListDao(DSLContext dsl) {
        super(dsl);
    }

    @Override
    public Optional<UserListMember> getByUniqueName(String uniqueName, String office) {
        return Optional.empty();
    }

    public Optional<UserList> getUserList(String officeId, String userListId) {
        Field<Long> listOfficeCode = field(name(atUserLists.getName(), "DB_OFFICE_CODE"), Long.class);
        Field<String> listUserListId = field(name(atUserLists.getName(), "USER_LIST_ID"), String.class);
        Field<String> listDescription = field(name(atUserLists.getName(), "USER_LIST_DESC"), String.class);
        Field<String> listOwner = field(name(atUserLists.getName(), "OWNED_BY_USERID"), String.class);
        Field<Timestamp> listCreatedAt = field(name(atUserLists.getName(), "CREATED_AT"), Timestamp.class);
        Field<Timestamp> listUpdatedAt = field(name(atUserLists.getName(), "UPDATED_AT"), Timestamp.class);
        Field<Long> officeCode = field(name(cwmsOffice.getName(), "OFFICE_CODE"), Long.class);
        Field<String> officeName = field(name(cwmsOffice.getName(), "OFFICE_ID"), String.class);

        return dsl.select(officeName, listUserListId, listDescription, listOwner, listCreatedAt, listUpdatedAt)
                .from(atUserLists)
                .join(cwmsOffice).on(listOfficeCode.eq(officeCode))
                .where(ignoreCaseEq(officeName, officeId))
                .and(ignoreCaseEq(listUserListId, userListId))
                .fetchOptional(record -> new UserList(
                        record.get(officeName),
                        record.get(listUserListId),
                        record.get(listDescription),
                        record.get(listOwner),
                        record.get(listCreatedAt).toInstant(),
                        Optional.ofNullable(record.get(listUpdatedAt)).map(Timestamp::toInstant).orElse(null)
                ));
    }

    public UserListMembers getMembers(String officeId, String userListId) {
        if (!userListExists(officeId, userListId)) {
            throw new NotFoundException("User list not found: " + officeId + "/" + userListId);
        }

        Field<String> viewOfficeId = field(name(avUserListMembers.getName(), "OFFICE_ID"), String.class);
        Field<String> viewUserListId = field(name(avUserListMembers.getName(), "USER_LIST_ID"), String.class);
        Field<String> viewUserId = field(name(avUserListMembers.getName(), "USER_ID"), String.class);
        Field<String> viewFullName = field(name(avUserListMembers.getName(), "FULL_NAME"), String.class);
        Field<String> viewEmail = field(name(avUserListMembers.getName(), "EMAIL"), String.class);

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
        Field<Long> listOfficeCode = field(name(atUserLists.getName(), "DB_OFFICE_CODE"), Long.class);
        Field<String> listUserListId = field(name(atUserLists.getName(), "USER_LIST_ID"), String.class);
        Field<Long> officeCode = field(name(cwmsOffice.getName(), "OFFICE_CODE"), Long.class);
        Field<String> officeName = field(name(cwmsOffice.getName(), "OFFICE_ID"), String.class);

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
