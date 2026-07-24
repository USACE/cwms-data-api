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
    private final Table<?> atSecUsers = table(name("CWMS_20", "AT_SEC_USERS")).as("su");
    private final Table<?> atSecUserGroups = table(name("CWMS_20", "AT_SEC_USER_GROUPS")).as("sg");

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

    public List<UserList> getUserLists(String officeId) {
        Field<Long> listOfficeCode = field(name(atUserLists.getName(), "DB_OFFICE_CODE"), Long.class);
        Field<String> listUserListId = field(name(atUserLists.getName(), "USER_LIST_ID"), String.class);
        Field<String> listDescription = field(name(atUserLists.getName(), "USER_LIST_DESC"), String.class);
        Field<String> listOwner = field(name(atUserLists.getName(), "OWNED_BY_USERID"), String.class);
        Field<Timestamp> listCreatedAt = field(name(atUserLists.getName(), "CREATED_AT"), Timestamp.class);
        Field<Timestamp> listUpdatedAt = field(name(atUserLists.getName(), "UPDATED_AT"), Timestamp.class);
        Field<Long> officeCode = field(name(cwmsOffice.getName(), "OFFICE_CODE"), Long.class);
        Field<String> officeName = field(name(cwmsOffice.getName(), "OFFICE_ID"), String.class);

        return dsl.select(officeName, listUserListId, listDescription, listOwner,
                        listCreatedAt, listUpdatedAt)
                .from(atUserLists)
                .join(cwmsOffice).on(listOfficeCode.eq(officeCode))
                .where(ignoreCaseEq(officeName, officeId))
                .orderBy(listUserListId)
                .fetch(record -> new UserList(
                        record.get(officeName),
                        record.get(listUserListId),
                        record.get(listDescription),
                        record.get(listOwner),
                        record.get(listCreatedAt).toInstant(),
                        Optional.ofNullable(record.get(listUpdatedAt))
                                .map(Timestamp::toInstant).orElse(null)
                ));
    }

    public UserList createUserList(String officeId, String userListId, String description,
            String owner) {
        Field<Long> listOfficeCode = field(name(atUserLists.getName(), "DB_OFFICE_CODE"), Long.class);
        Field<String> listUserListId = field(name(atUserLists.getName(), "USER_LIST_ID"), String.class);
        Field<String> listDescription = field(name(atUserLists.getName(), "USER_LIST_DESC"), String.class);
        Field<String> listOwner = field(name(atUserLists.getName(), "OWNED_BY_USERID"), String.class);
        Field<Long> officeCode = field(name(cwmsOffice.getName(), "OFFICE_CODE"), Long.class);
        Field<String> officeName = field(name(cwmsOffice.getName(), "OFFICE_ID"), String.class);

        Long resolvedOfficeCode = dsl.select(officeCode)
                .from(cwmsOffice)
                .where(ignoreCaseEq(officeName, officeId))
                .fetchOptional(officeCode)
                .orElseThrow(() -> new NotFoundException("Office not found: " + officeId));
        dsl.insertInto(atUserLists)
                .columns(listOfficeCode, listUserListId, listDescription, listOwner)
                .values(resolvedOfficeCode, userListId.toUpperCase(), description, owner.toUpperCase())
                .execute();
        return getUserList(officeId, userListId)
                .orElseThrow(() -> new NotFoundException("Created user list was not found"));
    }

    public UserList updateUserList(String officeId, String userListId, String description) {
        Field<Long> listOfficeCode = field(name(atUserLists.getName(), "DB_OFFICE_CODE"), Long.class);
        Field<String> listUserListId = field(name(atUserLists.getName(), "USER_LIST_ID"), String.class);
        Field<String> listDescription = field(name(atUserLists.getName(), "USER_LIST_DESC"), String.class);
        Field<Long> officeCode = field(name(cwmsOffice.getName(), "OFFICE_CODE"), Long.class);
        Field<String> officeName = field(name(cwmsOffice.getName(), "OFFICE_ID"), String.class);
        Long resolvedOfficeCode = dsl.select(officeCode).from(cwmsOffice)
                .where(ignoreCaseEq(officeName, officeId)).fetchOne(officeCode);
        int changed = dsl.update(atUserLists)
                .set(listDescription, description)
                .where(listOfficeCode.eq(resolvedOfficeCode))
                .and(ignoreCaseEq(listUserListId, userListId))
                .execute();
        if (changed == 0) {
            throw new NotFoundException("User list not found: " + officeId + "/" + userListId);
        }
        return getUserList(officeId, userListId).orElseThrow();
    }

    public void deleteUserList(String officeId, String userListId) {
        Field<String> memberListId = field(name("AT_USER_LIST_MEMBERS", "USER_LIST_ID"), String.class);
        Table<?> members = table(name("CWMS_20", "AT_USER_LIST_MEMBERS"));
        dsl.deleteFrom(members).where(ignoreCaseEq(memberListId, userListId)).execute();

        Field<Long> listOfficeCode = field(name(atUserLists.getName(), "DB_OFFICE_CODE"), Long.class);
        Field<String> listUserListId = field(name(atUserLists.getName(), "USER_LIST_ID"), String.class);
        Field<Long> officeCode = field(name(cwmsOffice.getName(), "OFFICE_CODE"), Long.class);
        Field<String> officeName = field(name(cwmsOffice.getName(), "OFFICE_ID"), String.class);
        Long resolvedOfficeCode = dsl.select(officeCode).from(cwmsOffice)
                .where(ignoreCaseEq(officeName, officeId)).fetchOne(officeCode);
        int deleted = dsl.deleteFrom(atUserLists)
                .where(listOfficeCode.eq(resolvedOfficeCode))
                .and(ignoreCaseEq(listUserListId, userListId))
                .execute();
        if (deleted == 0) {
            throw new NotFoundException("User list not found: " + officeId + "/" + userListId);
        }
    }

    public boolean isOfficeUserAdmin(String username, String officeId) {
        Field<String> securityUsername = field(name(atSecUsers.getName(), "USERNAME"), String.class);
        Field<Long> securityOfficeCode = field(name(atSecUsers.getName(), "DB_OFFICE_CODE"), Long.class);
        Field<Long> securityGroupCode = field(name(atSecUsers.getName(), "USER_GROUP_CODE"), Long.class);
        Field<Long> groupOfficeCode = field(name(atSecUserGroups.getName(), "DB_OFFICE_CODE"), Long.class);
        Field<Long> groupCode = field(name(atSecUserGroups.getName(), "USER_GROUP_CODE"), Long.class);
        Field<String> groupId = field(name(atSecUserGroups.getName(), "USER_GROUP_ID"), String.class);
        Field<Long> officeCode = field(name(cwmsOffice.getName(), "OFFICE_CODE"), Long.class);
        Field<String> officeName = field(name(cwmsOffice.getName(), "OFFICE_ID"), String.class);
        return dsl.fetchExists(selectOne()
                .from(atSecUsers)
                .join(atSecUserGroups).on(securityOfficeCode.eq(groupOfficeCode)
                        .and(securityGroupCode.eq(groupCode)))
                .join(cwmsOffice).on(securityOfficeCode.eq(officeCode))
                .where(ignoreCaseEq(securityUsername, username))
                .and(ignoreCaseEq(officeName, officeId))
                .and(ignoreCaseEq(groupId, "CWMS User Admins")));
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
