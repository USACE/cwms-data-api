package cwms.cda.data.dao;

import static org.jooq.impl.DSL.selectOne;
import static org.jooq.impl.DSL.upper;

import cwms.cda.api.errors.NotFoundException;
import cwms.cda.data.dao.userlists.AtSecCwmsUsers;
import cwms.cda.data.dao.userlists.AtSecUserGroups;
import cwms.cda.data.dao.userlists.AtSecUsers;
import cwms.cda.data.dao.userlists.AtUserListMembers;
import cwms.cda.data.dao.userlists.AtUserLists;
import cwms.cda.data.dao.userlists.AvUserListMembers;
import cwms.cda.data.dto.auth.userlists.UserList;
import cwms.cda.data.dto.auth.userlists.UserListCandidate;
import cwms.cda.data.dto.auth.userlists.UserListMember;
import cwms.cda.data.dto.auth.userlists.UserListMembers;
import io.javalin.http.ConflictResponse;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.impl.DSL;
import usace.cwms.db.jooq.codegen.tables.AV_CWMS_USER;
import usace.cwms.db.jooq.codegen.tables.CWMS_OFFICE;

public final class UserListDao {

    private final DSLContext dsl;

    public UserListDao(DSLContext dsl) {
        this.dsl = dsl;
    }

    public Optional<UserList> getUserList(String officeId, String userListId) {
        CWMS_OFFICE office = CWMS_OFFICE.CWMS_OFFICE;
        return dsl.select(office.OFFICE_ID, AtUserLists.USER_LIST_ID,
                        AtUserLists.USER_LIST_DESC, AtUserLists.OWNED_BY_USERID,
                        AtUserLists.CREATED_AT, AtUserLists.UPDATED_AT)
                .from(AtUserLists.AT_USER_LISTS)
                .join(office).on(AtUserLists.DB_OFFICE_CODE.eq(office.OFFICE_CODE))
                .where(ignoreCaseEq(office.OFFICE_ID, officeId))
                .and(ignoreCaseEq(AtUserLists.USER_LIST_ID, userListId))
                .fetchOptional(record -> new UserList(
                        record.get(office.OFFICE_ID),
                        record.get(AtUserLists.USER_LIST_ID),
                        record.get(AtUserLists.USER_LIST_DESC),
                        record.get(AtUserLists.OWNED_BY_USERID),
                        record.get(AtUserLists.CREATED_AT).toInstant(),
                        Optional.ofNullable(record.get(AtUserLists.UPDATED_AT))
                                .map(java.sql.Timestamp::toInstant).orElse(null)
                ));
    }

    public List<UserList> getUserLists(String officeId) {
        CWMS_OFFICE office = CWMS_OFFICE.CWMS_OFFICE;
        return dsl.select(office.OFFICE_ID, AtUserLists.USER_LIST_ID,
                        AtUserLists.USER_LIST_DESC, AtUserLists.OWNED_BY_USERID,
                        AtUserLists.CREATED_AT, AtUserLists.UPDATED_AT)
                .from(AtUserLists.AT_USER_LISTS)
                .join(office).on(AtUserLists.DB_OFFICE_CODE.eq(office.OFFICE_CODE))
                .where(ignoreCaseEq(office.OFFICE_ID, officeId))
                .orderBy(AtUserLists.USER_LIST_ID)
                .fetch(record -> new UserList(
                        record.get(office.OFFICE_ID),
                        record.get(AtUserLists.USER_LIST_ID),
                        record.get(AtUserLists.USER_LIST_DESC),
                        record.get(AtUserLists.OWNED_BY_USERID),
                        record.get(AtUserLists.CREATED_AT).toInstant(),
                        Optional.ofNullable(record.get(AtUserLists.UPDATED_AT))
                                .map(java.sql.Timestamp::toInstant).orElse(null)
                ));
    }

    public UserList createUserList(String officeId, String userListId, String description,
            String owner) {
        if (userListExists(officeId, userListId)) {
            throw new ConflictResponse("User list already exists: " + officeId + "/" + userListId);
        }
        return dsl.transactionResult(configuration -> {
            UserListDao transactionDao = new UserListDao(DSL.using(configuration));
            return transactionDao.insertUserList(officeId, userListId, description, owner);
        });
    }

    private UserList insertUserList(String officeId, String userListId, String description,
            String owner) {
        Long resolvedOfficeCode = resolveOfficeCode(officeId);
        dsl.insertInto(AtUserLists.AT_USER_LISTS)
                .columns(AtUserLists.DB_OFFICE_CODE, AtUserLists.USER_LIST_ID,
                        AtUserLists.USER_LIST_DESC, AtUserLists.OWNED_BY_USERID)
                .values(resolvedOfficeCode, normalizeId(userListId), description, normalizeId(owner))
                .execute();
        return getUserList(officeId, userListId)
                .orElseThrow(() -> new NotFoundException("Created user list was not found"));
    }

    public UserList updateUserList(String officeId, String userListId, String description) {
        Long resolvedOfficeCode = resolveOfficeCode(officeId);
        int changed = dsl.update(AtUserLists.AT_USER_LISTS)
                .set(AtUserLists.USER_LIST_DESC, description)
                .where(AtUserLists.DB_OFFICE_CODE.eq(resolvedOfficeCode))
                .and(ignoreCaseEq(AtUserLists.USER_LIST_ID, userListId))
                .execute();
        if (changed == 0) {
            throw new NotFoundException("User list not found: " + officeId + "/" + userListId);
        }
        return getUserList(officeId, userListId).orElseThrow();
    }

    public void deleteUserList(String officeId, String userListId) {
        if (!userListExists(officeId, userListId)) {
            throw new NotFoundException("User list not found: " + officeId + "/" + userListId);
        }
        dsl.transaction(configuration -> new UserListDao(DSL.using(configuration))
                .deleteUserListRows(officeId, userListId));
    }

    private void deleteUserListRows(String officeId, String userListId) {
        Long resolvedOfficeCode = resolveOfficeCode(officeId);
        dsl.deleteFrom(AtUserListMembers.AT_USER_LIST_MEMBERS)
                .where(AtUserListMembers.DB_OFFICE_CODE.eq(resolvedOfficeCode))
                .and(ignoreCaseEq(AtUserListMembers.USER_LIST_ID, userListId))
                .execute();
        int deleted = dsl.deleteFrom(AtUserLists.AT_USER_LISTS)
                .where(AtUserLists.DB_OFFICE_CODE.eq(resolvedOfficeCode))
                .and(ignoreCaseEq(AtUserLists.USER_LIST_ID, userListId))
                .execute();
        if (deleted == 0) {
            throw new NotFoundException("User list not found: " + officeId + "/" + userListId);
        }
    }

    public boolean isOfficeUserAdmin(String username, String officeId) {
        CWMS_OFFICE office = CWMS_OFFICE.CWMS_OFFICE;
        return dsl.fetchExists(selectOne()
                .from(AtSecUsers.AT_SEC_USERS)
                .join(AtSecUserGroups.AT_SEC_USER_GROUPS)
                .on(AtSecUsers.DB_OFFICE_CODE.eq(AtSecUserGroups.DB_OFFICE_CODE)
                        .and(AtSecUsers.USER_GROUP_CODE.eq(AtSecUserGroups.USER_GROUP_CODE)))
                .join(office).on(AtSecUsers.DB_OFFICE_CODE.eq(office.OFFICE_CODE))
                .where(ignoreCaseEq(AtSecUsers.USERNAME, username))
                .and(ignoreCaseEq(office.OFFICE_ID, officeId))
                .and(ignoreCaseEq(AtSecUserGroups.USER_GROUP_ID, "CWMS User Admins")));
    }

    public UserListMembers getMembers(String officeId, String userListId) {
        if (!userListExists(officeId, userListId)) {
            throw new NotFoundException("User list not found: " + officeId + "/" + userListId);
        }

        List<UserListMember> members = dsl.select(AvUserListMembers.OFFICE_ID,
                        AvUserListMembers.USER_LIST_ID, AvUserListMembers.USER_ID,
                        AvUserListMembers.FULL_NAME, AvUserListMembers.EMAIL)
                .from(AvUserListMembers.AV_USER_LIST_MEMBERS)
                .where(ignoreCaseEq(AvUserListMembers.OFFICE_ID, officeId))
                .and(ignoreCaseEq(AvUserListMembers.USER_LIST_ID, userListId))
                .orderBy(AvUserListMembers.FULL_NAME.asc().nullsLast(),
                        AvUserListMembers.USER_ID.asc())
                .fetch(record -> new UserListMember(
                        record.get(AvUserListMembers.OFFICE_ID),
                        record.get(AvUserListMembers.USER_LIST_ID),
                        record.get(AvUserListMembers.USER_ID),
                        record.get(AvUserListMembers.FULL_NAME),
                        record.get(AvUserListMembers.EMAIL)
                ));

        return new UserListMembers(members);
    }

    public List<UserListCandidate> searchCandidates(String search, int pageSize) {
        AV_CWMS_USER user = AV_CWMS_USER.AV_CWMS_USER;
        String contains = "%" + search.toUpperCase(Locale.ROOT)
                .replace("\\", "\\\\")
                .replace("%", "\\%")
                .replace("_", "\\_") + "%";
        Condition matches = upper(user.USER_ID).like(contains, '\\')
                .or(upper(user.FULL_NAME).like(contains, '\\'))
                .or(upper(user.EMAIL).like(contains, '\\'));
        return dsl.select(user.USER_ID, user.FULL_NAME, user.EMAIL, user.OFFICE_ID)
                .from(user)
                .where(matches)
                .orderBy(user.FULL_NAME.asc().nullsLast(), user.USER_ID)
                .limit(pageSize)
                .fetch(record -> new UserListCandidate(
                        record.get(user.USER_ID),
                        record.get(user.FULL_NAME),
                        record.get(user.EMAIL),
                        record.get(user.OFFICE_ID)));
    }

    public UserListMember addMember(String officeId, String userListId, String userId,
            String addedBy) {
        if (!userListExists(officeId, userListId)) {
            throw new NotFoundException("User list not found: " + officeId + "/" + userListId);
        }
        boolean userExists = dsl.fetchExists(selectOne().from(AtSecCwmsUsers.AT_SEC_CWMS_USERS)
                .where(ignoreCaseEq(AtSecCwmsUsers.USERID, userId)));
        if (!userExists) {
            throw new NotFoundException("CWMS user not found: " + userId);
        }

        Long resolvedOfficeCode = resolveOfficeCode(officeId);
        boolean memberExists = dsl.fetchExists(selectOne()
                .from(AtUserListMembers.AT_USER_LIST_MEMBERS)
                .where(AtUserListMembers.DB_OFFICE_CODE.eq(resolvedOfficeCode))
                .and(ignoreCaseEq(AtUserListMembers.USER_LIST_ID, userListId))
                .and(ignoreCaseEq(AtUserListMembers.USERID, userId)));
        if (memberExists) {
            throw new ConflictResponse("User is already a member of "
                    + officeId + "/" + userListId + ": " + userId);
        }
        dsl.insertInto(AtUserListMembers.AT_USER_LIST_MEMBERS)
                .columns(AtUserListMembers.DB_OFFICE_CODE, AtUserListMembers.USER_LIST_ID,
                        AtUserListMembers.USERID, AtUserListMembers.ADDED_BY_USERID)
                .values(resolvedOfficeCode, normalizeId(userListId), normalizeId(userId),
                        normalizeId(addedBy))
                .execute();
        return getMembers(officeId, userListId).getMembers().stream()
                .filter(member -> member.getUserId().equalsIgnoreCase(userId))
                .findFirst()
                .orElseThrow(() -> new NotFoundException("Added member was not found"));
    }

    public void removeMember(String officeId, String userListId, String userId) {
        if (!userListExists(officeId, userListId)) {
            throw new NotFoundException("User list not found: " + officeId + "/" + userListId);
        }
        Long resolvedOfficeCode = resolveOfficeCode(officeId);
        int deleted = dsl.deleteFrom(AtUserListMembers.AT_USER_LIST_MEMBERS)
                .where(AtUserListMembers.DB_OFFICE_CODE.eq(resolvedOfficeCode))
                .and(ignoreCaseEq(AtUserListMembers.USER_LIST_ID, userListId))
                .and(ignoreCaseEq(AtUserListMembers.USERID, userId))
                .execute();
        if (deleted == 0) {
            throw new NotFoundException("User list member not found: " + userId);
        }
    }

    private boolean userListExists(String officeId, String userListId) {
        CWMS_OFFICE office = CWMS_OFFICE.CWMS_OFFICE;
        return dsl.fetchExists(
                selectOne()
                        .from(AtUserLists.AT_USER_LISTS)
                        .join(office).on(AtUserLists.DB_OFFICE_CODE.eq(office.OFFICE_CODE))
                        .where(ignoreCaseEq(office.OFFICE_ID, officeId))
                        .and(ignoreCaseEq(AtUserLists.USER_LIST_ID, userListId))
        );
    }

    private static Condition ignoreCaseEq(Field<String> field, String value) {
        return upper(field).eq(normalizeId(value));
    }

    private Long resolveOfficeCode(String officeId) {
        CWMS_OFFICE office = CWMS_OFFICE.CWMS_OFFICE;
        return dsl.select(office.OFFICE_CODE)
                .from(office)
                .where(ignoreCaseEq(office.OFFICE_ID, officeId))
                .fetchOptional(office.OFFICE_CODE)
                .orElseThrow(() -> new NotFoundException("Office not found: " + officeId));
    }

    private static String normalizeId(String value) {
        return value.toUpperCase(Locale.ROOT);
    }
}
