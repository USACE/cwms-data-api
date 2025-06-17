package cwms.cda.data.dao;

import static org.jooq.impl.DSL.*;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.jooq.CommonTableExpression;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Record1;
import org.jooq.Record4;
import org.jooq.Record5;
import org.jooq.SelectConditionStep;
import org.jooq.SelectLimitPercentStep;
import org.jooq.SelectSeekStep2;
import org.jooq.Table;
import org.jooq.conf.ParamType;
import org.jooq.impl.DSL;

import com.google.common.flogger.FluentLogger;

import cwms.cda.data.dto.Clob;
import cwms.cda.data.dto.Clobs;
import cwms.cda.data.dto.CwmsDTOPaginated;
import cwms.cda.data.dto.auth.users.User;
import cwms.cda.data.dto.auth.users.Users;
import cwms.cda.security.CwmsAuthException;
import cwms.cda.security.DataApiPrincipal;
import usace.cwms.db.jooq.codegen.tables.AV_SEC_USERS;

public class UserDao extends JooqDao<User> {
    public static final FluentLogger logger = FluentLogger.forEnclosingClass();

    private static final String GET_USER =
        "select ut.userid as username,ut.email, ut.principle_name,groups.db_office_id as \"office\",groups.user_group_id as \"role\" " +
        " from cwms_20.at_sec_cwms_users ut " +
        "left join cwms_20.av_sec_users groups on ut.userid=groups.username and is_member='T' " +
        "where ut.principle_name = ? or upper(ut.userid) = upper(?) " +
        "order by \"office\", \"role\""
        ;

    private final Table<?> AT_SEC_CWMS_USERS = table("cwms_20.at_sec_cwms_users","userid", "email","principle_name");

    public UserDao(DSLContext dsl) {
        super(dsl);
    }

    @Override
    public Optional<User> getByUniqueName(String uniqueName, String cac_role) {
        return Optional.of(dsl.connectionResult(c -> {
                AuthDao.setSessionForAuthCheck(c);
                try (PreparedStatement getUser = c.prepareStatement(GET_USER)) {
                    getUser.setString(1, uniqueName);
                    getUser.setString(2, uniqueName);
                    String userName = null;
                    String principalName = null;
                    String email = null;
                    final Map<String,List<String>> roles = new HashMap<>();
                    try (ResultSet rs = getUser.executeQuery()) {
                        if (rs.isBeforeFirst()) {
                            while(rs.next()) {
                                if (userName == null) {
                                    userName = rs.getString("username");
                                    principalName = rs.getString("principle_name");
                                    email = rs.getString("email");
                                }
                                final String roleOffice = rs.getString("office");
                                final String role = rs.getString("role");
                                if (role != null) {
                                    roles.computeIfAbsent(roleOffice, (key) -> new ArrayList<>()).add(role);
                                }
                            }
                            logger.atInfo().log("Building user object.");
                           return new User(userName, principalName, email, cac_role != null,  roles);
                        } else {
                            return (User)null;
                        }
                    }
                }
            })
        );

    }

    public void addRoles(DataApiPrincipal p, String user, String office, String[] roles) {
        dsl.connection(c -> {
            setOffice(c, office);
            try (CallableStatement addUser = c.prepareCall("call cwms_20.cwms_sec.add_user_to_group(?,?,?)")) {
                for (String role: roles) {
                    addUser.setString(1, user);
                    addUser.setString(2, role);
                    addUser.setString(3, office);
                    addUser.addBatch();
                }
                addUser.executeBatch();
            }
        });
        logger.atInfo().log("Roles '%s' added for user '%s' and office '%s'", String.join(",", roles), user, office);
    }

    public void deleteRoles(DataApiPrincipal p, String user, String office, String[] roles) {
        dsl.connection(c -> {
            setOffice(c, office);
            try (CallableStatement addUser = c.prepareCall("call cwms_20.cwms_sec.remove_user_from_group(?,?,?)")) {
                for (String role: roles) {
                    addUser.setString(1, user);
                    addUser.setString(2, role);
                    addUser.setString(3, office);
                    addUser.addBatch();
                }
                addUser.executeBatch();
            }
        });
        logger.atInfo().log("Roles '%s' removed for user '%s' and office '%s'", String.join(",", roles), user, office);
    }

    public List<String> getRoles() {
        return dsl.connectionResult(c -> {
            AuthDao.setSessionForAuthCheck(c);
            final ArrayList<String> roles = new ArrayList<>();
            try (PreparedStatement getUser = c.prepareStatement("select distinct user_group_id from cwms_20.at_sec_user_groups order by user_group_id asc");
                 ResultSet result = getUser.executeQuery()) {
                while(result.next()) {
                    roles.add(result.getString("user_group_id"));
                }
                return roles;
            }
        });
    }

    public Users getAll(String cursor, int pageSize, String office, boolean includeRoles) {
        final AV_SEC_USERS vUserGroups = AV_SEC_USERS.AV_SEC_USERS.as("ug");
        final Table<?> vUsers = AT_SEC_CWMS_USERS.as("ut");
        final Field<String> userId = field(name(vUsers.getName(),"USERID"), String.class);
        final Field<String> email = field(name(vUsers.getName(),"EMAIL"), String.class);
        final Field<String> principal = field(name(vUsers.getName(),"PRINCIPLE_NAME"), String.class);

        return connectionResult(dsl, c -> {
            AuthDao.setSessionForAuthCheck(c);
            final DSLContext dsl = JooqDao.getDslContext(c, null);

            int total = 0;
            String cursorUserId = null;
            int pageSizeTmp = pageSize;
            String limitOffice = null;

            Condition whereClause = office == null ? DSL.noCondition()
            // If we are including only those users with permissions to a  specific office
            // we limit to those users that also have an entry in the at_sec_cwms_users_group table.
            : dsl.select(count(asterisk()))
                 .from(vUserGroups)
                 .where(upper(vUserGroups.DB_OFFICE_ID).eq(upper(office)))
                 .and(vUserGroups.IS_MEMBER.eq("T")).asField().gt(1)
            ;
/// TODO: instead of where clause we establish the join or not. If not limiting by office we don't
/// join on the vUserGroups table at this point.
            if (cursor == null || cursor.isEmpty()) {
                SelectConditionStep<Record1<Integer>> count = dsl.select(count(asterisk()))
                    .from(vUsers)
                    //.leftOuterJoin(vUserGroups).on(userId.eq(vUserGroups.USERNAME))
                    .where(whereClause);
                Record1<Integer> rec = count.fetchOne();
                if(rec != null) {
                    total = rec.value1();
                }
            } else {
                final String[] parts = CwmsDTOPaginated.decodeCursor(cursor, "||");

                logger.atFine().log("decoded cursor: " + String.join("||", parts));
                for (String p : parts) {
                    logger.atFinest().log(p);
                }

                if (parts.length > 1) {
                    cursorUserId = parts[0];
                    total = Integer.parseInt(parts[2]);
                    pageSizeTmp = Integer.parseInt(parts[1]);
                    limitOffice = parts[3];

                    // Rebuild the where clause to match the initial conditions
                    whereClause = limitOffice == null ? DSL.noCondition()
                    // If we are including only those users with permissions to a  specific office
                    // we limit to those users that also have an entry in the at_sec_cwms_users_group table.
                    : dsl.select(count(asterisk()))
                        .from(vUserGroups)
                        .where(upper(vUserGroups.DB_OFFICE_ID).eq(upper(limitOffice)))
                        .and(vUserGroups.IS_MEMBER.eq("T")).asField().gt(1);
                }
            }

            Condition pagingCondition = cursorUserId == null
                ? noCondition()
                : userId.greaterThan(cursorUserId);

            CommonTableExpression<?> limiter = name("limiter")
                                                .fields(userId.getName(),
                                                        email.getName(),
                                                        principal.getName())
                                                .as(
                                                    select(userId, email,principal)
                                                    .from(vUsers)
                                                    .where(whereClause)
                                                    .and(pagingCondition)
                                                    .orderBy(userId)
                                                    .limit(pageSize)
                                                );
            Field<String> limitUserId = field(name(limiter.getName(), userId.getName()), String.class);
            Field<String> limitEmail = field(name(limiter.getName(), email.getName()), String.class);
            Field<String> limitPrincipal = field(name(limiter.getName(), principal.getName()), String.class);
            SelectSeekStep2<Record5<String, String, String, String, String>, String, String> query = dsl.with(limiter).select(
                    limitUserId,
                    limitEmail,
                    limitPrincipal,
                    vUserGroups.DB_OFFICE_ID,
                    vUserGroups.USER_GROUP_ID
                )
                .from(limiter)
                .leftOuterJoin(vUserGroups).on(limitUserId.eq(vUserGroups.USERNAME)
                                        .and(vUserGroups.IS_MEMBER.eq("T")))
                // office id is included in order by to maintain consistent ordering.
                // office id It is not used in the pagination clause as we are only
                // paging on "users" not their roles, which is the only element with the office
                // association and always fully included per use in the response.
                .orderBy(limitUserId, vUserGroups.DB_OFFICE_ID)
                ;
                

            logger.atInfo().log(query.getSQL(ParamType.INLINED));


            final Users.Builder builder = new Users.Builder(cursor, pageSizeTmp, total, limitOffice);

            final HashMap<String, User.Builder> tmpUsers = new HashMap<>();

            query.fetch().forEach(row -> {
                User.Builder userBuilder = tmpUsers.computeIfAbsent(row.get(userId), (key) -> {
                    return new User.Builder(key, row.get(principal),row.get(principal), null);
                });
                final String roleOffice = row.get(vUserGroups.DB_OFFICE_ID);
                final String role = row.get(vUserGroups.USER_GROUP_ID);
                if (roleOffice != null) {
                    userBuilder.addRole(roleOffice, role);
                }

            });

            tmpUsers.entrySet().stream().map(e -> e.getValue().build()).forEach(builder::addUser);
            
            return builder.build();
        });
    }
}
