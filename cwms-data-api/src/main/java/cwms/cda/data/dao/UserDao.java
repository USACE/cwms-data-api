package cwms.cda.data.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.jooq.DSLContext;

import com.google.common.flogger.FluentLogger;

import cwms.cda.data.dto.auth.users.User;
import cwms.cda.security.CwmsAuthException;

public class UserDao extends Dao<User> {
    public static final FluentLogger logger = FluentLogger.forEnclosingClass();

    private static final String GET_USER =
        "select ut.userid as username,ut.email, ut.principle_name,groups.db_office_id as office,groups.user_group_id as role " +
        " from cwms_20.at_sec_cwms_users ut " +
        "left outer join cwms_20.av_sec_users groups on ut.userid=groups.username and is_member='T' " +
        "where ut.principle_name = ? or upper(ut.userid) = upper(?) " +
        "order by office, role"
        ;

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
                                roles.computeIfAbsent(roleOffice, (key) -> new ArrayList<>()).add(role);
                            }
                           return new User(userName, principalName, email, cac_role != null,  roles);
                        } else {
                            return (User)null;
                        }
                    }
                }
            })
        );
            
    }
}
