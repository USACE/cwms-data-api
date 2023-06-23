package cwms.cda.data.dao;

import static org.jooq.impl.DSL.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.logging.Level;

import javax.sql.DataSource;

import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;

import com.google.common.flogger.FluentLogger;

import cwms.cda.ApiServlet;
import cwms.cda.datasource.ConnectionPreparer;
import cwms.cda.datasource.ConnectionPreparingDataSource;
import cwms.cda.datasource.DelegatingConnectionPreparer;
import cwms.cda.datasource.DirectUserPreparer;
import cwms.cda.datasource.SessionOfficePreparer;
import cwms.cda.helpers.ResourceHelper;
import cwms.cda.security.CwmsAuthException;
import cwms.cda.security.DataApiPrincipal;
import cwms.cda.security.Role;
import io.javalin.core.security.RouteRole;
import io.javalin.http.Context;

public class AuthDao extends Dao<DataApiPrincipal>{
    public static final FluentLogger logger = FluentLogger.forEnclosingClass();
    public static final String SCHEMA_TOO_OLD = "The CWMS-Data-API requires schema version "
                                             + "23.03.16 or later to handle authorization operations.";
    // At this level we just care that the user has permissions in *any* office
    private final String RETRIEVE_GROUPS_OF_USER;

    private static final String SET_API_USER_DIRECT = "begin "
        + "cwms_env.set_session_user_direct(upper(?));"
        + "end;";

    private static final String SET_API_USER_DIRECT_WITH_OFFICE = "begin "
        + "cwms_env.set_session_user_direct(upper(?),upper(?)); end;";

    private static final String CHECK_API_KEY =
        "select userid from cwms_20.at_api_keys where apikey = ?";



    private boolean hasCwmsEnvMultiOficeAuthFix;
    private String connectionUser;
    private String defaultOffice;

    public AuthDao(DSLContext dsl, String defaultOffice) {
        super(dsl);
        if(getDbVersion() < Dao.CWMS_23_03_16) {
            throw new RuntimeException(SCHEMA_TOO_OLD);
        }
        this.defaultOffice = defaultOffice;
        try {
            connectionUser = dsl.connectionResult(c->c.getMetaData().getUserName());
            dsl.execute("BEGIN cwms_env.set_session_user_direct(?,?)", connectionUser,defaultOffice);
            hasCwmsEnvMultiOficeAuthFix = true;
        } catch (DataAccessException ex) {
            if( ex.getLocalizedMessage()
                  .toLowerCase()
                  .contains("wrong number or types of arguments in call")) {
                hasCwmsEnvMultiOficeAuthFix = false;
            }
        }
        this.RETRIEVE_GROUPS_OF_USER = ResourceHelper.getResourceAsString("/cwms/data/sql/user_groups.sql",this.getClass());
    }

    @Override
    public List<DataApiPrincipal> getAll(Optional<String> limitToOffice) {
        throw new UnsupportedOperationException("Unimplemented method 'getAll'");
    }

    /**
     * Reserved for future use, get user principal by presented unique name and office.
     * (Also required by Dao<dataApiPrincipal>)
     */
    @Override
    public Optional<DataApiPrincipal> getByUniqueName(String uniqueName, Optional<String> limitToOffice) throws CwmsAuthException {
        throw new UnsupportedOperationException("Unimplemented method 'getByUniqueName'");
    }

    /**
     * Retrieve required user information from database for a given APIKEY.
     * @param apikey
     * @return valid DataApiPrincipal object for further authorization verification.
     * @throws CwmsAuthException throw for any issue with verification of Key or user information.
     */
    public DataApiPrincipal getByApiKey(String apikey) throws CwmsAuthException {
        String userName = checkKey(apikey);
        Set<RouteRole> roles = getRolesForUser(userName);
        return new DataApiPrincipal(userName,roles);
    }   

    /**
     * Setup session environment so we can query the required tables.
     * @param conn
     * @throws SQLException
     */
    private void setSessionForAuthCheck(Connection conn) throws SQLException {
        if (hasCwmsEnvMultiOficeAuthFix) {
            try(PreparedStatement setApiUser = conn.prepareStatement(SET_API_USER_DIRECT_WITH_OFFICE);) {
                setApiUser.setString(1,connectionUser);
                setApiUser.setString(2,defaultOffice);
                setApiUser.execute();
            }
        } else {
            try(PreparedStatement setApiUser = conn.prepareStatement(SET_API_USER_DIRECT);) {
                setApiUser.setString(1,connectionUser);
                setApiUser.execute();
            }
        }
    }

    private String checkKey(String key) throws CwmsAuthException {
        return dsl.connectionResult(c-> {
            setSessionForAuthCheck(c);
            try (PreparedStatement checkForKey = c.prepareStatement(CHECK_API_KEY);) {
                checkForKey.setString(1,key);
                try (ResultSet rs = checkForKey.executeQuery()) {
                    if (rs.next()) {
                        return rs.getString(1);
                    } else {
                        throw new CwmsAuthException("No user for key");
                    }
                }
            } catch (SQLException ex) {
                throw new CwmsAuthException("Failed API key check",ex);
            }
        });
    }

    /**
     * Retrieve roles a user has. 
     * @param user
     * @return
     */
    private Set<RouteRole> getRolesForUser(String user) {
        final Set<RouteRole> roles = new HashSet<>();
        dsl.connection(c->{
            setSessionForAuthCheck(c);
            try (PreparedStatement getRoles = c.prepareStatement(RETRIEVE_GROUPS_OF_USER);) {
                getRoles.setString(1,user);
                try (ResultSet rs = getRoles.executeQuery()) {
                    while (rs.next()) {
                        roles.add(new Role(rs.getString(1)));
                    }
                }
            } catch (SQLException ex) {
                logger.atWarning().withCause(ex).log("Failed to retrieve any roles for user.");
            }
        });
        return roles;
    }

    /**
     * Allows connection to be correctly setup, key is used to assert user, then the office
     * is set to the user specified office for further checks within the database.
     *
     * NOTE: side affect on ctx for this session.
     * 
     * @param ctx javalin context if additional parameters are required.
     * @param user username, which is ignored except a log message
     * @param key the API key that was presented for this connection
     */
    public static void prepareContextWithUser(Context ctx, DataApiPrincipal p) throws SQLException {
        Objects.requireNonNull(ctx, "A valid Javalin Context must be provided to this call.");
        Objects.requireNonNull(p, "A valid data api principal must be provided to this call.");
        logger.atInfo().log("Validated Api Key for user=%s", p.getName());
        DataSource dataSource = ctx.attribute(ApiServlet.DATA_SOURCE);
        ConnectionPreparer userPreparer = new DirectUserPreparer(p.getName());

        if (dataSource instanceof ConnectionPreparingDataSource) {
            ConnectionPreparingDataSource cpDs = (ConnectionPreparingDataSource)dataSource;
            ConnectionPreparer existingPreparer = cpDs.getPreparer();

            // Have it do our extra step last.
            cpDs.setPreparer(new DelegatingConnectionPreparer(existingPreparer, userPreparer));
        } else {
            ctx.attribute(ApiServlet.DATA_SOURCE,
                          new ConnectionPreparingDataSource(userPreparer, dataSource));
        }
    }

    /**
     * Primary logic to determine if a given principal can perform the desired operation.
     * Throws exception if not valid, otherwise just returns.
     * @param ctx
     * @param p
     * @param routeRoles
     */
    public static void isAuthorized(Context ctx, DataApiPrincipal p, Set<RouteRole> routeRoles) throws CwmsAuthException {
        if (routeRoles == null || routeRoles.isEmpty()) {
            logger.atFinest().log("Passthrough, no required roles defined.");
            return;
        } else if (p != null) {
            Set<RouteRole> specifiedRoles = p.getRoles();
            if (specifiedRoles.containsAll(routeRoles)) {
                logger.atFinest().log("User has required roles.");
                return;
            } else {
                logger.atFine().log();
                throw new CwmsAuthException(getFailMessage(ctx,routeRoles,p),403);
            }
        } else {
            throw new CwmsAuthException("No credentials provided.",401);
        }
    }

    /**
     * Set the Context and datasource to be suitable for processing guest requests.
     */
    public void prepareGuestContext(Context ctx) {
        DataSource dataSource = ctx.attribute(ApiServlet.DATA_SOURCE);
        ConnectionPreparer officePreparer = new SessionOfficePreparer(defaultOffice);
        ConnectionPreparer userPreparer = new DirectUserPreparer(connectionUser);
        ConnectionPreparer guestPreparer = new DelegatingConnectionPreparer(userPreparer,officePreparer);

        if (dataSource instanceof ConnectionPreparingDataSource) {
            ConnectionPreparingDataSource cpDs = (ConnectionPreparingDataSource)dataSource;
            ConnectionPreparer existingPreparer = cpDs.getPreparer();

            // Have it do our extra step last.
            cpDs.setPreparer(new DelegatingConnectionPreparer(existingPreparer, guestPreparer));
        } else {
            ctx.attribute(ApiServlet.DATA_SOURCE,
                          new ConnectionPreparingDataSource(guestPreparer, dataSource));
        }
    }

    private static String getFailMessage(@NotNull Context ctx,
                                        @NotNull Set<RouteRole> requiredRoles,
                                        @NotNull DataApiPrincipal p) {
        Set<RouteRole> specifiedRoles = p.getRoles();
        Set<RouteRole> missing = new LinkedHashSet<>(requiredRoles);
        missing.removeAll(specifiedRoles);

        return "Request for: " + ctx.req.getRequestURI() + " denied. Missing roles: " + missing;
    }

}
