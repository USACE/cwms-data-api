package cwms.cda.data.dao;

import static org.jooq.impl.DSL.*;

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import javax.sql.DataSource;

import org.jetbrains.annotations.NotNull;
import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;

import com.google.common.flogger.FluentLogger;

import cwms.cda.ApiServlet;
import cwms.cda.data.dto.auth.ApiKey;
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
import io.javalin.http.HttpCode;

public class AuthDao extends Dao<DataApiPrincipal>{
    public static final FluentLogger logger = FluentLogger.forEnclosingClass();
    public static final String SCHEMA_TOO_OLD = "The CWMS-Data-API requires schema version "
                                             + "23.03.16 or later to handle authorization operations.";
    public static final String DATA_API_PRINCIPAL = "DataApiPrincipal";
    // At this level we just care that the user has permissions in *any* office
    private static String RETRIEVE_GROUPS_OF_USER = ResourceHelper.getResourceAsString("/cwms/data/sql/user_groups.sql",AuthDao.class);

    private static final String SET_API_USER_DIRECT = "begin "
        + "cwms_env.set_session_user_direct(upper(?));"
        + "end;";

    private static final String SET_API_USER_DIRECT_WITH_OFFICE = "begin "
        + "cwms_env.set_session_user_direct(upper(?),upper(?)); end;";

    private static final String CHECK_API_KEY =
        "select userid from cwms_20.at_api_keys where apikey = ?";

    public static final String CREATE_API_KEY = "insert into cwms_20.at_api_keys(userid,key_name,apikey,created,expires) values(UPPER(?),?,?,?,?)";
    public static final String REMOVE_API_KEY = "delete from cwms_20.at_api_keys where UPPER(userid) = UPPER(?) and key_name = ?";
    public static final String LIST_KEYS = "select userid,key_name,created,expires from cwms_20.at_api_keys where UPPER(userid) = UPPER(?) order by created desc";
    public static final String GET_SINGLE_KEY = "select userid,key_name,created,expires from cwms_20.at_api_keys where UPPER(userid) = UPPER(?) and key_name = ?";
    public static final String ONLY_OWN_KEY_MESSAGE = "You may not create API keys for any user other than your own.";

    private static boolean hasCwmsEnvMultiOfficeAuthFix = false;
    private static String connectionUser = null;
    private static String defaultOffice = null;

    /**
     * Since every request uses this instead of constantly creating new objects we keep
     * one per servicing thread and just reset the context so that setClientInfo still
     * makes sense.
     */
    private static ThreadLocal<AuthDao> instance = new ThreadLocal<>();

    private AuthDao(DSLContext dsl, String defaultOffice) {
        super(dsl);
        if (getDbVersion() < Dao.CWMS_23_03_16) {
            throw new RuntimeException(SCHEMA_TOO_OLD);
        }

        if (AuthDao.defaultOffice == null) {
            AuthDao.defaultOffice = defaultOffice;
            try {
                connectionUser = dsl.connectionResult(c->c.getMetaData().getUserName());
                dsl.execute("BEGIN cwms_env.set_session_user_direct(?,?)", connectionUser,defaultOffice);
                hasCwmsEnvMultiOfficeAuthFix = true;
            } catch (DataAccessException ex) {
                if( ex.getLocalizedMessage()
                    .toLowerCase()
                    .contains("wrong number or types of arguments in call")) {
                    hasCwmsEnvMultiOfficeAuthFix = false;
                }
            }
        }
    }

    /**
     * Get Appropriate instance of the AuthDAO. Setup with the given DSLContext.
     * The instance of AuthDAO returned is local to a given servicing thread.
     *
     * @param dsl
     * @param defaultOffice can be null
     * @return
     */
    public static AuthDao getInstance(DSLContext dsl, String defaultOffice) {
        AuthDao dao = instance.get();
        if (dao == null)
        {
            dao = new AuthDao(dsl,defaultOffice);
            instance.set(dao);
        } else {
            dao.resetContext(dsl);
        }
        return dao;
    }

    /**
     * Used in sections of code that will always be called after the default office is set
     * but that still need to interact with this DAO.
     * @param dsl
     * @return
     */
    public static AuthDao getInstance(DSLContext dsl) {
        return getInstance(dsl, null);
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
        if (hasCwmsEnvMultiOfficeAuthFix) {
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
        try {
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
        } catch (DataAccessException ex) {
            Throwable t = ex.getCause();
            if (t instanceof CwmsAuthException) {
                throw (CwmsAuthException)t;
            } else {
                throw ex;
            }
        }
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
        logger.atInfo()
              .atMostEvery(5,TimeUnit.SECONDS)
              .log("Validated Api Key for user=%s", p.getName());
        DataSource dataSource = ctx.attribute(ApiServlet.DATA_SOURCE);
        ConnectionPreparer userPreparer = new DirectUserPreparer(p.getName());
        ctx.attribute(DATA_API_PRINCIPAL,p);
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


    /**
     * Return create an ApiKey for future authentication and authorization.
     * @param p Principal object, to get the username
     * @param keyName Friendly name for this key
     * @param expires when this key expires; can be null to never expire
     * @return The created ApiKey
     */
    public ApiKey createApiKey(DataApiPrincipal p, ApiKey sourceData) throws CwmsAuthException {

        try {
            if (!p.getName().equalsIgnoreCase(sourceData.getUserId())) {
                throw new CwmsAuthException(ONLY_OWN_KEY_MESSAGE, HttpCode.UNAUTHORIZED.getStatus());
            }
            SecureRandom randomSource = SecureRandom.getInstanceStrong();
            String key = randomSource.ints((char)'0',(char)'z') // allow a-zA-Z0-9
                                 .filter(i -> (i <= 57 || i >= 65) && (i <= 90 || i >= 97)) // actually filter to above
                                 .limit(256)
                                 .collect(StringBuilder::new,StringBuilder::appendCodePoint, StringBuilder::append)
                                 .toString();
            final ApiKey newKey = new ApiKey(sourceData.getUserId().toUpperCase(),sourceData.getKeyName(),key,ZonedDateTime.now(ZoneId.of("UTC")),sourceData.getExpires());
            dsl.connection(c -> {
                setSessionForAuthCheck(c);
                try (PreparedStatement createKey = c.prepareStatement(CREATE_API_KEY);) {
                    createKey.setString(1,newKey.getUserId());
                    createKey.setString(2,newKey.getKeyName());
                    createKey.setString(3,newKey.getApiKey());
                    createKey.setDate(4,new Date(newKey.getCreated().toInstant().toEpochMilli()),Calendar.getInstance(TimeZone.getTimeZone("UTC")));
                    if (newKey.getExpires() != null) {
                        createKey.setDate(5,new Date(newKey.getExpires().toInstant().toEpochMilli()),Calendar.getInstance(TimeZone.getTimeZone("UTC")));
                    } else {
                        createKey.setDate(5,null);
                    }
                    createKey.execute();
                }
            });
            return newKey;
        } catch (NoSuchAlgorithmException ex) {
            throw new CwmsAuthException("Unable to generate appropriate key.", ex, HttpCode.INTERNAL_SERVER_ERROR.getStatus());
        }


    }

    /**
     * Return all API Keys for a given user
     * @param p User for which we want the keys
     * @return List of all the keys, with the actual key removed (only user,name,created, and expires)
     */
    public List<ApiKey> apiKeysForUser(DataApiPrincipal p) {
        List<ApiKey> keys = new ArrayList<ApiKey>();
        dsl.connection(c -> {
            setSessionForAuthCheck(c);
            try (PreparedStatement listKeys = c.prepareStatement(LIST_KEYS);) {
                listKeys.setString(1,p.getName());
                try (ResultSet rs = listKeys.executeQuery()) {
                    while(rs.next()) {
                        keys.add(rs2ApiKey(rs));
                    }
                }
            }
        });
        return keys;
    }

    public ApiKey apiKeyForUser(DataApiPrincipal p, String keyName) {
        return dsl.connectionResult(c -> {
            setSessionForAuthCheck(c);
            try (PreparedStatement singleKey = c.prepareStatement(GET_SINGLE_KEY);) {
                singleKey.setString(1,p.getName());
                singleKey.setString(2,keyName);
                try (ResultSet rs = singleKey.executeQuery()) {
                    if(rs.next()) {
                        return rs2ApiKey(rs);
                    } else {
                        return null;
                    }
                }
            }
        });
    }

    private static ApiKey rs2ApiKey(ResultSet rs) throws SQLException {
        String userId = rs.getString("userid");
        String keyName = rs.getString("key_name");
        ZonedDateTime created = rs.getObject("created",ZonedDateTime.class);
        ZonedDateTime expires = rs.getObject("expires",ZonedDateTime.class);
        return new ApiKey(userId,keyName,null,created,expires);
    }

    /**
     * Remove a given API Key
     * @param p User principal to narrow and limit request
     * @param keyName name of the key to remove
     */
    public void deleteKeyForUser(DataApiPrincipal p, String keyName)
    {
        dsl.connection( c -> {
            setSessionForAuthCheck(c);
            try (PreparedStatement deleteKey = c.prepareStatement(REMOVE_API_KEY);) {
                deleteKey.setString(1, p.getName());
                deleteKey.setString(2, keyName);
                deleteKey.execute();
            }
        });
    }


    public DataApiPrincipal getDataApiPrincipal(Context ctx)
    {
        return ctx.attribute(DATA_API_PRINCIPAL);
    }

    /**
     * Used to avoid constant instancing of the AuthDao objects
     * @param dslContext
     */
    public void resetContext(DSLContext dslContext) {
        this.dsl = dslContext;
    }
}
