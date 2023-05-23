package cwms.radar.security;

import cwms.radar.ApiServlet;
import cwms.radar.api.Controllers;
import cwms.radar.api.errors.RadarError;
import cwms.radar.datasource.ApiKeyUserPreparer;
import cwms.radar.datasource.ConnectionPreparer;
import cwms.radar.datasource.ConnectionPreparingDataSource;
import cwms.radar.datasource.DelegatingConnectionPreparer;
import cwms.radar.datasource.SessionOfficePreparer;
import cwms.radar.spi.RadarAccessManager;

import io.javalin.core.security.RouteRole;
import io.javalin.http.Context;
import io.javalin.http.Handler;

import io.swagger.v3.oas.models.security.SecurityScheme;
import io.swagger.v3.oas.models.security.SecurityScheme.In;
import io.swagger.v3.oas.models.security.SecurityScheme.Type;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.sql.DataSource;


public class KeyAccessManager extends RadarAccessManager {
    private static final Logger logger = Logger.getLogger(KeyAccessManager.class.getName());
    private static final String AUTH_HEADER = "Authorization";

    private static final String RETRIEVE_GROUPS_OF_USER = "select "
        + "privs.user_group_id"
        + " from table("
        + "cwms_sec.get_assigned_priv_groups_tab(?)"
        + ") privs"
        + " where is_member = 'T'";

    private static final String SET_API_USER_DIRECT = "begin " 
        + "cwms_env.set_session_user_direct(upper(?)); cwms_env.set_session_office_id(?)"
        + ";end;";

    private static final String SET_API_USER_DIRECT_WITH_OFFICE = "begin "
        + "cwms_env.set_session_office_id(upper(?));"
        + "cwms_env.set_session_user_direct(upper(?)); end;";

    private static final String CHECK_API_KEY =
        "select userid from cwms_20.at_api_keys where apikey = ?";

    private DataSource dataSource = null;

    @Override
    public void manage(Handler handler, Context ctx, Set<RouteRole> routeRoles) throws Exception {
        dataSource = ctx.attribute(ApiServlet.DATA_SOURCE);
        try {
            String key = getApiKey(ctx);
            String user = authorized(ctx, key, routeRoles);
            if (user==null) {
                throw new CwmsAuthException("Invalid credentials provided.");
            }
            prepareContextWithUser(ctx, user,key);
            handler.handle(ctx);
        } catch (CwmsAuthException ex) {
            logger.log(Level.WARNING,"Unauthorized access attempt",ex);
            HashMap<String,String> msg = new HashMap<>();
            msg.put("message",ex.getMessage());
            RadarError re = new RadarError("Unauthorized",msg,true);
            ctx.status(ex.getAuthFailCode()).json(re);
        }
    }

    /**
     * Allows connection to be correctly setup, key is used to assert user, then the office 
     * is set to the user specified office for further checks within the database.
     * 
     * @param ctx javalin context if additional parameters are required.
     * @param user username, which is ignored except a log message
     * @param key the API key that was presented for this connection
     */
    private void prepareContextWithUser(Context ctx, String user,String key) throws SQLException {
        logger.info("Validated Api Key for user=" + user);

        ConnectionPreparer keyPreparer = new ApiKeyUserPreparer(key);
        ConnectionPreparer officePrepare = new SessionOfficePreparer(ctx.queryParam(Controllers.OFFICE));
        DelegatingConnectionPreparer apiPreparer = 
            new DelegatingConnectionPreparer(keyPreparer,officePrepare);

        if (dataSource instanceof ConnectionPreparingDataSource) {
            ConnectionPreparingDataSource cpDs = (ConnectionPreparingDataSource)dataSource;
            ConnectionPreparer existingPreparer = cpDs.getPreparer();

            // Have it do our extra step last.
            cpDs.setPreparer(new DelegatingConnectionPreparer(existingPreparer, apiPreparer));
        } else {
            ctx.attribute(ApiServlet.DATA_SOURCE, 
                          new ConnectionPreparingDataSource(apiPreparer, dataSource));
        }
    }

    private String authorized(Context ctx, String key, Set<RouteRole> routeRoles) {
        String retval = null;
        String office = ctx.queryParam("office");
        String user = checkKey(key,ctx,office);

        if (routeRoles == null || routeRoles.isEmpty()) {
            retval = user;
        } else {
            Set<RouteRole> specifiedRoles = getRoles(user,office);
            if (specifiedRoles.containsAll(routeRoles)) {
                retval = user;
            } else {
                throw new CwmsAuthException("Operation not authorized for user",403);
            }
        }

        return retval;
    }

    private Set<RouteRole> getRoles(String user,String office) {
        Set<RouteRole> roles = new HashSet<>();
        try (Connection conn = dataSource.getConnection();
            PreparedStatement setApiUser = conn.prepareStatement(SET_API_USER_DIRECT);
            PreparedStatement getRoles = conn.prepareStatement(RETRIEVE_GROUPS_OF_USER);
        ) {
            setApiUser.setString(1,user);
            setApiUser.setString(2,office);
            setApiUser.execute();
            getRoles.setString(1,office);
            try (ResultSet rs = getRoles.executeQuery()) {
                while (rs.next()) {
                    roles.add(new Role(rs.getString(1)));
                }
            }
        } catch (SQLException ex) {
            logger.log(Level.WARNING,"Failed to retrieve roles for user",ex);
        }
        return roles;
    }

    private String checkKey(String key, Context ctx, String office) throws CwmsAuthException {
        try (Connection conn = dataSource.getConnection();
            PreparedStatement setApiUser = conn.prepareStatement(SET_API_USER_DIRECT_WITH_OFFICE);
            PreparedStatement checkForKey = conn.prepareStatement(CHECK_API_KEY);) {
            setApiUser.setString(1,office);
            setApiUser.setString(2,conn.getMetaData().getUserName());
            setApiUser.execute();
            checkForKey.setString(1,key);
            try (ResultSet rs = checkForKey.executeQuery()) {
                if (rs.next()) {
                    return rs.getString(1);
                } else {
                    logger.info("No user for key");
                    throw new CwmsAuthException("Access not authorized.");
                }
            }
        } catch (SQLException ex) {
            logger.log(Level.WARNING,"Failed API key check",ex);
            throw new CwmsAuthException("Authorized failed");
        }
    }

    private String getApiKey(Context ctx) {
        String header = ctx.header(AUTH_HEADER);
        if (header == null) {
            return null;
        }

        String []parts = header.split("\\s+");
        if (parts.length < 0) {
            return null;
        } else {
            return parts[1];
        }
    }

    @Override
    public SecurityScheme getScheme() {
        return new SecurityScheme()
                    .type(Type.APIKEY)
                    .in(In.HEADER)
                    .name("Authorization");
    }

    @Override
    public String getName() {
        return "ApiKey";    
    }

    @Override
    public boolean canAuth(Context ctx, Set<RouteRole> roles) {
        String header = ctx.header("Authorization");
        if (header == null) {
            return false;
        }
        return header.trim().startsWith("apikey");
    }

    

}
