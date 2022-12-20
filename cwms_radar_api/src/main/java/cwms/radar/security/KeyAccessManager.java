package cwms.radar.security;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.http.HttpServletResponse;
import javax.sql.DataSource;

import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;

import cwms.radar.ApiServlet;
import cwms.radar.api.errors.RadarError;
import cwms.radar.datasource.ApiKeyUserPreparer;
import cwms.radar.datasource.ConnectionPreparer;
import cwms.radar.datasource.ConnectionPreparingDataSource;
import cwms.radar.datasource.DelegatingConnectionPreparer;
import cwms.radar.datasource.DirectUserPreparer;
import cwms.radar.datasource.SessionOfficePreparer;
import cwms.radar.spi.RadarAccessManager;
import io.javalin.core.security.RouteRole;
import io.javalin.http.Context;
import io.javalin.http.Handler;
import io.swagger.v3.oas.models.security.SecurityScheme;
import io.swagger.v3.oas.models.security.SecurityScheme.In;
import io.swagger.v3.oas.models.security.SecurityScheme.Type;

public class KeyAccessManager extends RadarAccessManager{
    private static final Logger logger = Logger.getLogger(KeyAccessManager.class.getName());
    private static final String AUTH_HEADER = "Authorization";

    private static final String RETRIEVE_GROUPS_OF_USER = "select "
    + "privs.user_group_id"
    + " from table("
    + "cwms_sec.get_user_priv_groups_tab(?,?)"
    + ") privs"
    + " where is_member = 'T'";


    private DataSource dataSource = null;

	@Override
	public void manage(Handler handler, Context ctx, Set<RouteRole> routeRoles) throws Exception {
        dataSource = ctx.attribute(ApiServlet.DATA_SOURCE);
        try
        {
            if (!routeRoles.isEmpty()) {
                String key = getApiKey(ctx);
                String user = authorized(ctx, key, routeRoles);
                if (user == null) {
                    throw new CwmsAuthException("Invalid Credentials.");
                }
                prepareContextWithUser(ctx, user,key);
            }
            handler.handle(ctx);
        }
        catch(CwmsAuthException ex) {
            logger.log(Level.WARNING,"Unauthorized login attempt",ex);
	    	ctx.status(HttpServletResponse.SC_UNAUTHORIZED).json(RadarError.notAuthorized());
            HashMap<String,String> msg = new HashMap<>();
            msg.put("message",ex.getMessage());
            RadarError re = new RadarError("Unauthorized",msg,true);
            ctx.status(401).json(re);
        }		

	}

    /**
     * This isn't currently used from Tomcat so we don't have any issues with the 
     * session; just use the connection itself.
     * 
     * NOTE: we will need to expand a bit in the future, but I think another JAPSIC or valve
     * handler that sets up the principal for the CWMS Access Manager will likely
     * have easier workflow.
     * @param ctx
     * @param user
     * @param key the key
     */
    private void prepareContextWithUser(Context ctx, String user,String key) throws SQLException {
        logger.info("Validated Api Key for user=" + user);

        ConnectionPreparer keyPreparer = new ApiKeyUserPreparer(key);
        ConnectionPreparer officePrepare = new SessionOfficePreparer(ctx.queryParam("office"));
        //ConnectionPreparer resetPreparer = new DirectUserPreparer("q0webtest");
        DelegatingConnectionPreparer apiPreparer = new DelegatingConnectionPreparer(officePrepare,keyPreparer);

        if (dataSource instanceof ConnectionPreparingDataSource) {
            ConnectionPreparingDataSource cpDs = (ConnectionPreparingDataSource)    dataSource;
            ConnectionPreparer existingPreparer = cpDs.getPreparer();

            // Have it do our extra step last.
            cpDs.setPreparer(new DelegatingConnectionPreparer(existingPreparer, apiPreparer));
        } else {            
            ctx.attribute(ApiServlet.DATA_SOURCE, new ConnectionPreparingDataSource(apiPreparer, dataSource));
        }
    }

    private String authorized(Context ctx, String key, Set<RouteRole> routeRoles)
    {
        String retval = null;
        String user = checkKey(key,ctx);

        if (routeRoles == null || routeRoles.isEmpty()) {
            retval = user;
        } else {
            Set<RouteRole> specifiedRoles = getRoles(user,ctx.queryParam("office"));
            retval = specifiedRoles.containsAll(routeRoles) == true ? user : null;
        }

        return retval;
    }

    private Set<RouteRole> getRoles(String user,String office) {
        Set<RouteRole> roles = new HashSet<>();
        try(Connection conn = dataSource.getConnection();
            PreparedStatement getRoles = conn.prepareStatement(RETRIEVE_GROUPS_OF_USER);
        ) {
            getRoles.setString(1,user);
            getRoles.setString(2,office);
            try (ResultSet rs = getRoles.executeQuery()) {
                while(rs.next()) {
                    roles.add(new Role(rs.getString(1)));
                }
            }
        } catch(SQLException ex) {
            logger.log(Level.WARNING,"Failed to retrieve roles for user",ex);
        }
        return roles;
    }

    private String checkKey(String key, Context ctx) {
        try(Connection conn = dataSource.getConnection();
            PreparedStatement setApiUser = conn.prepareStatement("begin cwms_env.set_session_user_direct(upper('q0hectest_pu')); end;");
            PreparedStatement checkForKey = conn.prepareStatement("select userid from cwms_20.at_api_keys where apikey = ?")) 
        {
            setApiUser.execute();
            checkForKey.setString(1,key);
            try(ResultSet rs = checkForKey.executeQuery()) {
                if (rs.next()) {
                    return rs.getString(1);
                } else {
                    logger.info("No user for key");
                }
            }
        }
        catch(SQLException ex) {
            logger.log(Level.WARNING,"Failed API key check",ex);
        }
        return null;
    }

    public static String getDigest(byte[] bytes) {
        try {
            MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
            sha256.reset();
            sha256.update(bytes);
            byte[] digest = sha256.digest();
            return Base64.getEncoder().encodeToString(digest);
        } catch( NoSuchAlgorithmException ex) {
            throw new RuntimeException("Unable to process authentication informaiton.",ex);
        }
    }

    private String getApiKey(Context ctx) {
        String header = ctx.header(AUTH_HEADER);
        String parts[] = header.split("\\s+");
        if( parts.length < 0) {
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
    
}
