package cwms.radar.security;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Base64;
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
import cwms.radar.spi.RadarAccessManager;
import io.javalin.core.security.RouteRole;
import io.javalin.http.Context;
import io.javalin.http.Handler;
import io.swagger.v3.oas.models.security.SecurityScheme;
import io.swagger.v3.oas.models.security.SecurityScheme.In;
import io.swagger.v3.oas.models.security.SecurityScheme.Type;
import usace.cwms.db.jooq.codegen.packages.CWMS_ENV_PACKAGE;
import usace.cwms.db.jooq.codegen.packages.CWMS_SEC_PACKAGE;

public class KeyAccessManager extends RadarAccessManager{
    private static final Logger logger = Logger.getLogger(KeyAccessManager.class.getName());
    private static final String AUTH_HEADER = "Authorization";

	@Override
	public void manage(Handler handler, Context ctx, Set<RouteRole> routeRoles) throws Exception {
        try
        {
            String user = authorized(ctx,routeRoles);
            if(user != null)
            {
                prepareContextWithUser(ctx, user);
                handler.handle(ctx);
            }
        }
        catch(Exception ex)
        {
            logger.log(Level.WARNING,"Unauthorized loggin attempt",ex);
        }		
        
		ctx.status(HttpServletResponse.SC_UNAUTHORIZED).json(RadarError.notAuthorized());
        ctx.status(401).result("Unauthorized");
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
     */
    private void prepareContextWithUser(Context ctx, String user) throws SQLException {
        logger.info("Validated Api Key for user=" + user);
        DataSource dataSource = ctx.attribute(ApiServlet.DATA_SOURCE);

        ConnectionPreparer newPreparer = new ApiKeyUserPreparer(user);
        if(dataSource instanceof ConnectionPreparingDataSource) {
            ConnectionPreparingDataSource cpDs = (ConnectionPreparingDataSource)    dataSource;
            ConnectionPreparer existingPreparer = cpDs.getPreparer();

            // Have it do our extra step last.
            cpDs.setPreparer(new DelegatingConnectionPreparer(existingPreparer, newPreparer));
        } else {            
            ctx.attribute(ApiServlet.DATA_SOURCE, new ConnectionPreparingDataSource(newPreparer, dataSource));
        }
    }

    private String authorized(Context ctx, Set<RouteRole> routeRoles)
    {
        if (routeRoles.isEmpty())
        {
            return "guest";
        }
        else
        {
            String key = getAppKey(ctx);
            return checkKey(key,ctx);
        }
    }

    private String checkKey(String key, Context ctx) {
        DataSource dataSource = ctx.attribute(ApiServlet.DATA_SOURCE);
        try(Connection conn = dataSource.getConnection();
            PreparedStatement stmt = conn.prepareStatement("select username from cwms_20.apikeys where key = ?")) 
        {
            stmt.setString(1,key);
            try(ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getString(1);
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

    private String getAppKey(Context ctx) {
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
