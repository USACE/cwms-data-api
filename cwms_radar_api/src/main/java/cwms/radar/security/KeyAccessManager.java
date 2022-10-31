package cwms.radar.security;

import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.http.HttpServletResponse;

import cwms.radar.api.errors.RadarError;
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

	@Override
	public void manage(Handler handler, Context ctx, Set<RouteRole> routeRoles) throws Exception {
        try
        {
            if(authorized(ctx,routeRoles))
            {
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

    private boolean authorized(Context ctx, Set<RouteRole> routeRoles)
    {
        if (routeRoles.isEmpty())
        {
            return true;
        }
        else
        {
            String key = getAppKey(ctx);
        }
        return false;
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
