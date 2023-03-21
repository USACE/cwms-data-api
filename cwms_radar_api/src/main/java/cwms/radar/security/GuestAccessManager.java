package cwms.radar.security;

import java.util.Set;

import cwms.radar.spi.RadarAccessManager;
import io.javalin.core.security.RouteRole;
import io.javalin.http.Context;
import io.javalin.http.Handler;
import io.swagger.v3.oas.models.security.SecurityScheme;

/**
 * Pass through Manager only used for unauthenticated 
 * end points.
 */
public class GuestAccessManager extends RadarAccessManager{

    @Override
    public void manage(Handler handler, Context ctx, Set<RouteRole> routeRoles) throws Exception {
        handler.handle(ctx);
    }

    @Override
    public boolean canAuth(Context ctx, Set<RouteRole> roles) {       
        return roles.isEmpty();
    }

    @Override
    public String getName() {
        return "NoAuth";
    }

    @Override
    public SecurityScheme getScheme() {
        return null;
    }
    
}
