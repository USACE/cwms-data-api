package cwms.radar.security;

import java.util.Set;

import cwms.radar.spi.RadarAccessManager;
import io.javalin.core.security.RouteRole;
import io.javalin.http.Context;
import io.javalin.http.Handler;
import io.swagger.v3.oas.models.security.SecurityScheme;
import io.swagger.v3.oas.models.security.SecurityScheme.Type;

/**
 * Pass through Manager only used for unauthenticated 
 * end points.
 * Intentionally does not have a provider
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
        return "GuestAuth";
    }

    @Override
    public SecurityScheme getScheme() {
        return null;
    }
    
}
