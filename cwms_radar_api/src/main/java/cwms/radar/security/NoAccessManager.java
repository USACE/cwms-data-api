package cwms.radar.security;

import java.util.Set;

import cwms.radar.spi.RadarAccessManager;
import io.javalin.core.security.RouteRole;
import io.javalin.http.Context;
import io.javalin.http.Handler;
import io.swagger.v3.oas.models.security.SecurityScheme;
import io.swagger.v3.oas.models.security.SecurityScheme.Type;

/**
 * Always fail manager for CRUD operations on read only systems.
 * Intentionally does not have a provider.
 */
public class NoAccessManager extends RadarAccessManager{

    /**
     * Just fail.
     */
    @Override
    public void manage(Handler handler, Context ctx, Set<RouteRole> routeRoles) throws Exception {        
        throw new CwmsAuthException("Write Operation Not Authorized on this system.");
    }

    @Override
    public boolean canAuth(Context ctx, Set<RouteRole> roles) {       
        return true;
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
