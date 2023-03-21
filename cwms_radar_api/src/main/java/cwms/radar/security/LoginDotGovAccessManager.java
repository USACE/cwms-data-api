package cwms.radar.security;

import java.util.Set;

import cwms.radar.spi.RadarAccessManager;
import io.javalin.core.security.RouteRole;
import io.javalin.http.Context;
import io.javalin.http.Handler;
import io.swagger.v3.oas.models.security.SecurityScheme;
import io.swagger.v3.oas.models.security.SecurityScheme.Type;

/**
 * This is currently more a placeholder for example than actual implementation
 */
public class LoginDotGovAccessManager extends RadarAccessManager {

    private String sessionKey;

    public LoginDotGovAccessManager() {
        
    }

    @Override
    public void manage(Handler handler, Context ctx, Set<RouteRole> routeRoles) throws Exception {


    }

    @Override
    public SecurityScheme getScheme() {
        return new SecurityScheme().type(Type.OPENIDCONNECT);
    }

    @Override
    public String getName() {
        return "LoginDotGov";
    }

    @Override
    public boolean canAuth(Context ctx, Set<RouteRole> roles) {
        return false; // still implementing details;
    }

}
