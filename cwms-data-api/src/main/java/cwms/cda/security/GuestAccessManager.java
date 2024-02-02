package cwms.cda.security;

import java.util.Set;

import cwms.cda.ApiServlet;
import cwms.cda.data.dao.AuthDao;
import cwms.cda.data.dao.JooqDao;

import cwms.cda.spi.CdaAccessManager;
import io.javalin.core.security.RouteRole;
import io.javalin.http.Context;
import io.javalin.http.Handler;
import io.swagger.v3.oas.models.security.SecurityScheme;

/**
 * Pass through Manager only used for unauthenticated 
 * end points.
 * Intentionally does not have a provider
 */
public class GuestAccessManager extends CdaAccessManager{

    private AuthDao authDao = null;

    @Override
    public void manage(Handler handler, Context ctx, Set<RouteRole> routeRoles) throws Exception {
        init(ctx);
        if (authDao != null){
            authDao.prepareGuestContext(ctx);
        }
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
    
    private void init(Context ctx) {
        authDao = AuthDao.getInstance(JooqDao.getDslContext(ctx),ctx.attribute(ApiServlet.OFFICE_ID));
    }
}
