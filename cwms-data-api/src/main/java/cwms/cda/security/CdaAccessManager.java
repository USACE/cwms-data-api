package cwms.cda.security;

import java.util.Set;

import cwms.cda.ApiServlet;
import cwms.cda.data.dao.AuthDao;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.spi.IdentityProvider;
import io.javalin.core.security.AccessManager;
import io.javalin.core.security.RouteRole;
import io.javalin.http.Context;
import io.javalin.http.Handler;

public final class CdaAccessManager implements AccessManager {

    @Override
    public void manage(Handler handler, Context ctx, Set<RouteRole> routeRoles) throws Exception {
        DataApiPrincipal p = getApiPrincipal(ctx);
        AuthDao.isAuthorized(ctx,p,routeRoles);
        prepareContext(ctx, p);
        handler.handle(ctx);
    }

    private DataApiPrincipal getApiPrincipal(Context ctx) {
        return ctx.sessionAttribute(IdentityProvider.PRINCIPAL_KEY);
    }

    private void prepareContext(Context ctx, DataApiPrincipal p) {
        if (p == null) {
            AuthDao authDao = AuthDao.getInstance(JooqDao.getDslContext(ctx),ctx.attribute(ApiServlet.OFFICE_ID));
            authDao.prepareGuestContext(ctx);
        } else {
            AuthDao.prepareContextWithUser(ctx, p);
        }
    }
}
