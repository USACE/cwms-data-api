package cwms.cda.security;

import cwms.cda.spi.CdaAccessManager;
import cwms.cda.ApiServlet;
import cwms.cda.api.errors.CdaError;
import cwms.cda.data.dao.AuthDao;
import cwms.cda.data.dao.JooqDao;
import io.javalin.core.security.RouteRole;
import io.javalin.http.Context;
import io.javalin.http.Handler;

import io.swagger.v3.oas.models.security.SecurityScheme;
import io.swagger.v3.oas.models.security.SecurityScheme.In;
import io.swagger.v3.oas.models.security.SecurityScheme.Type;

import java.util.HashMap;
import java.util.Optional;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

public class KeyAccessManager extends CdaAccessManager {
    private static final Logger logger = Logger.getLogger(KeyAccessManager.class.getName());
    private static final String AUTH_HEADER = "Authorization";

    private AuthDao authDao;


    @Override
    public void manage(Handler handler, Context ctx, Set<RouteRole> routeRoles) throws Exception {
        init(ctx);        
        String key = getApiKey(ctx);
        DataApiPrincipal p = authDao.getByApiKey(key);
        AuthDao.isAuthorized(ctx, p, routeRoles);
        AuthDao.prepareContextWithUser(ctx, p);
        handler.handle(ctx);
    }

    private void init(Context ctx) {
        if (authDao == null) {
            authDao = new AuthDao(JooqDao.getDslContext(ctx),ctx.attribute(ApiServlet.OFFICE_ID));
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
