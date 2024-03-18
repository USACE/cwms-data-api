package cwms.cda.security;

import cwms.cda.ApiServlet;
import cwms.cda.data.dao.AuthDao;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.spi.CdaAccessManager;
import io.javalin.core.security.RouteRole;
import io.javalin.http.Context;
import io.javalin.http.Handler;
import io.swagger.v3.oas.models.security.SecurityScheme;
import io.swagger.v3.oas.models.security.SecurityScheme.In;
import io.swagger.v3.oas.models.security.SecurityScheme.Type;
import java.util.Set;
import org.jetbrains.annotations.NotNull;

public class KeyAccessManager extends CdaAccessManager {

    public static final String AUTH_HEADER = "Authorization";

    private static AuthDao authDao;


    @Override
    public void manage(Handler handler, @NotNull Context ctx, @NotNull Set<RouteRole> routeRoles) throws Exception {
        init(ctx);
        String key = getApiKey(ctx);
        DataApiPrincipal p = authDao.getByApiKey(key);
        AuthDao.isAuthorized(ctx, p, routeRoles);
        AuthDao.prepareContextWithUser(ctx, p);
        handler.handle(ctx);
    }

    private void init(Context ctx) {
        authDao = AuthDao.getInstance(JooqDao.getDslContext(ctx),
                ctx.attribute(ApiServlet.OFFICE_ID));
    }

    private String getApiKey(Context ctx) {
        String header = ctx.header(AUTH_HEADER);
        if (header == null) {
            return null;
        }

        String[] parts = header.split("\\s+");
        if (parts.length < 2) {
            return null;
        } else {
            return parts[1];
        }
    }

    @Override
    public SecurityScheme getScheme() {
        return new SecurityScheme()
                    .scheme("apikey")
                    .type(Type.APIKEY)
                    .in(In.HEADER)
                    .description("Key value as generated from the /auth/keys endpoint. "
                            + "NOTE: you MUST manually prefix your key with 'apikey ' "
                            + "(without the single quotes).")
                    .name(AUTH_HEADER);
    }

    @Override
    public String getName() {
        return "ApiKey";
    }

    @Override
    public boolean canAuth(Context ctx, Set<RouteRole> roles) {
        String header = ctx.header(AUTH_HEADER);
        if (header == null) {
            return false;
        }
        return header.trim().toLowerCase().startsWith("apikey");
    }
}
