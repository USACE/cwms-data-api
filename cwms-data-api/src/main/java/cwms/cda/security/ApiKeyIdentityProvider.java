package cwms.cda.security;

import com.google.auto.service.AutoService;
import cwms.cda.spi.IdentityProvider;
import io.javalin.http.Context;
import io.swagger.v3.oas.models.security.SecurityScheme;
import io.swagger.v3.oas.models.security.SecurityScheme.In;
import io.swagger.v3.oas.models.security.SecurityScheme.Type;

import java.security.Principal;

import cwms.cda.ApiServlet;
import cwms.cda.data.dao.AuthDao;
import cwms.cda.data.dao.JooqDao;

@AutoService(IdentityProvider.class)
public class ApiKeyIdentityProvider implements IdentityProvider {

    public static final String AUTH_HEADER = "Authorization";
    private static AuthDao authDao;
    
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
    public boolean canAuth(Context ctx) {
        String header = ctx.header(AUTH_HEADER);
        if (header == null) {
            return false;
        }
        return header.trim().toLowerCase().startsWith("apikey");
    }
   
    private void init(Context ctx) {
        authDao = AuthDao.getInstance(JooqDao.getDslContext(ctx),
                ctx.attribute(ApiServlet.OFFICE_ID));
    }

    @Override
    public Principal authenticate(Context ctx) {
        init(ctx);
        String key = getApiKey(ctx);
        return authDao.getByApiKey(key);
    }
}
