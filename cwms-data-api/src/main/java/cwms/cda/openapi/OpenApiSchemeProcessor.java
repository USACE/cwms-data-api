package cwms.cda.openapi;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import cwms.cda.security.Authenticator;
import io.javalin.http.Context;
import io.javalin.plugin.openapi.OpenApiModelModifier;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.security.SecurityRequirement;

public class OpenApiSchemeProcessor implements OpenApiModelModifier {

    private final Authenticator authenticator;
    private final ArrayList<SecurityRequirement> secReqs = new ArrayList<>();

    public OpenApiSchemeProcessor(Authenticator authenticator)
    {
        this.authenticator = authenticator;
    }

    @Override
    public OpenAPI apply(Context ctx, OpenAPI api) {
        var schemes = api.getComponents().getSecuritySchemes();
        if (schemes != null)
        {
            schemes.clear();
        }
        synchronized (secReqs) {
            secReqs.clear();
            authenticator.getActiveProviders().forEach(identityProvider -> {
                api.getComponents().addSecuritySchemes(identityProvider.getName(),identityProvider.getScheme());
                SecurityRequirement req = new SecurityRequirement();
                if (!identityProvider.getName().equalsIgnoreCase("guestauth")
                        && !identityProvider.getName().equalsIgnoreCase("noauth")) {
                    req.addList(identityProvider.getName());
                    secReqs.add(req);
                }
            });
        }
        return api;
    }


    public List<SecurityRequirement> getSecurityRequirements()
    {
        return Collections.unmodifiableList(secReqs);
    }
}
