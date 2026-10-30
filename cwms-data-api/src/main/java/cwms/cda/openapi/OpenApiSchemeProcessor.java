package cwms.cda.openapi;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.flogger.FluentLogger;

import cwms.cda.security.Authenticator;
import io.javalin.config.JavalinConfig;
import io.javalin.openapi.schema.OpenApiSchemaBuilder;
import io.swagger.v3.oas.models.security.SecurityRequirement;

// TODO: it works different now.
public class OpenApiSchemeProcessor {
    private static final FluentLogger logger = FluentLogger.forEnclosingClass();

    public static final String ANONYMOUS_ALLOWED = "anon";

    private final Authenticator authenticator;
    private final ArrayList<SecurityRequirement> secReqs = new ArrayList<>();

    public OpenApiSchemeProcessor(Authenticator authenticator)
    {
        this.authenticator = authenticator;
    }

    
    public OpenApiSchemaBuilder apply(JavalinConfig config, OpenApiSchemaBuilder api) {
        synchronized (secReqs) {
            secReqs.clear();
            authenticator.getActiveProviders().forEach(identityProvider -> {
                var scheme = identityProvider.getScheme();
                if (scheme == null) {
                    return;
                }
                api.withSecurityScheme(identityProvider.getName(), identityProvider.getScheme());
                SecurityRequirement req = new SecurityRequirement();
                if (!identityProvider.getName().equalsIgnoreCase("guestauth")
                        && !identityProvider.getName().equalsIgnoreCase("noauth")) {
                    req.addList(identityProvider.getName());
                    secReqs.add(req);
                    api.withGlobalSecurity(identityProvider.getName());
                }
            });
            api.withGlobalSecurity(ANONYMOUS_ALLOWED, c -> c.getScopes().clear());
        }
        return api;
    }

    public void establishCorrectAnonymous(JsonNode securityArrayNode, ObjectMapper objectMapper, boolean remove) {    
        if (securityArrayNode != null && securityArrayNode.isArray()) {
            var securityArray = (ArrayNode)securityArrayNode;
            int idx = -1;
            for (int i = 0; i < securityArray.size(); i++) {
                var sn = securityArray.get(i);
                if (sn.has(ANONYMOUS_ALLOWED))
                {
                    securityArray.set(i,  objectMapper.createObjectNode());
                    if (remove) {
                        idx = i;
                    }
                }
            }
            // we only clear this to an empty array *if* it's the only remaining option
            if (idx != -1 && securityArray.size() == 1) {
                securityArray.remove(idx);
            }
        }
    }


    public List<SecurityRequirement> getSecurityRequirements()
    {
        return Collections.unmodifiableList(secReqs);
    }
}
