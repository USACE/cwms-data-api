package cwms.cda.security;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.flogger.FluentLogger;

import io.swagger.v3.oas.models.security.SecurityScheme;
import io.swagger.v3.oas.models.security.SecurityScheme.Type;

public class OpenIDConfig {
    private static final FluentLogger log = FluentLogger.forEnclosingClass();
    
    private URL wellKnown;
    
    private String issuer;
    private String client_id;
    private String idp_hint; // keycloak specific kc_idp_hint to direct federation
    
    private URL jwksUrl;
    

    public OpenIDConfig(URL wellKnown, String client_id, String idp_hint) throws IOException {
        this.wellKnown = wellKnown;
        this.idp_hint = idp_hint;
        this.client_id = client_id;
        HttpURLConnection http = null;
        try
        {
            http = (HttpURLConnection)wellKnown.openConnection();
            http.setRequestMethod("GET");
            http.setInstanceFollowRedirects(true);            
            int status = http.getResponseCode();
            if (status == 200) {
                ObjectMapper mapper = new ObjectMapper();
                JsonNode node = mapper.readTree(http.getInputStream());
                jwksUrl = new URL(node.get("jwks_uri").asText());
                issuer = node.get("issuer").asText();
            } else {
                log.atSevere().log("Unable to retrieve data from realm. Response code %d",status);
            }
        } finally {
            if (http != null) {
                http.disconnect();
            }
        }
    }
    
    public URL getJwksUrl() {
        return jwksUrl;
    }

    public SecurityScheme getScheme() {

        
        SecurityScheme scheme =  new SecurityScheme().type(Type.OPENIDCONNECT)
                                                    .openIdConnectUrl(wellKnown.toString())
                                                    .scheme("openid");
        if (idp_hint != null)
        {
            Map<String, Object> hint = new HashMap<>();
            hint.put("query-parameter", "kc_idp_hint");
            ArrayList<String> values = new ArrayList<>();
            for (String value: idp_hint.split(",")) {
                values.add(value.trim());
            }
            hint.put("values", values);
            scheme.addExtension("x-kc_idp_hint", hint);
        }

        scheme.addExtension("x-oidc-client-id", client_id);
        return scheme;
    }
}
