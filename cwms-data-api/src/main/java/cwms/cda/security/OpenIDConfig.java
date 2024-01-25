package cwms.cda.security;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.flogger.FluentLogger;

import io.swagger.v3.oas.models.security.OAuthFlow;
import io.swagger.v3.oas.models.security.OAuthFlows;
import io.swagger.v3.oas.models.security.Scopes;
import io.swagger.v3.oas.models.security.SecurityScheme;
import io.swagger.v3.oas.models.security.SecurityScheme.In;
import io.swagger.v3.oas.models.security.SecurityScheme.Type;

public class OpenIDConfig {
    private static final FluentLogger log = FluentLogger.forEnclosingClass();
    private URL wellKnown;
    private String issuer;
    private URL authUrl;
    private URL tokenUrl;
    private URL userInfoUrl;
    private URL logoutUrl;
    private URL jwksUrl;
    
    private Scopes scopes = new Scopes();
    private OAuthFlows flows = new OAuthFlows();

    public OpenIDConfig(URL wellKnown, String altAuthUrl) throws IOException {
        this.wellKnown = wellKnown;
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
                tokenUrl = substituteBase(new URL(node.get("token_endpoint").asText()),altAuthUrl);
                userInfoUrl = substituteBase(new URL(node.get("userinfo_endpoint").asText()),altAuthUrl);
                logoutUrl = substituteBase(new URL(node.get("end_session_endpoint").asText()),altAuthUrl);
                authUrl = substituteBase(new URL(node.get("authorization_endpoint").asText()),altAuthUrl);
                JsonNode scopes = node.get("scopes_supported");
                for(JsonNode scope: scopes) {
                    this.scopes.addString(scope.asText(), "");
                }

                JsonNode grants = node.get("grant_types_supported");
                for(JsonNode grant: grants) {
                    OAuthFlow flow = new OAuthFlow();
                    flow.setTokenUrl(tokenUrl.toString());
                    flow.setAuthorizationUrl(authUrl.toString());
                    flow.setScopes(this.scopes);
                    String grantStr = grant.asText();
                    if (grantStr.equalsIgnoreCase("implicit")) {
                        flows.setImplicit(flow);
                    } else if(grantStr.equalsIgnoreCase("password")) {
                        flows.setPassword(flow);
                    } else if(grantStr.equalsIgnoreCase("authorization_code")) {
                        flows.setAuthorizationCode(flow);
                    } else if (grantStr.equalsIgnoreCase("client_credentials")) {
                        flows.setClientCredentials(flow);
                    }
                }


            } else {
                log.atSevere().log("Unable to retrieve data from realm. Response code %d",status);
            }
        } finally {
            if (http != null) {
                http.disconnect();
            }
        }
    }

    private URL substituteBase(URL endPoint, String altAuthUrl) throws MalformedURLException {
        if (altAuthUrl == null) {
            return endPoint;
        }
        log.atInfo().log("Changing '%s' with '%s'", endPoint.toString(), altAuthUrl);
        String originalPath = endPoint.getPath();
        log.atInfo().log("New Path = %s", altAuthUrl+originalPath);
        return new URL(altAuthUrl+originalPath);
    }

    public URL getJwksUrl() {
        return jwksUrl;
    }

    public SecurityScheme getScheme() {
        return new SecurityScheme().type(Type.OPENIDCONNECT)
                                   .openIdConnectUrl(wellKnown.toString())
                                   .name("Authorization")
                                   .flows(flows)
                                   .in(In.HEADER);
    }
}
