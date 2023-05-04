package cwms.radar.security;

import java.io.IOException;
import java.math.BigInteger;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.Key;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.RSAPublicKeySpec;
import java.time.ZonedDateTime;
import java.util.Base64;
import java.util.HashMap;
import java.util.Set;
import java.util.Base64.Decoder;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.flogger.FluentLogger;

import cwms.radar.spi.RadarAccessManager;
import io.javalin.core.security.RouteRole;
import io.javalin.http.Context;
import io.javalin.http.Handler;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jws;
import io.jsonwebtoken.JwsHeader;
import io.jsonwebtoken.JwtParser;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SigningKeyResolverAdapter;
import io.swagger.v3.oas.models.security.OAuthFlow;
import io.swagger.v3.oas.models.security.OAuthFlows;
import io.swagger.v3.oas.models.security.Scopes;
import io.swagger.v3.oas.models.security.SecurityScheme;
import io.swagger.v3.oas.models.security.SecurityScheme.Type;

/**
 * This is currently more a placeholder for example than actual implementation
 */
public class OpenIDAccessManager extends RadarAccessManager {
    private static final FluentLogger log = FluentLogger.forEnclosingClass();
    private JwtParser jwtParser = null;
    private String wellKnownUrl = null;
    private OpenIDConfig config = null;

    public OpenIDAccessManager(String wellKnownUrl, String issuer, int realmKeyTimeout) {
        this.wellKnownUrl = wellKnownUrl;
        try {
            config = new OpenIDConfig(new URL(wellKnownUrl));
            jwtParser = Jwts.parserBuilder()
                        .requireIssuer(issuer)
                        .setSigningKeyResolver(new UrlResolver(config.getJwksUrl(),realmKeyTimeout))
                        .build();
        } catch (IOException ex) {
            log.atSevere().withCause(ex).log("Unable to initialize realm.");
        }
    }

    @Override
    public void manage(Handler handler, Context ctx, Set<RouteRole> routeRoles) throws Exception {
        String user = getUserFromToken(ctx);
        // TODO: prepare user
        handler.handle(ctx);
    }

    private String getUserFromToken(Context ctx) {
        Jws<Claims> token = jwtParser.parseClaimsJws(getToken(ctx));
        String edipi = token.getBody().get("edipi",String.class);
        return edipi;
    }

    private String getToken(Context ctx) {
        String header = ctx.header("Authorization");
        return header.split("\\s+")[1];
    }

    /**
     * TODO: build from the wellknown information.
     */
    @Override
    public SecurityScheme getScheme() {
        return config.getScheme();
    }

    @Override
    public String getName() {
        return "OpenIDConnect";
    }

    @Override
    public boolean canAuth(Context ctx, Set<RouteRole> roles) {
        String header = ctx.header("Authorization");
        if (header == null) {
            return false;
        }
        return header.trim().toLowerCase().startsWith("bearer");
    }


    private static class UrlResolver extends SigningKeyResolverAdapter {
        private URL jwksUrl;
        private ZonedDateTime lastCheck;
        private HashMap<String,Key> realmPublicKeys = new HashMap<>();
        private int realmPublicKeyTimeoutMinutes;
        private KeyFactory keyFactory = null; 

        public UrlResolver(URL jwksUrl, int keyTimeoutMinutes) {
            this.jwksUrl = jwksUrl;
            this.realmPublicKeyTimeoutMinutes = keyTimeoutMinutes;
            
        }

        /**
         * TODO: This needs more, some configurations may be more complex (like the
         * authelia test environment) than others.
         */
        private void updateKey() {
            if (realmPublicKeys.isEmpty() || ZonedDateTime.now().isAfter(lastCheck.plusMinutes(realmPublicKeyTimeoutMinutes))) {
                log.atInfo().log("Checking for new key at %s",jwksUrl);
                try {
                    realmPublicKeys.clear();
                    updateSigningKey();
                } catch (IOException ex) {
                    log.atSevere().withCause(ex).log("Unable to update key. Will continue to use previous key.");
                } catch (InvalidKeySpecException ex) {
                    log.atSevere().withCause(ex).log("New Public Key was not valid. Will continue to use previous key.");
                }
                lastCheck = ZonedDateTime.now();
            }
        }        

        private void updateSigningKey() throws IOException, InvalidKeySpecException {
            HttpURLConnection http = null;
            try
            {
                http = (HttpURLConnection)jwksUrl.openConnection();
                http.setRequestMethod("GET");
                http.setInstanceFollowRedirects(true);
                int status = http.getResponseCode();
                if (status == 200) {
                    ObjectMapper mapper = new ObjectMapper();
                    JsonNode keys = mapper.readTree(http.getInputStream()).get("keys");
                    for(JsonNode key: keys) {
                        String kid = key.get("kid").asText();
                        Decoder b64 = Base64.getDecoder();
                        BigInteger n = new BigInteger(b64.decode(key.get("n").asText()));
                        BigInteger e = new BigInteger(b64.decode(key.get("e").asText()));
                        Key pubKey = keyFactory.generatePublic(new RSAPublicKeySpec(n, e));    
                        realmPublicKeys.put(kid,pubKey);
                    }
                } else {
                    log.atSevere().log("Unable to retrieve actual keys. Response code %d",status);
                }
            } finally {
                if (http != null) {
                    http.disconnect();
                }
            }
        }

        @Override
        public Key resolveSigningKey(JwsHeader header, Claims claims) {
            if (!header.getAlgorithm().toLowerCase().startsWith("rs")) {
                return null; // we only deal with RSA keys right now.
            }
            updateKey();
            Key key = realmPublicKeys.get(header.getKeyId());
            return key;
        }
    }
}
