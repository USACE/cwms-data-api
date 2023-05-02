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
import java.security.spec.KeySpec;
import java.security.spec.RSAPublicKeySpec;
import java.security.spec.X509EncodedKeySpec;
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
import io.swagger.v3.oas.models.security.SecurityScheme;
import io.swagger.v3.oas.models.security.SecurityScheme.Type;

/**
 * This is currently more a placeholder for example than actual implementation
 */
public class OpenIDAccessManager extends RadarAccessManager {
    private static final FluentLogger log = FluentLogger.forEnclosingClass();
    private JwtParser jwtParser = null;
    private String wellKnownUrl = null;

    public OpenIDAccessManager(String wellKnownUrl, String issuer, int realmKeyTimeout) {
        this.wellKnownUrl = wellKnownUrl;
        try {
            jwtParser = Jwts.parserBuilder()
                        .requireIssuer(issuer)
                        .setSigningKeyResolver(new UrlResolver(wellKnownUrl,realmKeyTimeout))
                        .build();
        } catch (MalformedURLException ex) {
            log.atSevere().log("Unable to initialize realm.",ex);
            throw new RuntimeException("Unable to initialized required realm.",ex);
        } catch (NoSuchAlgorithmException ex) {
            throw new RuntimeException("This platform does not support RSA encryption. Unable to setup OpenID Manager",ex);
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

    @Override
    public SecurityScheme getScheme() {
        return new SecurityScheme().type(Type.OPENIDCONNECT)
                                   .openIdConnectUrl(wellKnownUrl);
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
        private URL realmUrl;
        private URL jwksUrl;
        private ZonedDateTime lastCheck;
        private HashMap<String,Key> realmPublicKeys = new HashMap<>();
        private int realmPublicKeyTimeoutMinutes;
        private KeyFactory keyFactory = null; 

        public UrlResolver(String realmUrl, int keyTimeoutMinutes) 
                throws MalformedURLException, NoSuchAlgorithmException {
            this.realmUrl = new URL(realmUrl);
            this.realmPublicKeyTimeoutMinutes = keyTimeoutMinutes;
            updateKey();
            lastCheck = ZonedDateTime.now();
            keyFactory = KeyFactory.getInstance("RSA");
        }

        /**
         * TODO: This needs more, some configurations may be more complex (like the
         * authelia test environment) than others.
         */
        private void updateKey() {
            if (realmPublicKeys.isEmpty() || ZonedDateTime.now().isAfter(lastCheck.plusMinutes(realmPublicKeyTimeoutMinutes))) {
                log.atInfo().log("Checking for new key at %s",realmUrl);
                try {
                    realmPublicKeys.clear();
                    updateJwksUrl();
                    updateSigningKey();
                } catch (IOException ex) {
                    log.atSevere().log("Unable to update key. Will continue to use previous key.",ex);
                } catch (InvalidKeySpecException ex) {
                    log.atSevere().log("New Public Key was not valid. Will continue to use previous key.",ex);
                }
                lastCheck = ZonedDateTime.now();
            }
        }

        private void updateJwksUrl() throws IOException {
            HttpURLConnection http = null;
            try
            {
                http = (HttpURLConnection)realmUrl.openConnection();
                http.setRequestMethod("GET");
                http.setInstanceFollowRedirects(true);
                int status = http.getResponseCode();
                if (status == 200) {
                    ObjectMapper mapper = new ObjectMapper();
                    JsonNode node = mapper.readTree(http.getInputStream());
                    jwksUrl = new URL(node.get("jwks_uri").asText());
                } else {
                    log.atSevere().log("Unable to retrieve data from realm. Response code %d",status);
                }
            } finally {
                if (http != null) {
                    http.disconnect();
                }
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
