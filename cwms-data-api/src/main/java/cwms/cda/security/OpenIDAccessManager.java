package cwms.cda.security;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.flogger.FluentLogger;
import cwms.cda.ApiServlet;
import cwms.cda.data.dao.AuthDao;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.spi.CdaAccessManager;
import io.javalin.core.security.RouteRole;
import io.javalin.http.Context;
import io.javalin.http.Handler;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jws;
import io.jsonwebtoken.JwsHeader;
import io.jsonwebtoken.JwtException;
import io.jsonwebtoken.JwtParser;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SigningKeyResolverAdapter;
import io.swagger.v3.oas.models.security.SecurityScheme;
import java.io.IOException;
import java.math.BigInteger;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.Key;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.RSAPublicKeySpec;
import java.time.ZonedDateTime;
import java.util.Base64;
import java.util.Base64.Decoder;
import java.util.HashMap;
import java.util.Set;
import javax.servlet.http.HttpServletResponse;

/**
 * This is currently more a placeholder for example than actual implementation
 */
public class OpenIDAccessManager extends CdaAccessManager {
    private static final FluentLogger log = FluentLogger.forEnclosingClass();
    private JwtParser jwtParser = null;
    private OpenIDConfig config = null;


    public OpenIDAccessManager(String wellKnownUrl, String issuer, int realmKeyTimeout, String authUrl) {
        try {
            config = new OpenIDConfig(new URL(wellKnownUrl), authUrl);
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
        DataApiPrincipal p = getUserFromToken(ctx);
        AuthDao.isAuthorized(ctx,p,routeRoles);
        AuthDao.prepareContextWithUser(ctx, p);
        handler.handle(ctx);
    }

    private DataApiPrincipal getUserFromToken(Context ctx) throws CwmsAuthException {
        try {
            Jws<Claims> token = jwtParser.parseClaimsJws(getToken(ctx));
            String username = token.getBody().get("preferred_username",String.class);
            AuthDao dao = AuthDao.getInstance(JooqDao.getDslContext(ctx),ctx.attribute(ApiServlet.OFFICE_ID));
            String edipiStr = username.substring(username.lastIndexOf(".")+1);
            long edipi = Long.parseLong(edipiStr);
            return dao.getPrincipalFromEdipi(edipi);
        } catch (NumberFormatException | JwtException ex) {
            throw new CwmsAuthException("JWT not valid",ex,HttpServletResponse.SC_UNAUTHORIZED);
        }
    }

    private String getToken(Context ctx) {
        String header = ctx.header("Authorization");
        if (header == null) {
            throw new IllegalArgumentException("Authorization not found");
        } else {
            String[] parts = header.split("\\s+");
            if (parts.length >= 2) {
                return parts[1];
            } else {
                throw new IllegalArgumentException(String.format("Authorization header:%s could not be split.", header));
            }
        }
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
            try {
                keyFactory = KeyFactory.getInstance("RSA");
            } catch (NoSuchAlgorithmException ex) {
                log.atSevere().withCause(ex).log("Unable to initialize key factory.");
            }
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
                        String kid = key.get("kid").textValue();
                        Decoder b64 = Base64.getUrlDecoder(); // https://datatracker.ietf.org/doc/id/draft-jones-json-web-key-01.html#RFC4648
                        String nStr = key.get("n").textValue();
                        String eStr = key.get("e").textValue();
                        log.atInfo().log("Loading Key %s with parameters (n,e) -> (%s,%s)",kid,nStr,eStr);
                        BigInteger n = new BigInteger(1,b64.decode(nStr));
                        BigInteger e = new BigInteger(1,b64.decode(eStr));
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
                log.atWarning().log("Request with invalid algorithm '%s'",header.getAlgorithm());
                return null; // we only deal with RSA keys right now.
            }
            updateKey();
            Key key = realmPublicKeys.get(header.getKeyId());
            if (key == null) {
                log.atSevere().log("Key not found for id '%s'",header.getKeyId());
            }
            return key;
        }
    }
}
