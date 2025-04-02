package cwms.cda.security;

import cwms.cda.ApiServlet;
import cwms.cda.data.dao.AuthDao;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.spi.IdentityProvider;
import io.javalin.core.security.RouteRole;
import io.javalin.http.Context;
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
import java.security.Principal;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.RSAPublicKeySpec;
import java.time.ZonedDateTime;
import java.util.Base64;
import java.util.Base64.Decoder;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import javax.servlet.http.HttpServletResponse;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.flogger.FluentLogger;



public class OpenIDAccessManagerProvider implements IdentityProvider {
    private static final FluentLogger log = FluentLogger.forEnclosingClass();

    public static final String WELL_KNOWN_PROPERTY = "cwms.dataapi.access.openid.wellKnownUrl";
    public static final String ALT_AUTH_URL = "cwms.dataapi.access.openid.altAuthUrl";
    public static final String ISSUER_PROPERTY = "cwms.dataapi.access.openid.issuer";
    public static final String TIMEOUT_PROPERTY = "cwms.dataapi.access.openid.timeout";
    public static final String AUTHORIZATION = "Authorization";
    public static final String CREATE_USERS_KEY = "cwms.dataapi.access.openid.create_users";
    public static final String EMAIL_CLAIM = "email";
    public static final String PREFERRED_USERNAME_CLAIM = "preferred_username";
    public static final String GIVEN_NAME_CLAIM = "given_name";


    private static final boolean CREATE_USERS = Boolean.parseBoolean(System.getProperty(CREATE_USERS_KEY,"true"));

    private JwtParser jwtParser = null;
    private OpenIDConfig config = null;

    public OpenIDAccessManagerProvider() {
        String wellKnownUrl = System.getProperty(WELL_KNOWN_PROPERTY,System.getenv(WELL_KNOWN_PROPERTY));
        String issuer = System.getProperty(ISSUER_PROPERTY,System.getenv(ISSUER_PROPERTY));
        String timeoutStr = System.getProperty(TIMEOUT_PROPERTY,System.getenv(TIMEOUT_PROPERTY));
        String altAuthUrl = System.getProperty(ALT_AUTH_URL, System.getenv(ALT_AUTH_URL));
        int timeout = 3600; 
        if (timeoutStr != null && !timeoutStr.isEmpty()) {
            timeout = Integer.parseInt(timeoutStr);
        }
        try {
            config = new OpenIDConfig(new URL(wellKnownUrl), altAuthUrl);
            jwtParser = Jwts.parserBuilder()
                        .requireIssuer(issuer)
                        .setSigningKeyResolver(new UrlResolver(config.getJwksUrl(),timeout))
                        .build();
        } catch (IOException ex) {
            log.atSevere().withCause(ex).log("Unable to initialize realm.");
        }
    }


    @Override
    public Principal authenticate(Context ctx) {
       return getUserFromToken(ctx);
    }

   private DataApiPrincipal getUserFromToken(Context ctx) throws CwmsAuthException {
        try {
            Jws<Claims> token = jwtParser.parseClaimsJws(getToken(ctx));
            Claims claims = token.getBody();
            final String issuer = claims.getIssuer();
            final String subject = claims.getSubject();
            final String oidcPrincipal = issuer + "::" + subject;
            AuthDao dao = AuthDao.getInstance(JooqDao.getDslContext(ctx), ctx.attribute(ApiServlet.OFFICE_ID));
            Optional<DataApiPrincipal> principal = dao.getPrincipalFromPrincipal(oidcPrincipal);
            if (principal.isPresent()) {
                return principal.get();
            } else if (CREATE_USERS) {
                final String preferredUserName = claims.get(PREFERRED_USERNAME_CLAIM, String.class);
                final String givenName = claims.get(GIVEN_NAME_CLAIM, String.class);
                final String email = claims.get(EMAIL_CLAIM, String.class);
                return dao.createUser(preferredUserName, oidcPrincipal, givenName, email);
            } else {
                throw new CwmsAuthException("Not Authorized",HttpServletResponse.SC_UNAUTHORIZED);
            }
        } catch (NumberFormatException | JwtException ex) {
            throw new CwmsAuthException("JWT not valid",ex,HttpServletResponse.SC_UNAUTHORIZED);
        }
    }

    private String getToken(Context ctx) {
        String header = ctx.header(AUTHORIZATION);
        if (header == null) {
            throw new IllegalStateException(AUTHORIZATION + " not found");
        } else {
            String[] parts = header.split("\\s+");
            if (parts.length >= 2) {
                return parts[1];
            } else {
                throw new IllegalArgumentException(String.format(AUTHORIZATION + " header:%s could not be split.", header));
            }
        }
    }

    @Override
    public SecurityScheme getScheme() {
        return config.getScheme();
    }

    @Override
    public String getName() {
        return "OpenIDConnect";
    }

    @Override
    public boolean canAuth(Context ctx) {
        String header = ctx.header(AUTHORIZATION);
        if (header == null) {
            return false;
        }
        return header.trim().toLowerCase().startsWith("bearer");
    }


    private static class UrlResolver extends SigningKeyResolverAdapter {
        private final URL jwksUrl;
        private ZonedDateTime lastCheck;
        private final Map<String,Key> realmPublicKeys = new HashMap<>();
        private final int realmPublicKeyTimeoutMinutes;
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
            try {
                http = (HttpURLConnection)jwksUrl.openConnection();
                http.setRequestMethod("GET");
                http.setInstanceFollowRedirects(true);
                int status = http.getResponseCode();
                if (status == 200) {
                    ObjectMapper mapper = new ObjectMapper();
                    JsonNode keys = mapper.readTree(http.getInputStream()).get("keys");
                    for (JsonNode key: keys) {
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
