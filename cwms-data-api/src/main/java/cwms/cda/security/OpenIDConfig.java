package cwms.cda.security;

import java.io.IOException;
import java.math.BigInteger;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.Key;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.RSAPublicKeySpec;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Base64.Decoder;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.flogger.FluentLogger;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.JwsHeader;
import io.jsonwebtoken.JwtParser;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SigningKeyResolverAdapter;
import io.swagger.v3.oas.models.security.SecurityScheme;
import io.swagger.v3.oas.models.security.SecurityScheme.Type;

public class OpenIDConfig {
    private static final FluentLogger log = FluentLogger.forEnclosingClass();

    private URL wellKnown;

    private String issuer;
    private String client_id;
    private String idp_hint; // keycloak specific kc_idp_hint to direct federation
    private JwtParser jwtParser;
    private URL jwksUrl;


    private OpenIDConfig(URL wellKnown, String client_id, String idp_hint, JwtParser jwtParser) throws IOException {
        this.wellKnown = wellKnown;
        this.idp_hint = idp_hint;
        this.client_id = client_id;
        this.jwtParser = jwtParser;
    }

    public URL getJwksUrl() {
        return jwksUrl;
    }

    public static OpenIDConfig from(URL wellKnown, String clientId, String idpHint, int timeout) throws IOException
    {
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
                URL jwksUrl = new URL(node.get("jwks_uri").asText());
                String issuer = node.get("issuer").asText();

                JwtParser jwtParser = Jwts.parserBuilder()
                        .requireIssuer(issuer)
                        .setSigningKeyResolver(new UrlResolver(jwksUrl, timeout))
                        .build();
                return new OpenIDConfig(wellKnown, clientId, idpHint, jwtParser);
            } else {
                log.atSevere().log("Unable to retrieve data from realm. Response code %d", status);
            }
        } finally {
            if (http != null) {
                http.disconnect();
            }
        }
        throw new IOException("Unable to retrieve OIDC information from provider.");
    }

    public JwtParser getJwtParser()
    {
        return this.jwtParser;
    }

    static SecurityScheme buildScheme(String wellKnownUrl, String clientId, String idpHint) {
        SecurityScheme scheme =  new SecurityScheme().type(Type.OPENIDCONNECT)
                                                    .openIdConnectUrl(wellKnownUrl);
        if (idpHint != null)
        {
            Map<String, Object> hint = new HashMap<>();
            hint.put("query-parameter", "kc_idp_hint");
            ArrayList<String> values = new ArrayList<>();
            for (String value: idpHint.split(",")) {
                values.add(value.trim());
            }
            hint.put("values", values);
            scheme.addExtension("x-kc_idp_hint", hint);
        }

        scheme.addExtension("x-oidc-client-id", clientId);
        return scheme;
    }

    public SecurityScheme getScheme() {
        return buildScheme(wellKnown.toString(), client_id, idp_hint);
    }


    private static class UrlResolver extends SigningKeyResolverAdapter {
        private static final ZoneId UTC = ZoneId.of("UTC");
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

        private void updateKey() {
            if (realmPublicKeys.isEmpty() || ZonedDateTime.now(UTC).isAfter(lastCheck.plusMinutes(realmPublicKeyTimeoutMinutes))) {
                log.atInfo().log("Checking for new key at %s",jwksUrl);
                try {
                    realmPublicKeys.clear();
                    updateSigningKey();
                } catch (IOException ex) {
                    log.atSevere().withCause(ex).log("Unable to update key. Will continue to use previous key.");
                } catch (InvalidKeySpecException ex) {
                    log.atSevere().withCause(ex).log("New Public Key was not valid. Will continue to use previous key.");
                } finally {
                    lastCheck = ZonedDateTime.now(UTC);
                }
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
