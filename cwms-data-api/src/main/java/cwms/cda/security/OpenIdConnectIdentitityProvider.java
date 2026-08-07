package cwms.cda.security;

import com.google.auto.service.AutoService;
import com.google.common.flogger.FluentLogger;
import cwms.cda.ApiServlet;
import cwms.cda.data.dao.AuthDao;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.spi.IdentityProvider;
import io.javalin.http.Context;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jws;
import io.jsonwebtoken.JwtException;
import io.swagger.v3.oas.models.security.SecurityScheme;
import java.io.IOException;
import java.net.URL;
import java.security.Principal;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.servlet.http.HttpServletResponse;

@AutoService(IdentityProvider.class)
public final class OpenIdConnectIdentitityProvider implements IdentityProvider {
    private static final FluentLogger log = FluentLogger.forEnclosingClass();

    public static final String WELL_KNOWN_PROPERTY = "cwms.dataapi.access.openid.wellKnownUrl";
    public static final String CLIENT_ID = "cwms.dataapi.access.openid.clientId";
    public static final String IDP_HINT = "cwms.dataapi.access.openid.idpHint";
    public static final String ISSUER_PROPERTY = "cwms.dataapi.access.openid.issuer";
    public static final String TIMEOUT_PROPERTY = "cwms.dataapi.access.openid.timeout";
    public static final String AUTHORIZATION = "Authorization";
    public static final String CREATE_USERS_KEY = "cwms.dataapi.access.openid.create_users";
    public static final String EMAIL_CLAIM = "email";
    public static final String PREFERRED_USERNAME_CLAIM = "preferred_username";
    public static final String GIVEN_NAME_CLAIM = "given_name";


    private static final boolean CREATE_USERS = Boolean.parseBoolean(System.getProperty(CREATE_USERS_KEY,"true"));

    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);

    private final AtomicReference<OpenIdConfig> config = new AtomicReference<>(null);

    private final String wellKnownUrl;
    private final String issuer;
    private final String clientId;
    private final int timeout;
    private final String idpHint;

    /**
     * Create a new instance of OpenIDConnectProvider
     * Constructor will pull configuration from the environment with fixed property names.
     */
    public OpenIdConnectIdentitityProvider() {
        wellKnownUrl = System.getProperty(WELL_KNOWN_PROPERTY,System.getenv(WELL_KNOWN_PROPERTY));
        issuer = System.getProperty(ISSUER_PROPERTY,System.getenv(ISSUER_PROPERTY));
        String timeoutStr = System.getProperty(TIMEOUT_PROPERTY,System.getenv(TIMEOUT_PROPERTY));
        clientId = System.getProperty(CLIENT_ID, System.getenv(CLIENT_ID));
        idpHint = System.getProperty(IDP_HINT, System.getenv(IDP_HINT));
        if (timeoutStr != null && !timeoutStr.isEmpty()) {
            timeout = Integer.parseInt(timeoutStr);
        } else {
            timeout = 3600;
        }
        if (wellKnownUrl == null || wellKnownUrl.isEmpty()) {
            log.atInfo().log("OpenID Connect well-known URL is not set; provider will remain disabled.");
            executor.shutdown();
            return;
        }
        // try it once, then every 5 minutes until we get it.
        initializeProvider();
        if (config.get() == null) {
            log.atWarning().log("Could not initially configure OpenID Connect Configuration, will poll every 5 minutes");
            executor.scheduleAtFixedRate(this::initializeProvider, 5, 5, TimeUnit.MINUTES);
        }
    }

    private void initializeProvider() {
        var foundConfig = config.getAndUpdate(c -> {
            if (c != null) {
                return c; // already initialized, don't change it.
            }
            try {
                log.atFine().log("Attempting to initalize OIDC provider for %s", wellKnownUrl);
                URL wellKnown = new URL(wellKnownUrl);
                return OpenIdConfig.from(wellKnown, clientId, idpHint, timeout);
            } catch (IOException ex) {
                // The downstream users of this check if the Provider is valid and respond appropriate.
                // To test manually have OpenIDConfig throw an IOException so config stays null and
                // see the resulting explained failure in the logs.
                // That said it's possible we should maybe just have the system fail completely.
                log.atSevere().withCause(ex).log("Unable to initialize realm.");
            }
            return c;
        });
        if (foundConfig != null) { // If somehow the config was suddenly set just as we get to this line
                                   // the logic in config.getAndUpdate will take care of the situtation.
            log.atInfo().log("OpenID Config processed, shutting down polling thread.");
            executor.shutdown(); // we have it, don't need to keep polling
        }
    }

    @Override
    public Principal authenticate(Context ctx) {
        return getUserFromToken(ctx);
    }

    private DataApiPrincipal getUserFromToken(Context ctx) throws CwmsAuthException {
        try {
            Jws<Claims> token = config.get().getJwtParser().parseClaimsJws(getToken(ctx));
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
            log.atFine().withCause(ex).log(
                "JWT validation failed for bearer token from issuer configuration '%s'",
                System.getProperty(ISSUER_PROPERTY, System.getenv(ISSUER_PROPERTY))
            );
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
                throw new IllegalArgumentException(
                    String.format(AUTHORIZATION + " header:%s could not be split.", header));
            }
        }
    }

    @Override
    public SecurityScheme getScheme() {
        var configActual = config.get();
        return configActual != null ? configActual.getScheme() : null;
    }

    @Override
    public String getName() {
        return "OpenIDConnect";
    }

    @Override
    public boolean canAuth(Context ctx) {
        if (config.get() == null) {
            return false;
        }
        String header = ctx.header(AUTHORIZATION);
        if (header == null) {
            return false;
        }
        return header.trim().toLowerCase().startsWith("bearer");
    }
}