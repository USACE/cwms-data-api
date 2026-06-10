package cwms.cda.security;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.flogger.FluentLogger;
import cwms.cda.ApiServlet;
import cwms.cda.datasource.ConnectionPreparingDataSource;
import cwms.cda.datasource.ConnectionPreparer;
import cwms.cda.datasource.DelegatingConnectionPreparer;
import cwms.cda.datasource.SessionOfficePreparer;
import io.javalin.http.Context;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.ExpiredJwtException;
import io.jsonwebtoken.JwtException;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.Key;
import java.util.Base64;
import java.util.Locale;
import java.util.Map;
import javax.sql.DataSource;
import javax.servlet.http.HttpServletResponse;

public final class BatchJobContext {
    private static final FluentLogger logger = FluentLogger.forEnclosingClass();
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static final String HEADER = "X-CWMS-Job-Context";
    public static final String RUN_AS_OFFICE_ATTR = "BatchRunAsOffice";
    public static final String MACHINE_AUTH_CLAIM = "machine_auth";
    public static final String RUN_AS_OFFICE_CLAIM = "run_as_office";
    public static final String LEGACY_OFFICE_CLAIM = "office";

    public static final String SECRET_PROPERTY = "cwms.dataapi.batch.jobContext.secret";
    public static final String PREVIOUS_SECRET_PROPERTY = "cwms.dataapi.batch.jobContext.previousSecret";
    public static final String KEY_ID_PROPERTY = "cwms.dataapi.batch.jobContext.keyId";
    public static final String ISSUER_PROPERTY = "cwms.dataapi.batch.jobContext.issuer";
    public static final String AUDIENCE_PROPERTY = "cwms.dataapi.batch.jobContext.audience";
    public static final String MACHINE_USERS_PROPERTY = "cwms.dataapi.batch.machineUsers";

    private static final String DEFAULT_ISSUER = "cwms-batch-events";
    private static final String DEFAULT_AUDIENCE = "cwms-data-api";
    private static final String DEFAULT_MACHINE_USERS = "";

    private BatchJobContext() {
    }

    public static boolean isBatchMachineUser(String username) {
        if (username == null) {
            return false;
        }
        String machineUsers = readSetting(MACHINE_USERS_PROPERTY, DEFAULT_MACHINE_USERS);
        if (machineUsers.isBlank()) {
            return false;
        }
        for (String machineUser : machineUsers.split(",")) {
            if (username.equalsIgnoreCase(machineUser.trim())) {
                return true;
            }
        }
        return false;
    }

    public static boolean isBatchMachinePrincipal(String username, Claims claims) {
        return hasMachineAuthClaim(claims) || isBatchMachineUser(username);
    }

    public static void prepareContext(Context ctx, DataApiPrincipal principal, Claims claims)
        throws CwmsAuthException {
        if (hasMachineAuthClaim(claims)) {
            setRunOfficeFromClaims(ctx, claims);
            return;
        }
        prepareContext(ctx, principal);
    }

    public static void prepareContext(Context ctx, DataApiPrincipal principal) throws CwmsAuthException {
        if (!isBatchMachineUser(principal.getName())) {
            return;
        }

        String token = ctx.header(HEADER);
        if (token == null || token.isBlank()) {
            throw new CwmsAuthException("Batch machine request missing signed job context",
                HttpServletResponse.SC_UNAUTHORIZED);
        }

        try {
            Claims claims = parse(token);
            setRunOfficeFromClaims(ctx, claims);
        } catch (ExpiredJwtException ex) {
            logger.atFine().withCause(ex).log("Batch job context token expired.");
            throw new CwmsAuthException("Batch job context token expired", ex,
                HttpServletResponse.SC_UNAUTHORIZED);
        } catch (JwtException | IllegalArgumentException ex) {
            logger.atFine().withCause(ex).log("Batch job context token validation failed.");
            throw new CwmsAuthException("Batch job context token not valid", ex,
                HttpServletResponse.SC_UNAUTHORIZED);
        }
    }

    private static boolean hasMachineAuthClaim(Claims claims) {
        if (claims == null) {
            return false;
        }
        Object value = claims.get(MACHINE_AUTH_CLAIM);
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        if (value instanceof String) {
            return Boolean.parseBoolean((String) value);
        }
        return false;
    }

    private static void setRunOfficeFromClaims(Context ctx, Claims claims) throws CwmsAuthException {
        String office = claims.get(RUN_AS_OFFICE_CLAIM, String.class);
        if (office == null || office.isBlank()) {
            office = claims.get(LEGACY_OFFICE_CLAIM, String.class);
        }
        if (office == null || office.isBlank()) {
            throw new CwmsAuthException("Batch job context missing run_as_office",
                HttpServletResponse.SC_UNAUTHORIZED);
        }
        ctx.attribute(RUN_AS_OFFICE_ATTR, office.toUpperCase(Locale.ROOT));
    }

    public static void applyRunContext(Context ctx) {
        String runAsOffice = ctx.attribute(RUN_AS_OFFICE_ATTR);
        if (runAsOffice == null || runAsOffice.isBlank()) {
            return;
        }

        DataSource dataSource = ctx.attribute(ApiServlet.DATA_SOURCE);
        ConnectionPreparer officePreparer = new SessionOfficePreparer(runAsOffice);
        if (dataSource instanceof ConnectionPreparingDataSource) {
            ConnectionPreparingDataSource preparingDataSource = (ConnectionPreparingDataSource) dataSource;
            preparingDataSource.setPreparer(new DelegatingConnectionPreparer(
                preparingDataSource.getPreparer(), officePreparer));
        } else {
            ctx.attribute(ApiServlet.DATA_SOURCE,
                new ConnectionPreparingDataSource(officePreparer, dataSource));
        }
    }

    private static Claims parse(String token) {
        String secret = secretForToken(token);
        if (secret.length() < 32) {
            throw new IllegalArgumentException("Batch job context secret must be at least 32 characters");
        }
        Key key = Keys.hmacShaKeyFor(secret.getBytes(StandardCharsets.UTF_8));
        return Jwts.parserBuilder()
            .requireIssuer(readSetting(ISSUER_PROPERTY, DEFAULT_ISSUER))
            .requireAudience(readSetting(AUDIENCE_PROPERTY, DEFAULT_AUDIENCE))
            .setSigningKey(key)
            .build()
            .parseClaimsJws(token)
            .getBody();
    }

    private static String secretForToken(String token) {
        String expectedKeyId = readSetting(KEY_ID_PROPERTY, "current");
        String keyId = keyIdForToken(token);
        if (keyId == null || keyId.isBlank() || expectedKeyId.equals(keyId)) {
            return readSetting(SECRET_PROPERTY, "");
        }
        if ("previous".equals(keyId)) {
            return readSetting(PREVIOUS_SECRET_PROPERTY, "");
        }
        throw new IllegalArgumentException("Batch job context key id is not recognized");
    }

    private static String keyIdForToken(String token) {
        String[] parts = token.split("\\.");
        if (parts.length != 3) {
            throw new IllegalArgumentException("Batch job context token is malformed");
        }
        try {
            byte[] headerBytes = Base64.getUrlDecoder().decode(parts[0]);
            Map<?, ?> header = OBJECT_MAPPER.readValue(headerBytes, Map.class);
            Object keyId = header.get("kid");
            return keyId instanceof String ? (String) keyId : null;
        } catch (IllegalArgumentException | IOException e) {
            throw new IllegalArgumentException("Batch job context token header is malformed", e);
        }
    }

    private static String readSetting(String key, String defaultValue) {
        String value = System.getProperty(key, System.getenv(key));
        return value == null || value.isBlank() ? defaultValue : value;
    }
}
