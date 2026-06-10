package cwms.cda.security;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.doAnswer;

import cwms.cda.ApiServlet;
import cwms.cda.datasource.ConnectionPreparingDataSource;
import io.javalin.http.Context;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.security.Keys;
import java.nio.charset.StandardCharsets;
import java.io.PrintWriter;
import java.security.Key;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;
import javax.sql.DataSource;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;

class BatchJobContextTest {
    private static final String SECRET = "test-batch-context-secret-32-characters";
    private static final String OTHER_SECRET = "other-batch-context-secret-32-chars";
    private static final String ISSUER = "cwms-batch-events";
    private static final String AUDIENCE = "cwms-data-api";
    private static final String MACHINE_USER = "SERVICE-ACCOUNT-CWMS-BATCH-RUNNER";

    @AfterEach
    void clearProperties() {
        System.clearProperty(BatchJobContext.SECRET_PROPERTY);
        System.clearProperty(BatchJobContext.PREVIOUS_SECRET_PROPERTY);
        System.clearProperty(BatchJobContext.KEY_ID_PROPERTY);
        System.clearProperty(BatchJobContext.ISSUER_PROPERTY);
        System.clearProperty(BatchJobContext.AUDIENCE_PROPERTY);
        System.clearProperty(BatchJobContext.MACHINE_USERS_PROPERTY);
    }

    @Test
    void machineUserDetectionReturnsFalseWhenUnset() {
        assertFalse(BatchJobContext.isBatchMachineUser(MACHINE_USER));
    }

    @Test
    void machineUserDetectionMatchesConfiguredUsersCaseInsensitively() {
        System.setProperty(BatchJobContext.MACHINE_USERS_PROPERTY,
            "some-user, service-account-cwms-batch-runner ");

        assertTrue(BatchJobContext.isBatchMachineUser(MACHINE_USER));
    }

    @Test
    void prepareContextDoesNotRequireTokenForNormalUsers() throws CwmsAuthException {
        Context ctx = contextWithHeaders(Map.of());
        DataApiPrincipal principal = new DataApiPrincipal("normal-user", Set.of());

        BatchJobContext.prepareContext(ctx, principal);

        assertNull(ctx.attribute(BatchJobContext.RUN_AS_OFFICE_ATTR));
    }

    @Test
    void validTokenSetsRunContextAttribute() throws CwmsAuthException {
        configureBatchContext();
        String token = token(Map.of(
            "run_as_office", "swt",
            "job_id", "job-123",
            "requested_by", "m5hectest",
            "dispatch_source", "api"
        ), SECRET, ISSUER, AUDIENCE, Instant.now().plusSeconds(300));
        Context ctx = contextWithHeaders(Map.of(BatchJobContext.HEADER, token));

        BatchJobContext.prepareContext(ctx, machinePrincipal());

        assertEquals("SWT", ctx.attribute(BatchJobContext.RUN_AS_OFFICE_ATTR));
    }

    @Test
    void missingTokenForMachineUserThrowsUnauthorized() {
        configureBatchContext();
        Context ctx = contextWithHeaders(Map.of());

        CwmsAuthException ex = assertThrows(CwmsAuthException.class,
            () -> BatchJobContext.prepareContext(ctx, machinePrincipal()));

        assertEquals(HttpServletResponse.SC_UNAUTHORIZED, ex.getAuthFailCode());
        assertEquals("Batch machine request missing signed job context", ex.getMessage());
    }

    @Test
    void expiredTokenThrowsSpecificUnauthorizedMessage() {
        configureBatchContext();
        String token = token(Map.of("run_as_office", "SWT"), SECRET, ISSUER, AUDIENCE,
            Instant.now().minusSeconds(60));
        Context ctx = contextWithHeaders(Map.of(BatchJobContext.HEADER, token));

        CwmsAuthException ex = assertThrows(CwmsAuthException.class,
            () -> BatchJobContext.prepareContext(ctx, machinePrincipal()));

        assertEquals(HttpServletResponse.SC_UNAUTHORIZED, ex.getAuthFailCode());
        assertEquals("Batch job context token expired", ex.getMessage());
    }

    @Test
    void forgedTokenThrowsInvalidMessage() {
        configureBatchContext();
        String token = token(Map.of("run_as_office", "SWT"), OTHER_SECRET, ISSUER, AUDIENCE,
            Instant.now().plusSeconds(300));
        Context ctx = contextWithHeaders(Map.of(BatchJobContext.HEADER, token));

        CwmsAuthException ex = assertThrows(CwmsAuthException.class,
            () -> BatchJobContext.prepareContext(ctx, machinePrincipal()));

        assertEquals(HttpServletResponse.SC_UNAUTHORIZED, ex.getAuthFailCode());
        assertEquals("Batch job context token not valid", ex.getMessage());
    }

    @Test
    void wrongIssuerOrAudienceIsRejected() {
        configureBatchContext();
        String wrongIssuer = token(Map.of("run_as_office", "SWT"), SECRET, "other-issuer",
            AUDIENCE, Instant.now().plusSeconds(300));
        String wrongAudience = token(Map.of("run_as_office", "SWT"), SECRET, ISSUER,
            "other-audience", Instant.now().plusSeconds(300));

        CwmsAuthException issuerEx = assertThrows(CwmsAuthException.class,
            () -> BatchJobContext.prepareContext(
                contextWithHeaders(Map.of(BatchJobContext.HEADER, wrongIssuer)), machinePrincipal()));
        CwmsAuthException audienceEx = assertThrows(CwmsAuthException.class,
            () -> BatchJobContext.prepareContext(
                contextWithHeaders(Map.of(BatchJobContext.HEADER, wrongAudience)), machinePrincipal()));

        assertEquals("Batch job context token not valid", issuerEx.getMessage());
        assertEquals("Batch job context token not valid", audienceEx.getMessage());
    }

    @Test
    void legacyOfficeClaimIsUsedWhenRunAsOfficeIsMissing() throws CwmsAuthException {
        configureBatchContext();
        String token = token(Map.of("office", "spk"), SECRET, ISSUER, AUDIENCE,
            Instant.now().plusSeconds(300));
        Context ctx = contextWithHeaders(Map.of(BatchJobContext.HEADER, token));

        BatchJobContext.prepareContext(ctx, machinePrincipal());

        assertEquals("SPK", ctx.attribute(BatchJobContext.RUN_AS_OFFICE_ATTR));
    }

    @Test
    void applyRunContextWrapsDataSourceWhenOfficeIsPresent() {
        Context ctx = contextWithAttributes();
        DataSource dataSource = new StubDataSource();
        ctx.attribute(ApiServlet.DATA_SOURCE, dataSource);
        ctx.attribute(BatchJobContext.RUN_AS_OFFICE_ATTR, "SWT");

        BatchJobContext.applyRunContext(ctx);

        assertInstanceOf(ConnectionPreparingDataSource.class, ctx.attribute(ApiServlet.DATA_SOURCE));
    }

    @Test
    void applyRunContextLeavesDataSourceUnchangedWhenOfficeIsMissing() {
        Context ctx = contextWithAttributes();
        DataSource dataSource = new StubDataSource();
        ctx.attribute(ApiServlet.DATA_SOURCE, dataSource);

        BatchJobContext.applyRunContext(ctx);

        assertSame(dataSource, ctx.attribute(ApiServlet.DATA_SOURCE));
    }

    private static void configureBatchContext() {
        System.setProperty(BatchJobContext.SECRET_PROPERTY, SECRET);
        System.setProperty(BatchJobContext.ISSUER_PROPERTY, ISSUER);
        System.setProperty(BatchJobContext.AUDIENCE_PROPERTY, AUDIENCE);
        System.setProperty(BatchJobContext.MACHINE_USERS_PROPERTY, MACHINE_USER);
    }

    private static DataApiPrincipal machinePrincipal() {
        return new DataApiPrincipal(MACHINE_USER, Set.of());
    }

    private static String token(Map<String, Object> claims, String secret, String issuer,
        String audience, Instant expiration) {
        Key key = Keys.hmacShaKeyFor(secret.getBytes(StandardCharsets.UTF_8));
        return Jwts.builder()
            .setHeaderParam("kid", "current")
            .setIssuer(issuer)
            .setAudience(audience)
            .setIssuedAt(Date.from(Instant.now()))
            .setExpiration(Date.from(expiration))
            .addClaims(claims)
            .signWith(key, SignatureAlgorithm.HS256)
            .compact();
    }

    private static Context contextWithHeaders(Map<String, String> headers) {
        return context(headers);
    }

    private static Context contextWithAttributes() {
        return context(Map.of());
    }

    private static Context context(Map<String, String> headers) {
        HttpServletRequest request = mock(HttpServletRequest.class);
        HttpServletResponse response = mock(HttpServletResponse.class);
        Map<String, Object> attributes = new HashMap<>();
        org.mockito.Mockito.when(request.getHeader(org.mockito.ArgumentMatchers.anyString()))
            .thenAnswer((Answer<String>) invocation -> headers.get(invocation.getArgument(0)));
        org.mockito.Mockito.when(request.getAttribute(org.mockito.ArgumentMatchers.anyString()))
            .thenAnswer((Answer<Object>) invocation -> attributes.get(invocation.getArgument(0)));
        doAnswer((Answer<Void>) invocation -> {
                attributes.put(invocation.getArgument(0), invocation.getArgument(1));
                return null;
            })
            .when(request).setAttribute(org.mockito.ArgumentMatchers.anyString(), org.mockito.ArgumentMatchers.any());
        return new Context(request, response, new HashMap<>());
    }

    private static final class StubDataSource implements DataSource {
        @Override
        public Connection getConnection() throws SQLException {
            throw new SQLException("not used");
        }

        @Override
        public Connection getConnection(String username, String password) throws SQLException {
            throw new SQLException("not used");
        }

        @Override
        public PrintWriter getLogWriter() {
            return null;
        }

        @Override
        public void setLogWriter(PrintWriter out) {
        }

        @Override
        public void setLoginTimeout(int seconds) {
        }

        @Override
        public int getLoginTimeout() {
            return 0;
        }

        @Override
        public Logger getParentLogger() {
            return Logger.getGlobal();
        }

        @Override
        public <T> T unwrap(Class<T> iface) throws SQLException {
            throw new SQLException("not used");
        }

        @Override
        public boolean isWrapperFor(Class<?> iface) {
            return false;
        }
    }
}
