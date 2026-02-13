package cwms.cda.helpers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.javalin.http.Context;
import java.util.List;
import org.junit.jupiter.api.Test;

final class AuthorizationContextHelperTest {

    private static final String FULL_AUTH_CONTEXT = "{"
        + "\"policy\": {\"allow\": true, \"decision_id\": \"test-123\"},"
        + "\"user\": {"
        + "  \"id\": \"m5hectest\","
        + "  \"username\": \"m5hectest\","
        + "  \"email\": \"m5hectest@test.com\","
        + "  \"roles\": [\"CWMS Users\", \"TS ID Creator\"],"
        + "  \"offices\": [\"SWT\", \"SPK\"],"
        + "  \"primary_office\": \"SWT\","
        + "  \"persona\": \"dam_operator\""
        + "},"
        + "\"constraints\": {"
        + "  \"allowed_offices\": [\"SWT\", \"SPK\"],"
        + "  \"embargo_exempt\": false"
        + "}"
        + "}";

    private AuthorizationContextHelper buildHelper(String headerValue) {
        Context ctx = mock(Context.class);
        when(ctx.header("x-cwms-auth-context")).thenReturn(headerValue);
        return new AuthorizationContextHelper(ctx);
    }

    @Test
    void testParseValidAuthContext() {
        AuthorizationContextHelper helper = buildHelper(FULL_AUTH_CONTEXT);
        assertTrue(helper.isAuthorizationHeaderPresent());
    }

    @Test
    void testNoHeader() {
        AuthorizationContextHelper helper = buildHelper(null);
        assertFalse(helper.isAuthorizationHeaderPresent());
        assertNull(helper.getUserId());
        assertNull(helper.getUsername());
    }

    @Test
    void testEmptyHeader() {
        AuthorizationContextHelper helper = buildHelper("");
        assertFalse(helper.isAuthorizationHeaderPresent());
    }

    @Test
    void testInvalidJsonHeader() {
        AuthorizationContextHelper helper = buildHelper("not-valid-json");
        assertFalse(helper.isAuthorizationHeaderPresent());
    }

    @Test
    void testExtractUserId() {
        AuthorizationContextHelper helper = buildHelper(FULL_AUTH_CONTEXT);
        assertEquals("m5hectest", helper.getUserId());
    }

    @Test
    void testExtractUsername() {
        AuthorizationContextHelper helper = buildHelper(FULL_AUTH_CONTEXT);
        assertEquals("m5hectest", helper.getUsername());
    }

    @Test
    void testExtractEmail() {
        AuthorizationContextHelper helper = buildHelper(FULL_AUTH_CONTEXT);
        assertEquals("m5hectest@test.com", helper.getEmail());
    }

    @Test
    void testExtractRoles() {
        AuthorizationContextHelper helper = buildHelper(FULL_AUTH_CONTEXT);
        List<String> roles = helper.getRoles();
        assertNotNull(roles);
        assertEquals(2, roles.size());
        assertTrue(roles.contains("CWMS Users"));
        assertTrue(roles.contains("TS ID Creator"));
    }

    @Test
    void testExtractOffices() {
        AuthorizationContextHelper helper = buildHelper(FULL_AUTH_CONTEXT);
        List<String> offices = helper.getOffices();
        assertNotNull(offices);
        assertEquals(2, offices.size());
        assertTrue(offices.contains("SWT"));
        assertTrue(offices.contains("SPK"));
    }

    @Test
    void testExtractPrimaryOffice() {
        AuthorizationContextHelper helper = buildHelper(FULL_AUTH_CONTEXT);
        assertEquals("SWT", helper.getPrimaryOffice());
    }

    @Test
    void testExtractPersona() {
        AuthorizationContextHelper helper = buildHelper(FULL_AUTH_CONTEXT);
        assertEquals("dam_operator", helper.getPersona());
    }

    @Test
    void testHasRole() {
        AuthorizationContextHelper helper = buildHelper(FULL_AUTH_CONTEXT);
        assertTrue(helper.hasRole("CWMS Users"));
        assertFalse(helper.hasRole("system_admin"));
    }

    @Test
    void testIsEmbargoExempt() {
        AuthorizationContextHelper helper = buildHelper(FULL_AUTH_CONTEXT);
        assertFalse(helper.isEmbargoExempt());
    }

    @Test
    void testIsEmbargoExemptTrue() {
        String json = "{"
            + "\"constraints\": {\"embargo_exempt\": true}"
            + "}";
        AuthorizationContextHelper helper = buildHelper(json);
        assertTrue(helper.isEmbargoExempt());
    }

    @Test
    void testNoUserContext() {
        String json = "{\"constraints\": {}}";
        AuthorizationContextHelper helper = buildHelper(json);
        assertNull(helper.getUserId());
        assertTrue(helper.getRoles().isEmpty());
        assertTrue(helper.getOffices().isEmpty());
    }

    @Test
    void testToStringContainsFields() {
        AuthorizationContextHelper helper = buildHelper(FULL_AUTH_CONTEXT);
        String str = helper.toString();
        assertTrue(str.contains("m5hectest"));
        assertTrue(str.contains("dam_operator"));
    }
}
