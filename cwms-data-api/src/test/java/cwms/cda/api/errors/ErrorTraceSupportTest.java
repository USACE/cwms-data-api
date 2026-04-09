package cwms.cda.api.errors;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import cwms.cda.security.DataApiPrincipal;
import cwms.cda.security.Role;
import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class ErrorTraceSupportTest {

    @Test
    void includesStackTraceWhenFeatureEnabledForAdminUser() {
        DataApiPrincipal principal = new DataApiPrincipal("admin-user",
                Set.of(new Role("CWMS User Admins")));

        Map<String, Serializable> details = ErrorTraceSupport.buildDetails(Map.of(),
                new IllegalStateException("boom"),
                ErrorTraceSupport.shouldIncludeStackTrace(principal, true));

        assertTrue(details.containsKey(ErrorTraceSupport.STACK_TRACE_KEY));
        assertTrue(details.containsKey(ErrorTraceSupport.STACK_TRACE_LINES_KEY));
        assertTrue(details.get(ErrorTraceSupport.STACK_TRACE_KEY).toString()
                .contains("IllegalStateException"));
        assertTrue(assertInstanceOf(Iterable.class, details.get(ErrorTraceSupport.STACK_TRACE_LINES_KEY))
                .iterator().next().toString().contains("IllegalStateException"));
    }

    @Test
    void featureEnablesStackTraceForUserAdminsRole() {
        DataApiPrincipal principal = new DataApiPrincipal("admin-user",
                Set.of(new Role("CWMS User Admins")));

        assertTrue(ErrorTraceSupport.shouldIncludeStackTrace(principal, true));
    }

    @Test
    void omitsStackTraceWhenFeatureDisabled() {
        DataApiPrincipal principal = new DataApiPrincipal("admin-user",
                Set.of(new Role("CWMS User Admins")));

        Map<String, Serializable> details = ErrorTraceSupport.buildDetails(Map.of(),
                new IllegalStateException("boom"),
                ErrorTraceSupport.shouldIncludeStackTrace(principal, false));

        assertFalse(details.containsKey(ErrorTraceSupport.STACK_TRACE_KEY));
        assertFalse(details.containsKey(ErrorTraceSupport.STACK_TRACE_LINES_KEY));
    }

    @Test
    void omitsStackTraceForNonAdminUser() {
        DataApiPrincipal principal = new DataApiPrincipal("normal-user",
                Set.of(new Role("CWMS Users")));

        assertFalse(ErrorTraceSupport.shouldIncludeStackTrace(principal, true));
    }

    @Test
    void omitsStackTraceForGuestUser() {
        assertFalse(ErrorTraceSupport.shouldIncludeStackTrace(null, true));
    }

    @Test
    void omitsStackTraceForUndefinedCwmsAdminRole() {
        DataApiPrincipal principal = new DataApiPrincipal("admin-user",
                Set.of(new Role("CWMS Admin")));

        assertFalse(ErrorTraceSupport.shouldIncludeStackTrace(principal, true));
    }

    @Test
    void localhostRequestEnablesStackTraceWithoutAuth() {
        assertTrue(ErrorTraceSupport.localhostRequestOverrideEnabled("localhost"));
        assertTrue(ErrorTraceSupport.localhostRequestOverrideEnabled("127.0.0.1"));
        assertFalse(ErrorTraceSupport.localhostRequestOverrideEnabled("example.com"));
    }
}
