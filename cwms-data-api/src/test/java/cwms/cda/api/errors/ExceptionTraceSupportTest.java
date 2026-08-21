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

class ExceptionTraceSupportTest {

    @Test
    void includesStackTraceLinesWhenFeatureEnabledForTraceRole() {
        DataApiPrincipal principal = new DataApiPrincipal("trace-user",
                Set.of(new Role(ExceptionTraceSupport.SHOW_STACK_TRACE_ROLE_NAME)));

        Map<String, Serializable> details = ExceptionTraceSupport.buildDetails(Map.of(),
                new IllegalStateException("boom"),
                ExceptionTraceSupport.shouldIncludeStackTrace(principal, true));

        assertFalse(details.containsKey("stackTrace"));
        assertTrue(details.containsKey(ExceptionTraceSupport.STACK_TRACE_LINES_KEY));
        assertTrue(assertInstanceOf(Iterable.class, details.get(ExceptionTraceSupport.STACK_TRACE_LINES_KEY))
                .iterator().next().toString().contains("IllegalStateException"));
    }

    @Test
    void featureEnablesStackTraceForTraceRole() {
        DataApiPrincipal principal = new DataApiPrincipal("trace-user",
                Set.of(new Role(ExceptionTraceSupport.SHOW_STACK_TRACE_ROLE_NAME)));

        assertTrue(ExceptionTraceSupport.shouldIncludeStackTrace(principal, true));
    }

    @Test
    void omitsStackTraceWhenFeatureDisabled() {
        DataApiPrincipal principal = new DataApiPrincipal("trace-user",
                Set.of(new Role(ExceptionTraceSupport.SHOW_STACK_TRACE_ROLE_NAME)));

        Map<String, Serializable> details = ExceptionTraceSupport.buildDetails(Map.of(),
                new IllegalStateException("boom"),
                ExceptionTraceSupport.shouldIncludeStackTrace(principal, false));

        assertFalse(details.containsKey(ExceptionTraceSupport.STACK_TRACE_LINES_KEY));
    }

    @Test
    void omitsStackTraceForDifferentRole() {
        DataApiPrincipal principal = new DataApiPrincipal("normal-user",
                Set.of(new Role("CWMS Users")));

        assertFalse(ExceptionTraceSupport.shouldIncludeStackTrace(principal, true));
    }

    @Test
    void omitsStackTraceForGuestUser() {
        assertFalse(ExceptionTraceSupport.shouldIncludeStackTrace(null, true));
    }

    @Test
    void omitsStackTraceForUserAdminsRoleWithoutTraceRole() {
        DataApiPrincipal principal = new DataApiPrincipal("admin-user",
                Set.of(new Role("CWMS User Admins")));

        assertFalse(ExceptionTraceSupport.shouldIncludeStackTrace(principal, true));
    }
}
