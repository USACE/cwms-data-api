package cwms.cda.api.errors;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import cwms.cda.security.DataApiPrincipal;
import cwms.cda.security.Role;
import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class ErrorTraceSupportTest {

    @AfterEach
    void clearProperties() {
        System.clearProperty(ErrorTraceSupport.ALWAYS_SHOW_STACK_TRACE_PROPERTY);
        System.clearProperty(ErrorTraceSupport.PRIMARY_ENVIRONMENT_PROPERTY);
        System.clearProperty(ErrorTraceSupport.LEGACY_ENVIRONMENT_PROPERTY);
        System.clearProperty(ErrorTraceSupport.WAR_CONTEXT_PROPERTY);
    }

    @Test
    void includesStackTraceForDevAdminUser() {
        DataApiPrincipal principal = new DataApiPrincipal("admin-user",
                Set.of(new Role("CWMS User Admins")));

        Map<String, Serializable> details = ErrorTraceSupport.buildDetails(Map.of(),
                new IllegalStateException("boom"),
                ErrorTraceSupport.shouldIncludeStackTrace(principal, "CWMS_DEV-West"));

        assertTrue(details.containsKey(ErrorTraceSupport.STACK_TRACE_KEY));
        assertTrue(details.get(ErrorTraceSupport.STACK_TRACE_KEY).toString()
                .contains("IllegalStateException"));
    }

    @Test
    void includesStackTraceForExistingUserAdminsRole() {
        DataApiPrincipal principal = new DataApiPrincipal("admin-user",
                Set.of(new Role("CWMS User Admins")));

        assertTrue(ErrorTraceSupport.shouldIncludeStackTrace(principal, "spk-dev"));
    }

    @Test
    void omitsStackTraceOutsideDev() {
        DataApiPrincipal principal = new DataApiPrincipal("admin-user",
                Set.of(new Role("CWMS User Admins")));

        Map<String, Serializable> details = ErrorTraceSupport.buildDetails(Map.of(),
                new IllegalStateException("boom"),
                ErrorTraceSupport.shouldIncludeStackTrace(principal, "production"));

        assertFalse(details.containsKey(ErrorTraceSupport.STACK_TRACE_KEY));
    }

    @Test
    void omitsStackTraceForNonAdminUser() {
        DataApiPrincipal principal = new DataApiPrincipal("normal-user",
                Set.of(new Role("CWMS Users")));

        assertFalse(ErrorTraceSupport.shouldIncludeStackTrace(principal, "dev"));
    }

    @Test
    void omitsStackTraceForGuestUser() {
        assertFalse(ErrorTraceSupport.shouldIncludeStackTrace(null, "dev"));
    }

    @Test
    void normalizesEnvironmentNameBeforeDevCheck() {
        assertTrue(ErrorTraceSupport.environmentLooksLikeDev("CWMS.Dev-West"));
        assertTrue(ErrorTraceSupport.environmentLooksLikeDev("cwms_dev_west"));
        assertFalse(ErrorTraceSupport.environmentLooksLikeDev("production"));
    }

    @Test
    void fallsBackToContextPathWhenEnvironmentPropertyMissing() {
        assertTrue(ErrorTraceSupport.environmentLooksLikeDev("/spk-data-dev"));
    }

    @Test
    void environmentPropertyStillResolvesForDevChecks() {
        System.setProperty(ErrorTraceSupport.PRIMARY_ENVIRONMENT_PROPERTY, "local-dev");

        assertTrue(ErrorTraceSupport.environmentLooksLikeDev(
                ErrorTraceSupport.resolveEnvironmentName(null)));
    }

    @Test
    void omitsStackTraceForUndefinedCwmsAdminRole() {
        DataApiPrincipal principal = new DataApiPrincipal("admin-user",
                Set.of(new Role("CWMS Admin")));

        assertFalse(ErrorTraceSupport.shouldIncludeStackTrace(principal, "dev"));
    }

    @Test
    void overrideEnablesStackTraceWithoutAdminRole() {
        DataApiPrincipal principal = new DataApiPrincipal("normal-user",
                Set.of(new Role("CWMS Users")));
        System.setProperty(ErrorTraceSupport.ALWAYS_SHOW_STACK_TRACE_PROPERTY, "true");

        assertTrue(ErrorTraceSupport.shouldIncludeStackTrace(principal, "production"));
    }

    @Test
    void overrideAddsStackTraceToDetails() {
        System.setProperty(ErrorTraceSupport.ALWAYS_SHOW_STACK_TRACE_PROPERTY, "true");

        Map<String, Serializable> details = ErrorTraceSupport.buildDetails(Map.of(),
                new IllegalStateException("boom"),
                ErrorTraceSupport.shouldIncludeStackTrace(null, "production"));

        assertTrue(details.containsKey(ErrorTraceSupport.STACK_TRACE_KEY));
    }

    @Test
    void localhostRequestEnablesStackTraceWithoutAuth() {
        assertTrue(ErrorTraceSupport.localhostRequestOverrideEnabled("localhost"));
        assertTrue(ErrorTraceSupport.localhostRequestOverrideEnabled("127.0.0.1"));
        assertFalse(ErrorTraceSupport.localhostRequestOverrideEnabled("example.com"));
    }
}
