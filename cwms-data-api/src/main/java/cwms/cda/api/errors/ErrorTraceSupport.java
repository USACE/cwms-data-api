package cwms.cda.api.errors;

import cwms.cda.data.dao.AuthDao;
import cwms.cda.security.DataApiPrincipal;
import cwms.cda.security.Role;
import cwms.cda.spi.IdentityProvider;
import io.javalin.http.Context;
import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public final class ErrorTraceSupport {
    public static final String STACK_TRACE_KEY = "stackTrace";
    public static final String STACK_TRACE_LINES_KEY = "stackTraceLines";
    static final String ALWAYS_SHOW_STACK_TRACE_PROPERTY = "cwms.dataapi.errors.alwaysShowStackTrace";
    static final String ALWAYS_SHOW_STACK_TRACE_VARIABLE = "CWMS_DATAAPI_ERRORS_ALWAYS_SHOW_STACK_TRACE";
    static final String PRIMARY_ENVIRONMENT_PROPERTY = "cwms.dataapi.environment.name";
    static final String PRIMARY_ENVIRONMENT_VARIABLE = "CWMS_DATAAPI_ENVIRONMENT_NAME";
    static final String HOST_ENVIRONMENT_VARIABLE = "ENVIRONMENT";
    static final String LEGACY_ENVIRONMENT_PROPERTY = "cda.environment.name";
    static final String LEGACY_ENVIRONMENT_VARIABLE = "CDA_ENVIRONMENT_NAME";
    static final String WAR_CONTEXT_PROPERTY = "warContext";
    private static final String DEV_MARKER = "dev";
    private static final Role CWMS_USER_ADMINS_ROLE = new Role("CWMS User Admins");

    private ErrorTraceSupport() {
    }

    public static CdaError buildError(Context ctx, String message, Throwable cause) {
        Map<String, Serializable> details = buildDetails(ctx, Collections.emptyMap(), cause);
        if (details.isEmpty()) {
            return new CdaError(message);
        }
        return new CdaError(message, details);
    }

    public static CdaError buildError(Context ctx, String message, String source,
            Map<String, Serializable> details, Throwable cause) {
        return new CdaError(message, source, buildDetails(ctx, details, cause));
    }

    static Map<String, Serializable> buildDetails(Context ctx, Map<String, Serializable> details,
            Throwable cause) {
        return buildDetails(details, cause, shouldIncludeStackTrace(ctx));
    }

    static Map<String, Serializable> buildDetails(Map<String, Serializable> details,
            Throwable cause, boolean includeStackTrace) {
        Map<String, Serializable> merged = new HashMap<>();
        if (details != null) {
            merged.putAll(details);
        }
        if (cause != null && includeStackTrace) {
            String stackTrace = stackTraceOf(cause);
            merged.put(STACK_TRACE_KEY, stackTrace);
            merged.put(STACK_TRACE_LINES_KEY, stackTraceLinesOf(stackTrace));
        }
        return Collections.unmodifiableMap(merged);
    }

    static boolean shouldIncludeStackTrace(Context ctx) {
        if (alwaysShowStackTraceOverrideEnabled()) {
            return true;
        }
        if (localhostRequestOverrideEnabled(ctx)) {
            return true;
        }
        return shouldIncludeStackTrace(resolvePrincipal(ctx).orElse(null), resolveEnvironmentName(ctx));
    }

    static boolean shouldIncludeStackTrace(DataApiPrincipal principal, String environmentName) {
        if (alwaysShowStackTraceOverrideEnabled()) {
            return true;
        }
        return hasAdminRole(principal) && environmentLooksLikeDev(environmentName);
    }

    static boolean alwaysShowStackTraceOverrideEnabled() {
        return Boolean.parseBoolean(firstNonBlank(
                System.getProperty(ALWAYS_SHOW_STACK_TRACE_PROPERTY),
                System.getenv(ALWAYS_SHOW_STACK_TRACE_VARIABLE),
                "false"
        ));
    }

    static boolean localhostRequestOverrideEnabled(Context ctx) {
        if (ctx == null || ctx.req == null) {
            return false;
        }
        return localhostRequestOverrideEnabled(ctx.req.getServerName());
    }

    static boolean localhostRequestOverrideEnabled(String serverName) {
        return serverName != null
                && ("localhost".equalsIgnoreCase(serverName)
                || "127.0.0.1".equals(serverName)
                || "::1".equals(serverName));
    }

    static Optional<DataApiPrincipal> resolvePrincipal(Context ctx) {
        if (ctx == null) {
            return Optional.empty();
        }
        DataApiPrincipal attributePrincipal = ctx.attribute(AuthDao.DATA_API_PRINCIPAL);
        if (attributePrincipal != null) {
            return Optional.of(attributePrincipal);
        }
        DataApiPrincipal sessionPrincipal = ctx.sessionAttribute(IdentityProvider.PRINCIPAL_KEY);
        return Optional.ofNullable(sessionPrincipal);
    }

    static boolean hasAdminRole(DataApiPrincipal principal) {
        return principal != null
                && principal.getRoles().contains(CWMS_USER_ADMINS_ROLE);
    }

    static String resolveEnvironmentName(Context ctx) {
        String configuredName = firstNonBlank(
                System.getProperty(PRIMARY_ENVIRONMENT_PROPERTY),
                System.getenv(PRIMARY_ENVIRONMENT_VARIABLE),
                System.getenv(HOST_ENVIRONMENT_VARIABLE),
                System.getProperty(LEGACY_ENVIRONMENT_PROPERTY),
                System.getenv(LEGACY_ENVIRONMENT_VARIABLE),
                System.getProperty(WAR_CONTEXT_PROPERTY)
        );
        if (configuredName != null) {
            return configuredName;
        }
        if (ctx == null) {
            return "";
        }
        return firstNonBlank(ctx.contextPath(), ctx.req != null ? ctx.req.getContextPath() : null, "");
    }

    static boolean environmentLooksLikeDev(String environmentName) {
        return normalizeEnvironmentName(environmentName).contains(DEV_MARKER);
    }

    static String normalizeEnvironmentName(String environmentName) {
        if (environmentName == null) {
            return "";
        }
        return environmentName.toLowerCase().replaceAll("[^a-z0-9]", "");
    }

    private static String stackTraceOf(Throwable cause) {
        StringWriter sw = new StringWriter();
        cause.printStackTrace(new PrintWriter(sw));
        return sw.toString();
    }

    private static ArrayList<String> stackTraceLinesOf(String stackTrace) {
        ArrayList<String> lines = new ArrayList<>();
        Collections.addAll(lines, stackTrace.split("\\R"));
        return lines;
    }

    private static String firstNonBlank(String... values) {
        for (String value : values) {
            if (value != null && !value.isBlank()) {
                return value;
            }
        }
        return null;
    }
}
