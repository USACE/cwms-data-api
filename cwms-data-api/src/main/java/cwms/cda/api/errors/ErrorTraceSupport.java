package cwms.cda.api.errors;

import cwms.cda.data.dao.AuthDao;
import cwms.cda.features.CdaFeatures;
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
import java.util.Map;
import java.util.Optional;
import org.togglz.core.context.FeatureContext;
import org.togglz.core.manager.FeatureManager;

public final class ErrorTraceSupport {
    public static final String STACK_TRACE_KEY = "stackTrace";
    public static final String STACK_TRACE_LINES_KEY = "stackTraceLines";
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
        if (localhostRequestOverrideEnabled(ctx)) {
            return true;
        }
        return shouldIncludeStackTrace(resolvePrincipal(ctx).orElse(null), stackTraceFeatureEnabled());
    }

    static boolean shouldIncludeStackTrace(DataApiPrincipal principal, boolean stackTraceFeatureEnabled) {
        return stackTraceFeatureEnabled && hasAdminRole(principal);
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

    static boolean stackTraceFeatureEnabled() {
        try {
            FeatureManager featureManager = FeatureContext.getFeatureManager();
            return featureManager.isActive(CdaFeatures.INCLUDE_ERROR_STACK_TRACES);
        } catch (Throwable ignore) {
            return false;
        }
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

}
