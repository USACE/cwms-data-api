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

public final class ExceptionTraceSupport {
    public static final String STACK_TRACE_LINES_KEY = "stackTraceLines";
    static final String SHOW_STACK_TRACE_ROLE_NAME = "SHOW STACK TRACE";
    private static final Role SHOW_STACK_TRACE_ROLE = new Role(SHOW_STACK_TRACE_ROLE_NAME);

    private ExceptionTraceSupport() {
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
            merged.put(STACK_TRACE_LINES_KEY, stackTraceLinesOf(cause));
        }
        return Collections.unmodifiableMap(merged);
    }

    static boolean shouldIncludeStackTrace(Context ctx) {
        return stackTraceFeatureEnabled()
                && resolvePrincipal(ctx).map(ExceptionTraceSupport::hasStackTraceRole).orElse(false);
    }

    static boolean shouldIncludeStackTrace(DataApiPrincipal principal, boolean stackTraceFeatureEnabled) {
        return stackTraceFeatureEnabled && hasStackTraceRole(principal);
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

    static boolean hasStackTraceRole(DataApiPrincipal principal) {
        return principal != null
                && principal.getRoles().contains(SHOW_STACK_TRACE_ROLE);
    }

    static boolean stackTraceFeatureEnabled() {
        try {
            FeatureManager featureManager = FeatureContext.getFeatureManager();
            return featureManager.isActive(CdaFeatures.INCLUDE_ERROR_STACK_TRACES);
        } catch (Exception ignore) {
            return false;
        }
    }

    private static ArrayList<String> stackTraceLinesOf(Throwable cause) {
        StringWriter sw = new StringWriter();
        cause.printStackTrace(new PrintWriter(sw));
        ArrayList<String> lines = new ArrayList<>();
        Collections.addAll(lines, sw.toString().split("\\R"));
        return lines;
    }

}
