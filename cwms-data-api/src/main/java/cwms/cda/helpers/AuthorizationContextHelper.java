package cwms.cda.helpers;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.javalin.http.Context;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class AuthorizationContextHelper {
    private static final Logger LOGGER = Logger.getLogger(AuthorizationContextHelper.class.getName());
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String AUTH_CONTEXT_HEADER = "x-cwms-auth-context";

    private final Map<String, Object> authContext;
    private final Map<String, Object> userContext;
    private final Map<String, Object> constraints;

    public AuthorizationContextHelper(Context ctx) {
        this.authContext = parseAuthContextHeader(ctx);
        this.userContext = extractUserContext(authContext);
        this.constraints = extractConstraints(authContext);
    }

    private Map<String, Object> parseAuthContextHeader(Context ctx) {
        String headerValue = ctx.header(AUTH_CONTEXT_HEADER);
        if (headerValue == null || headerValue.isEmpty()) {
            LOGGER.log(Level.FINE, "No authorization context header found");
            return Collections.emptyMap();
        }

        try {
            return OBJECT_MAPPER.readValue(headerValue, Map.class);
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Failed to parse authorization context header", e);
            return Collections.emptyMap();
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> extractUserContext(Map<String, Object> authContext) {
        if (authContext.containsKey("user")) {
            return (Map<String, Object>) authContext.get("user");
        }
        return Collections.emptyMap();
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> extractConstraints(Map<String, Object> authContext) {
        if (authContext.containsKey("constraints")) {
            return (Map<String, Object>) authContext.get("constraints");
        }
        return Collections.emptyMap();
    }

    public String getUserId() {
        return (String) userContext.getOrDefault("id", null);
    }

    public String getUsername() {
        return (String) userContext.getOrDefault("username", null);
    }

    public String getEmail() {
        return (String) userContext.getOrDefault("email", null);
    }

    @SuppressWarnings("unchecked")
    public List<String> getRoles() {
        Object roles = userContext.get("roles");
        if (roles instanceof List) {
            return (List<String>) roles;
        }
        return Collections.emptyList();
    }

    @SuppressWarnings("unchecked")
    public List<String> getOffices() {
        Object offices = userContext.get("offices");
        if (offices instanceof List) {
            return (List<String>) offices;
        }
        return Collections.emptyList();
    }

    public String getPrimaryOffice() {
        return (String) userContext.getOrDefault("primary_office", null);
    }

    public String getPersona() {
        return (String) userContext.getOrDefault("persona", null);
    }

    public String getRegion() {
        return (String) userContext.getOrDefault("region", null);
    }

    public String getAllowedOfficesConstraint() {
        return (String) constraints.getOrDefault("allowed_offices", null);
    }

    public boolean isEmbargoExempt() {
        Object exempt = constraints.get("embargo_exempt");
        return exempt != null && (boolean) exempt;
    }

    public String getTimezone() {
        return (String) constraints.getOrDefault("timezone", null);
    }

    public boolean hasRole(String role) {
        return getRoles().contains(role);
    }

    public boolean hasOfficeAccess(String office) {
        if (office == null) {
            return false;
        }
        List<String> userOffices = getOffices();
        return userOffices.contains(office);
    }

    public String buildOfficeFilter() {
        String allowedOffices = getAllowedOfficesConstraint();
        if (allowedOffices != null && !allowedOffices.isEmpty()) {
            if ("*".equals(allowedOffices)) {
                return null;
            }
            return allowedOffices;
        }

        List<String> offices = getOffices();
        if (offices.isEmpty()) {
            return null;
        }

        return String.join(",", offices);
    }

    public boolean isAuthorizationHeaderPresent() {
        return !authContext.isEmpty();
    }

    public Map<String, Object> getFullContext() {
        return Collections.unmodifiableMap(authContext);
    }

    @Override
    public String toString() {
        return "AuthorizationContextHelper{" +
                "userId='" + getUserId() + '\'' +
                ", username='" + getUsername() + '\'' +
                ", offices=" + getOffices() +
                ", roles=" + getRoles() +
                ", persona='" + getPersona() + '\'' +
                '}';
    }
}
