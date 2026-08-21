package cwms.cda.security;

import java.util.List;
import javax.servlet.http.HttpServletResponse;

public class MissingRolesException extends CwmsAuthException {
    private final List<String> missingRoles;
    private final String message;

    public MissingRolesException(List<String> missingRoles) {
        this(missingRoles, buildMessage(missingRoles));
    }

    public MissingRolesException(List<String> missingRoles, String message) {
        super(message, HttpServletResponse.SC_FORBIDDEN, message);
        this.missingRoles = missingRoles;
        this.message = message;
    }

    @Override
    public String getMessage() {
        return message;
    }

    private static String buildMessage(List<String> roles) {
        return "Missing roles {" + String.join(",",roles) + "}";
    }
}
