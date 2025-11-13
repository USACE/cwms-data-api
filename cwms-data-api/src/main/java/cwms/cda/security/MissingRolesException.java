package cwms.cda.security;

import java.util.List;
import javax.servlet.http.HttpServletResponse;

public class MissingRolesException extends CwmsAuthException {
    private final List<String> missingRoles;

    public MissingRolesException(List<String> missingRoles) {
        super(buildMessage(missingRoles), HttpServletResponse.SC_FORBIDDEN, buildMessage(missingRoles));
        this.missingRoles = missingRoles;
    }

    @Override
    public String getMessage() {
        return buildMessage(missingRoles);
    }

    private static String buildMessage(List<String> roles) {
        return "Missing roles {" + String.join(",",roles) + "}";
    }
}
