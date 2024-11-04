package cwms.cda.security;

import java.util.List;

import javax.servlet.http.HttpServletResponse;

public class MissingRolesException extends CwmsAuthException {
    private List<String> missingRoles;

    public MissingRolesException(List<String> missingRoles) {
        super("Missing Roles", HttpServletResponse.SC_FORBIDDEN);
        this.missingRoles = missingRoles;
    }

    @Override
    public String getMessage() {
        return "Missing roles {" + String.join(",",missingRoles) + "}";
    }
}
