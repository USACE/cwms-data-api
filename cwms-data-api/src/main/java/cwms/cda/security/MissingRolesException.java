package cwms.cda.security;

import javax.servlet.http.HttpServletResponse;

public class MissingRolesException extends CwmsAuthException {
    private String[] missingRoles;

    public MissingRolesException(String []missingRoles) {
        super("Missing Roles", HttpServletResponse.SC_FORBIDDEN);
        this.missingRoles = missingRoles;
    }

    @Override
    public String getMessage() {
        return "Missing roles {" + String.join(",",missingRoles) + "}";
    }
}
