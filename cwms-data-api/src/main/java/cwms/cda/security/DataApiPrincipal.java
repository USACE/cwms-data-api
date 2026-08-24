package cwms.cda.security;

import io.javalin.core.security.RouteRole;
import java.security.Principal;
import java.util.Collections;
import java.util.Set;

public class DataApiPrincipal implements Principal {

    private String userName;
    private Set<RouteRole> roles;

    /**
     * Create Data-Api internal principal.
     * @param username cwmsdb username
     * @param roles list of cwms roles
     */
    public DataApiPrincipal(String username, Set<RouteRole> roles) {
        this.userName = username;
        this.roles = roles;
    }

    @Override
    public String getName() {
        return userName;
    }

    /**
     * Get the roles of this user.
     * @return RouteRole set for this user.
     */
    public Set<RouteRole> getRoles() {
        return Collections.unmodifiableSet(roles);
    }
    
}
