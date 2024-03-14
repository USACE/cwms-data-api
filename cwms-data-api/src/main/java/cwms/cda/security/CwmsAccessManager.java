package cwms.cda.security;

import cwms.auth.CwmsUserPrincipal;
import cwms.cda.spi.CdaAccessManager;
import cwms.cda.data.dao.AuthDao;
import io.javalin.core.security.RouteRole;
import io.javalin.http.Context;
import io.javalin.http.Handler;
import io.swagger.v3.oas.models.security.SecurityScheme;
import io.swagger.v3.oas.models.security.SecurityScheme.In;
import io.swagger.v3.oas.models.security.SecurityScheme.Type;
import java.security.Principal;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.jetbrains.annotations.NotNull;

public class CwmsAccessManager extends CdaAccessManager {
    private static final Logger logger = Logger.getLogger(CwmsAccessManager.class.getName());
    private static final String SESSION_COOKIE_NAME = "JSESSIONIDSSO";

    @Override
    public void manage(@NotNull Handler handler, @NotNull Context ctx,
                       @NotNull Set<RouteRole> requiredRoles)
            throws Exception {
        DataApiPrincipal p = getApiPrincipal(ctx);
        AuthDao.isAuthorized(ctx,p,requiredRoles);
        AuthDao.prepareContextWithUser(ctx, p);
        handler.handle(ctx);
    }

    private DataApiPrincipal getApiPrincipal(Context ctx) {
        Optional<String> user = getUser(ctx);
        if (user.isPresent()) {
            Set<RouteRole> roles = getRoles(ctx);
            return new DataApiPrincipal(user.get(), roles);
        } else {
            throw new CwmsAuthException("Invalid credentials provided",401);
        }
    }

    private static Optional<String> getUser(Context ctx) {
        Optional<String> retval = Optional.empty();
        if (ctx != null && ctx.req != null && ctx.req.getUserPrincipal() != null) {
            retval = Optional.of(ctx.req.getUserPrincipal().getName());
        } else {
            logger.log(Level.FINE, "No user principal found in request.");
        }
        return retval;
    }

    /**
     * Retrieve listed roles from the CwmsPrincipal. No additional db checks are required.
     * @param ctx Javalin Context
     * @return Set of roles
     */
    private static Set<RouteRole> getRoles(@NotNull Context ctx) {
        Objects.requireNonNull(ctx,"Configuration is horribly wrong. This system is not usable.");
        Set<RouteRole> retval = new LinkedHashSet<>();
        Principal principal = ctx.req.getUserPrincipal();

        Set<RouteRole> specifiedRoles = getRoles(principal);
        if (!specifiedRoles.isEmpty()) {
            retval.addAll(specifiedRoles);
        }

        return retval;
    }

    private static Set<RouteRole> getRoles(Principal principal) {
        Set<RouteRole> retval = new LinkedHashSet<>();
        if (principal != null) {
            List<String> roleNames;
            try {
                CwmsUserPrincipal cup = (CwmsUserPrincipal) principal;
                roleNames = cup.getRoles();
                if (roleNames != null) {
                    roleNames.stream().map(CwmsAccessManager::buildRole).forEach(retval::add);
                }
                logger.log(Level.FINE, "Principal had roles: {0}", retval);
            } catch (ClassCastException e) {
                logger.severe("cwmsaaa api and implementation jars should only be in the system "
                        + "classpath, not the war file. Verify and restart application");
            }
        } else {
            throw new CwmsAuthException("Provided User credentials are not valid.");
        }
        return retval;
    }

    public static RouteRole buildRole(String roleName) {
        return new Role(roleName);
    }

    @Override
    public SecurityScheme getScheme() {
        return new SecurityScheme()
                .type(Type.APIKEY)
                .in(In.COOKIE)
                .name(SESSION_COOKIE_NAME)
                .description("Auth handler running on same tomcat instance as the data api.");
    }

    @Override
    public String getName() {
        return "CwmsAAACacAuth";
    }

    @Override
    public boolean canAuth(Context ctx, Set<RouteRole> roles) {
        return ctx.cookie(SESSION_COOKIE_NAME) != null;
    }

}
