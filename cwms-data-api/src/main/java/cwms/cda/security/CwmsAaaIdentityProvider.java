package cwms.cda.security;

import com.google.auto.service.AutoService;
import cwms.cda.spi.IdentityProvider;
import io.javalin.core.security.RouteRole;
import io.javalin.http.Context;
import io.swagger.v3.oas.models.security.SecurityScheme;
import io.swagger.v3.oas.models.security.SecurityScheme.In;
import io.swagger.v3.oas.models.security.SecurityScheme.Type;

import java.security.Principal;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import com.google.common.flogger.FluentLogger;

import org.jetbrains.annotations.NotNull;

import cwms.auth.CwmsUserPrincipal;

@AutoService(IdentityProvider.class)
public class CwmsAaaIdentityProvider implements IdentityProvider {

    private static final FluentLogger logger = FluentLogger.forEnclosingClass();
    private static final String SESSION_COOKIE_NAME = "JSESSIONIDSSO";    

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
            logger.atFine().log("No user principal found in request.");
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
                    roleNames.stream().map(CwmsAaaIdentityProvider::buildRole).forEach(retval::add);
                }
                logger.atFine().log("Principal had roles: %s", retval);
            } catch (ClassCastException e) {
                logger.atSevere().log("cwmsaaa api and implementation jars should only be in the system "
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
    public boolean canAuth(Context ctx) {
        return ctx.cookie(SESSION_COOKIE_NAME) != null;
    }

    @Override
    public Principal authenticate(Context ctx) {
        return getApiPrincipal(ctx);
    }
}
