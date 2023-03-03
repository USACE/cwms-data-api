package cwms.radar.security;

import cwms.auth.CwmsUserPrincipal;
import cwms.radar.ApiServlet;
import cwms.radar.api.errors.RadarError;
import cwms.radar.datasource.ConnectionPreparer;
import cwms.radar.datasource.ConnectionPreparingDataSource;
import cwms.radar.datasource.DelegatingConnectionPreparer;
import cwms.radar.datasource.DirectUserPreparer;
import cwms.radar.datasource.SessionOfficePreparer;
import cwms.radar.spi.RadarAccessManager;
import io.javalin.core.security.RouteRole;
import io.javalin.http.Context;
import io.javalin.http.Handler;
import io.swagger.v3.oas.models.security.SecurityScheme;
import io.swagger.v3.oas.models.security.SecurityScheme.In;
import io.swagger.v3.oas.models.security.SecurityScheme.Type;
import java.security.Principal;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.servlet.http.HttpServletResponse;
import javax.sql.DataSource;
import org.jetbrains.annotations.NotNull;

public class CwmsAccessManager extends RadarAccessManager {
    public static final Logger logger = Logger.getLogger(CwmsAccessManager.class.getName());

    @Override
    public void manage(@NotNull Handler handler, @NotNull Context ctx,
                       @NotNull Set<RouteRole> requiredRoles)
            throws Exception {
        boolean shouldProceed = isAuthorized(ctx, requiredRoles);

        if (shouldProceed) {
            buildDataSource(ctx);

            // Let the handler handle the request.
            handler.handle(ctx);
        } else {
            String msg = getFailMessage(ctx, requiredRoles);
            logger.info(msg);
            ctx.status(HttpServletResponse.SC_UNAUTHORIZED).json(RadarError.notAuthorized());
            ctx.status(401).result("Unauthorized");
        }
    }

    private void buildDataSource(@NotNull Context ctx) {
        ConnectionPreparer userPreparer = null;
        Optional<String> user = getUser(ctx);
        if(user.isPresent()) {
            userPreparer = new DirectUserPreparer(user.get());
        }

        ConnectionPreparer officePrepare = null;
        Optional<String> office = getOffice(ctx);
        if(office.isPresent()) {
            officePrepare = new SessionOfficePreparer(office.get());
        }

        if(user.isPresent() || office.isPresent()) {
            DelegatingConnectionPreparer newPreparer = new DelegatingConnectionPreparer(officePrepare, userPreparer);
            DataSource dataSource = ctx.attribute(ApiServlet.DATA_SOURCE);
            if (dataSource instanceof ConnectionPreparingDataSource) {
                ConnectionPreparingDataSource cpDs = (ConnectionPreparingDataSource) dataSource;
                ConnectionPreparer existingPreparer = cpDs.getPreparer();

                // Have it do our extra step last.
                cpDs.setPreparer(new DelegatingConnectionPreparer(existingPreparer, newPreparer));
            } else {
                ctx.attribute(ApiServlet.DATA_SOURCE, new ConnectionPreparingDataSource(newPreparer, dataSource));
            }
        } else {
            logger.log(Level.FINE, "No user or office found in request, not adding a DataSource Preparer.");
        }
    }

    private static Optional<String> getOffice(Context ctx) {
        Optional<String> retval = Optional.empty();
        Map<String, Object> attributeMap = Collections.emptyMap();
        if (ctx != null) {
            attributeMap = ctx.attributeMap();
        }

        if(attributeMap.containsKey("office")) {
            String attr = String.valueOf(attributeMap.get("office"));
            if(attr != null && !attr.isEmpty()) {
                retval = Optional.of(attr);
            } else {
                logger.log(Level.FINE, "office attribute value was null or empty");
            }
        } else {
            logger.log(Level.FINE, "No office attribute");
        }

        return retval;
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

    public static String getFailMessage(@NotNull Context ctx, @NotNull Set<RouteRole> requiredRoles) {
        Set<RouteRole> specifiedRoles = getRoles(ctx);
        Set<RouteRole> missing = new LinkedHashSet<>(requiredRoles);
        missing.removeAll(specifiedRoles);

        return "Request for: " + ctx.req.getRequestURI() + " denied. Missing roles: " + missing;
    }


    public boolean isAuthorized(Context ctx, Set<RouteRole> requiredRoles) {
        boolean retval;
        if (requiredRoles == null || requiredRoles.isEmpty()) {
            retval = true;
        } else {
            Set<RouteRole> specifiedRoles = getRoles(ctx);
            retval = specifiedRoles.containsAll(requiredRoles);
        }
        return retval;
    }

    public static Set<RouteRole> getRoles(Context ctx) {
        Set<RouteRole> retval = new LinkedHashSet<>();
        if (ctx != null) {
            Principal principal = ctx.req.getUserPrincipal();

            Set<RouteRole> specifiedRoles = getRoles(principal);
            if (!specifiedRoles.isEmpty()) {
                retval.addAll(specifiedRoles);
            }
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
                logger.fine("Principal had roles: " + retval);
            } catch (ClassCastException e) {
                logger.severe("cwmsaaa api and implementation jars should only be in the system "
                        + "classpath, not the war file. Verify and restart application");
            }
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
                .name("JSESSIONIDSSO");
    }

}
