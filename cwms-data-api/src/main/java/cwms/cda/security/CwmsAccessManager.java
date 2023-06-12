package cwms.cda.security;

import cwms.auth.CwmsUserPrincipal;
import cwms.cda.spi.CdaAccessManager;
import cwms.cda.ApiServlet;
import cwms.cda.api.Controllers;
import cwms.cda.api.errors.CdaError;
import cwms.cda.datasource.ConnectionPreparer;
import cwms.cda.datasource.ConnectionPreparingDataSource;
import cwms.cda.datasource.DelegatingConnectionPreparer;
import cwms.cda.datasource.DirectUserPreparer;
import cwms.cda.datasource.SessionOfficePreparer;
import io.javalin.core.security.RouteRole;
import io.javalin.http.Context;
import io.javalin.http.Handler;
import io.swagger.v3.oas.models.security.SecurityScheme;
import io.swagger.v3.oas.models.security.SecurityScheme.In;
import io.swagger.v3.oas.models.security.SecurityScheme.Type;
import java.security.Principal;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.servlet.http.HttpServletResponse;
import javax.sql.DataSource;
import org.jetbrains.annotations.NotNull;

public class CwmsAccessManager extends CdaAccessManager {
    private static final Logger logger = Logger.getLogger(CwmsAccessManager.class.getName());
    private static final String SESSION_COOKIE_NAME = "JSESSIONIDSSO";

    @Override
    public void manage(@NotNull Handler handler, @NotNull Context ctx,
                       @NotNull Set<RouteRole> requiredRoles)
            throws Exception {

        if (isAuthorized(ctx, requiredRoles)) {
            buildDataSource(ctx);

            // Let the handler handle the request.
            handler.handle(ctx);
        } else {
            throw new CwmsAuthException("Access not authorized",HttpServletResponse.SC_FORBIDDEN);
        }
    }

    private void buildDataSource(@NotNull Context ctx) {
        ConnectionPreparer userPreparer = null;
        Optional<String> user = getUser(ctx);
        if (user.isPresent()) {
            userPreparer = new DirectUserPreparer(user.get());
        }

        ConnectionPreparer officePrepare = null;
        Optional<String> office = getOffice(ctx);
        if (office.isPresent()) {
            officePrepare = new SessionOfficePreparer(office.get());
        }

        if (user.isPresent() || office.isPresent()) {
            DelegatingConnectionPreparer newPreparer =
                    new DelegatingConnectionPreparer(officePrepare, userPreparer);
            DataSource dataSource = ctx.attribute(ApiServlet.DATA_SOURCE);
            if (dataSource instanceof ConnectionPreparingDataSource) {
                ConnectionPreparingDataSource cpDs = (ConnectionPreparingDataSource) dataSource;
                ConnectionPreparer existingPreparer = cpDs.getPreparer();

                // Have it do our extra step last.
                cpDs.setPreparer(new DelegatingConnectionPreparer(existingPreparer, newPreparer));
            } else {
                ctx.attribute(ApiServlet.DATA_SOURCE,
                        new ConnectionPreparingDataSource(newPreparer, dataSource));
            }
        } else {
            logger.log(Level.FINE, "No user or office found in request, not adding a DataSource Preparer.");
        }
    }

    private static Optional<String> getOffice(Context ctx) {
        Optional<String> retval = getStringAttribute(Controllers.OFFICE, ctx);
        if (!retval.isPresent()) {
            // ApiServet guesses the office from the context and puts it in office_id.
            retval = getStringAttribute(ApiServlet.OFFICE_ID, ctx);
        }
        return retval;
    }

    private static Optional<String> getStringAttribute(String attrName, Context ctx) {
        Optional<String> retval = Optional.empty();
        if (ctx != null && attrName != null && !attrName.isEmpty()) {
            Map<String, Object> attributeMap = ctx.attributeMap();
            String attr = null;
            if (attributeMap.containsKey(attrName)) {
                attr = ctx.attribute(attrName);
            } else if (ctx.queryParamMap().containsKey(attrName)) {
                attr = ctx.queryParam(attrName);
            } else {
                logger.log(Level.FINE, "No {0} attribute", new Object[]{attrName});
            }
            if (attr != null && !attr.isEmpty()) {
                retval = Optional.of(attr);
            } else {
                logger.log(Level.FINE, "{0} attribute value was null or empty",
                        new Object[]{attrName});
            }
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

    public static String getFailMessage(@NotNull Context ctx,
                                        @NotNull Set<RouteRole> requiredRoles) {
        Set<RouteRole> specifiedRoles = getRoles(ctx);
        Set<RouteRole> missing = new LinkedHashSet<>(requiredRoles);
        missing.removeAll(specifiedRoles);

        return "Request for: " + ctx.req.getRequestURI() + " denied. Missing roles: " + missing;
    }


    public boolean isAuthorized(Context ctx, Set<RouteRole> requiredRoles) {
        Set<RouteRole> specifiedRoles = getRoles(ctx);
        return specifiedRoles.containsAll(requiredRoles);
    }

    public static Set<RouteRole> getRoles(@NotNull Context ctx) {
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
                logger.fine("Principal had roles: " + retval);
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
                .name("JSESSIONIDSSO")
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