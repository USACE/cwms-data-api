package cwms.cda.security;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

import javax.servlet.http.HttpServletResponse;

import cwms.cda.spi.CdaAccessManager;
import cwms.cda.api.errors.CdaError;
import io.javalin.core.security.RouteRole;
import io.javalin.http.Context;
import io.javalin.http.Handler;
import io.swagger.v3.oas.models.security.SecurityScheme;

public class MultipleAccessManager extends CdaAccessManager {
    private static final Logger log = Logger.getLogger(MultipleAccessManager.class.getName());

    private List<CdaAccessManager> managers = new ArrayList<>();

    public MultipleAccessManager(List<CdaAccessManager> managerList) {
        managers.addAll(managerList);
    }   

    private CdaAccessManager getManagerFor(Context ctx, Set<RouteRole> roles) {
        for (CdaAccessManager am: managers) {            
            if (am.canAuth(ctx, roles)) {
                return am;
            }
        }
        return null;
    }

    @Override
    public void manage(Handler handler, Context ctx, Set<RouteRole> routeRoles) throws Exception {
        CdaAccessManager am = getManagerFor(ctx, routeRoles);
        log.fine("Principal: " + ctx.req.getUserPrincipal());
        log.fine("Session: " + ctx.req.getSession(false));
        if (am != null) {
            am.manage(handler, ctx, routeRoles);
        } else {
            log.warning("No valid credentials on request.");
            ctx.status(HttpServletResponse.SC_UNAUTHORIZED).json(CdaError.notAuthorized());
        }
    }

    @Override
    public String getName() {
        return "MultipleAuthContainer";
    }

    @Override
    public SecurityScheme getScheme() {
        throw new UnsupportedOperationException("This manager does not have it's own schema and this should not be called.");
    }

    @Override
    public List<CdaAccessManager> getContainedManagers() {
        return new ArrayList<>(managers);
    }

    @Override
    public boolean canAuth(Context ctx, Set<RouteRole> roles) {
        return true; // at the very least, it can try.
    }


    
}
