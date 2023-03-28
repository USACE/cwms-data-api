package cwms.radar.security;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

import javax.servlet.http.HttpServletResponse;

import cwms.radar.api.errors.RadarError;
import cwms.radar.spi.RadarAccessManager;
import io.javalin.core.security.RouteRole;
import io.javalin.http.Context;
import io.javalin.http.Handler;
import io.swagger.v3.oas.models.security.SecurityScheme;

public class MultipleAccessManager extends RadarAccessManager {
    private static final Logger log = Logger.getLogger(MultipleAccessManager.class.getName());

    private List<RadarAccessManager> managers = new ArrayList<>();

    public MultipleAccessManager(List<RadarAccessManager> managerList) {
        managers.addAll(managerList);
    }   

    private RadarAccessManager getManagerFor(Context ctx, Set<RouteRole> roles) {
        // There is always a GUEST manager so downstream managers don't have to worry about.
        managers.add(new GuestAccessManager()); 
        for (RadarAccessManager am: managers) {            
            if (am.canAuth(ctx, roles)) {
                return am;
            }
        }
        return null;
    }

    @Override
    public void manage(Handler handler, Context ctx, Set<RouteRole> routeRoles) throws Exception {
        RadarAccessManager am = getManagerFor(ctx, routeRoles);
        
        if (am != null) {
            am.manage(handler, ctx, routeRoles);
        } else {
            log.warning("No valid credentials on request.");
            ctx.status(HttpServletResponse.SC_UNAUTHORIZED).json(RadarError.notAuthorized());
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
    public List<RadarAccessManager> getContainedManagers() {
        return new ArrayList<>(managers);
    }

    @Override
    public boolean canAuth(Context ctx, Set<RouteRole> roles) {
        return true; // at the very least, it can try.
    }


    
}
