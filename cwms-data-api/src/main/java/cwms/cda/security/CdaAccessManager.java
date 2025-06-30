package cwms.cda.security;

import com.google.common.flogger.FluentLogger;
import io.javalin.http.HttpResponseException;
import io.javalin.http.util.NaiveRateLimit;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import cwms.cda.ApiServlet;
import cwms.cda.data.dao.AuthDao;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.spi.IdentityProvider;
import io.javalin.core.security.AccessManager;
import io.javalin.core.security.RouteRole;
import io.javalin.http.Context;
import io.javalin.http.Handler;
import java.util.concurrent.TimeUnit;

public final class CdaAccessManager implements AccessManager {
    public static final FluentLogger logger = FluentLogger.forEnclosingClass();

    // specify the maximum number of requests allowed per time unit
    public static final String REQUEST_LIMIT_KEY = "cwms.dataapi.request.limit";
    private static final int REQUEST_LIMIT = Integer.parseInt(System.getProperty(REQUEST_LIMIT_KEY, "100"));
    private static final TimeUnit REQUEST_LIMIT_UNIT = TimeUnit.MINUTES;
    private final Map<String, RouteRole[]> rateLimitedPaths = new HashMap<>();

    @Override
    public void  manage(Handler handler, Context ctx, Set<RouteRole> routeRoles) throws Exception {
        DataApiPrincipal principal = getApiPrincipal(ctx);
        AuthDao.isAuthorized(ctx, principal, routeRoles);
        checkRateLimit(ctx);
        prepareContext(ctx, principal);
        handler.handle(ctx);
    }

    private void checkRateLimit(Context ctx) {
        String path = ctx.endpointHandlerPath();
        RouteRole[] routeRoles = rateLimitedPaths.get(path);
        if (routeRoles != null && routeRoles.length != 0) {
            try {
                NaiveRateLimit.requestPerTimeUnit(ctx, REQUEST_LIMIT, REQUEST_LIMIT_UNIT);
            } catch (HttpResponseException ex) {
                try {
                    AuthDao.isAuthorized(ctx, getApiPrincipal(ctx), new HashSet<>(Arrays.asList(routeRoles)));
                } catch (CwmsAuthException e) {
                    // If user is unauthorized, rethrow the rate limit exception
                    logger.atFinest().log("Unauthorized access to rate limited path: %s", path, e);
                    throw new HttpResponseException(ex.getStatus(), "Rate limit exceeded. "
                        + "Please try again later or contact an administrator if you believe this is an error.");
                }
            }
        }
    }

    private DataApiPrincipal getApiPrincipal(Context ctx) {
        return ctx.sessionAttribute(IdentityProvider.PRINCIPAL_KEY);
    }

    private void prepareContext(Context ctx, DataApiPrincipal p) {
        if (p == null) {
            AuthDao authDao = AuthDao.getInstance(JooqDao.getDslContext(ctx),ctx.attribute(ApiServlet.OFFICE_ID));
            authDao.prepareGuestContext(ctx);
        } else {
            AuthDao.prepareContextWithUser(ctx, p);
        }
    }

    public void addRateLimitedEndpoint(String path, RouteRole[] roles) {
        rateLimitedPaths.put(path, roles);
    }
}
