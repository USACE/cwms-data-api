package cwms.radar.security;

import cwms.auth.CwmsUserPrincipal;
import cwms.radar.ApiServlet;
import cwms.radar.api.errors.RadarError;
import cwms.radar.datasource.ConnectionPreparer;
import cwms.radar.datasource.ConnectionPreparingDataSource;
import cwms.radar.datasource.DelegatingConnectionPreparer;
import cwms.radar.datasource.SessionUserPreparer;
import io.javalin.core.security.AccessManager;
import io.javalin.core.security.RouteRole;
import io.javalin.http.Context;
import io.javalin.http.Handler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.Principal;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.servlet.http.HttpServletResponse;
import javax.sql.DataSource;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class CwmsAccessManager implements AccessManager {
    public static final Logger logger = Logger.getLogger(CwmsAccessManager.class.getName());
    public static final String DATABASE = "database";

    @Override
    public void manage(@NotNull Handler handler, @NotNull Context ctx,
					   @NotNull Set<RouteRole> requiredRoles)
            throws Exception {
        boolean shouldProceed = isAuthorized(ctx, requiredRoles);

        if (shouldProceed) {
            setSessionKey(ctx);

            // Let the handler handle the request.
            handler.handle(ctx);
        } else {
            String msg = getFailMessage(ctx, requiredRoles);
            logger.info(msg);
            ctx.status(HttpServletResponse.SC_UNAUTHORIZED).json(RadarError.notAuthorized());
            ctx.status(401).result("Unauthorized");
        }

    }

    private void setSessionKey(@NotNull Context ctx) {
        String sessionKey = getSessionKey(ctx);
        if (sessionKey != null) {
//            // Set the user's session key in the database.
//            Connection conn = ctx.attribute(DATABASE);
//            setSession(conn, sessionKey);

            DataSource dataSource = ctx.attribute(ApiServlet.DATA_SOURCE);

            DataSource sessionSettingDataSource = buildDataSource(dataSource, sessionKey);
            ctx.attribute(ApiServlet.DATA_SOURCE, sessionSettingDataSource);
        }
    }

    private DataSource buildDataSource(DataSource dataSource, String sessionKey) {
        DataSource retval = null;

        ConnectionPreparer newPreparer = new SessionUserPreparer(sessionKey);

        if(dataSource instanceof ConnectionPreparingDataSource) {
            ConnectionPreparingDataSource cpDs = (ConnectionPreparingDataSource)    dataSource;
            ConnectionPreparer existingPreparer = cpDs.getPreparer();

            // Have it do our extra step last.
            cpDs.setPreparer(new DelegatingConnectionPreparer(existingPreparer, newPreparer));
        } else {
            retval = new ConnectionPreparingDataSource(newPreparer, dataSource);
        }

        return retval;
    }

    @NotNull
    private String getFailMessage(@NotNull Context ctx, @NotNull Set<RouteRole> requiredRoles) {
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

    public Set<RouteRole> getRoles(Context ctx) {
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

    @Nullable
    private String getSessionKey(@NotNull Context ctx) {
        String sessionKey = null;

        Principal principal = ctx.req.getUserPrincipal();
        if (principal != null) {
            try {
                CwmsUserPrincipal cup = (CwmsUserPrincipal) principal;
                sessionKey = cup.getSessionKey();
            } catch (ClassCastException e) {
                // The object is created by cwms_aaa with the cwms_aaa classloader.
                // It's a CwmsUserPrincipal but it's not our CwmsUserPrincipal.
                sessionKey = callGetSessionKeyReflectively(principal);
            }
        }
        return sessionKey;
    }

    private String callGetSessionKeyReflectively(Principal principal) {
        String sessionKey = null;
        Method getSessionKeyMethod;
        try {
            getSessionKeyMethod = principal.getClass().getMethod("getSessionKey", new Class[]{});
            Object retval = getSessionKeyMethod.invoke(principal, new Object[]{});
            if (retval instanceof String) {
                sessionKey = (String) retval;
            }
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            logger.log(Level.WARNING, "Could not call getSessionKey() on principal.", e);
        }

        return sessionKey;
    }

    private Set<RouteRole> getRoles(Principal principal) {
        Set<RouteRole> retval = new LinkedHashSet<>();
        if (principal != null) {
            List<String> roleNames;
            try {
                CwmsUserPrincipal cup = (CwmsUserPrincipal) principal;
                roleNames = cup.getRoles();
            } catch (ClassCastException e) {
                // The object is created by cwms_aaa with the cwms_aaa classloader.
                // It's a CwmsUserPrincipal but it's not our CwmsUserPrincipal.
                roleNames = callGetRolesReflectively(principal);
            }

            if (roleNames != null) {
                roleNames.stream().map(CwmsAccessManager::buildRole).forEach(retval::add);
            }
            logger.info("Principal had roles: " + retval);
        }
        return retval;
    }

    List<String> callGetRolesReflectively(Principal principal) {
        List<String> retval = new ArrayList<>();

        Method getRolesMethod;
        try {
            getRolesMethod = principal.getClass().getMethod("getRoles", new Class[]{});
            Object retvalObj = getRolesMethod.invoke(principal, new Object[]{});
            if (retvalObj instanceof List) {
                retval = (List<String>) retvalObj;
            }
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            logger.log(Level.WARNING, "Could not call getRoles() on principal.", e);
        }
        return retval;
    }


    public static RouteRole buildRole(String roleName) {
        return new Role(roleName);
    }

//    @NotNull
//    private static Connection setSession(Connection conn, String sessionKey) {
//        // Need to figure out a legit way to skip this if we are doing testing.
//        if (sessionKey == null || !sessionKey.startsWith("testing")) {
//            try (DSLContext dsl = DSL.using(conn, SQLDialect.ORACLE11G)) {
//                CWMS_ENV_PACKAGE.call_SET_SESSION_USER(dsl.configuration(), sessionKey);
//            }
//
//        }
//
//        return conn;
//    }

}
