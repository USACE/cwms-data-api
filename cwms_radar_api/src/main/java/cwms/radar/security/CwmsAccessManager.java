package cwms.radar.security;

import java.security.Principal;
import java.sql.Connection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;
import javax.servlet.http.HttpServletResponse;

import cwms.auth.CwmsUserPrincipal;
import cwms.radar.api.errors.RadarError;
import io.javalin.core.security.AccessManager;
import io.javalin.core.security.RouteRole;
import io.javalin.http.Context;
import io.javalin.http.Handler;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;

import usace.cwms.db.jooq.codegen.packages.CWMS_ENV_PACKAGE;

public class CwmsAccessManager implements AccessManager
{
	public static final Logger logger = Logger.getLogger(CwmsAccessManager.class.getName());
	public static final String DATABASE = "database";

	@Override
	public void manage(@NotNull Handler handler, @NotNull Context ctx, @NotNull Set<RouteRole> requiredRoles)
			throws Exception
	{
		boolean shouldProceed = isAuthorized(ctx, requiredRoles);

		if(shouldProceed)
		{
			setSessionKey(ctx);

			// Let the handler handle the request.
			handler.handle(ctx);
		}
		else
		{
			String msg = getFailMessage(ctx, requiredRoles);
			logger.info(msg);
			ctx.status(HttpServletResponse.SC_UNAUTHORIZED).json(RadarError.notAuthorized());
			ctx.status(401).result("Unauthorized");
		}

	}

	private void setSessionKey(@NotNull Context ctx)
	{
		String sessionKey = getSessionKey(ctx);
		if(sessionKey != null)
		{
			// Set the user's session key in the database.
			Connection conn = ctx.attribute(DATABASE);
			setSession(conn, sessionKey);
		}
	}

	@NotNull
	private String getFailMessage(@NotNull Context ctx, @NotNull Set<RouteRole> requiredRoles)
	{
		Set<RouteRole> specifiedRoles = getRoles(ctx);
		Set<RouteRole> missing = new LinkedHashSet<>(requiredRoles);
		missing.removeAll(specifiedRoles);

		return "Request for: " + ctx.req.getRequestURI() + " denied. Missing roles: " + missing;
	}

	@Nullable
	private String getSessionKey(@NotNull Context ctx)
	{
		String sessionKey = null;

		CwmsUserPrincipal principal = getPrincipal(ctx);

		if(principal != null){
			sessionKey = principal.getSessionKey();
		}
		return sessionKey;
	}

	@Nullable
	private CwmsUserPrincipal getPrincipal(@NotNull Context ctx)
	{
//		CwmsUserPrincipal principal = ctx.attribute("principal"); // wrong
		Principal userPrincipal = ctx.req.getUserPrincipal();
		return (CwmsUserPrincipal) userPrincipal;
	}

	public boolean isAuthorized(Context ctx, Set<RouteRole> requiredRoles)
	{
		boolean retval;
		if(requiredRoles == null || requiredRoles.isEmpty())
		{
			retval = true;
		}
		else
		{
			Set<RouteRole> specifiedRoles = getRoles(ctx);
			retval = specifiedRoles.containsAll(requiredRoles);
		}
		return retval;
	}

	public Set<RouteRole> getRoles(Context ctx){
		Set<RouteRole> retval = new LinkedHashSet<>();
		if(ctx != null)
		{
			CwmsUserPrincipal principal = getPrincipal(ctx);
			Set<RouteRole> specifiedRoles = getRoles(principal);
			if(!specifiedRoles.isEmpty())
			{
				retval.addAll(specifiedRoles);
			}
		}
		return retval;
	}

	private Set<RouteRole> getRoles(CwmsUserPrincipal principal)
	{
		Set<RouteRole> retval = new LinkedHashSet<>();
		if(principal != null)
		{
			List<String> roleNames = principal.getRoles();
			roleNames.stream().map(CwmsAccessManager::buildRole).forEach(retval::add);
			logger.info("Principal had roles: " + retval);
		}
		return retval;
	}

	public static RouteRole buildRole(String roleName)
	{
		return new Role(roleName);
	}

	@NotNull
	private static Connection setSession(Connection conn, String sessionKey)
	{
		try(DSLContext dsl = DSL.using(conn, SQLDialect.ORACLE11G))
		{
			CWMS_ENV_PACKAGE.call_SET_SESSION_USER(dsl.configuration(), sessionKey);
		}

		return conn;
	}

}
