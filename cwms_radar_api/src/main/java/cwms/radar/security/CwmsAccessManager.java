package cwms.radar.security;

import java.sql.Connection;
import java.sql.SQLException;
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
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;

import usace.cwms.db.jooq.codegen.packages.CWMS_ENV_PACKAGE;

public class CwmsAccessManager implements AccessManager
{
	public static final Logger logger = Logger.getLogger(CwmsAccessManager.class.getName());

	@Override
	public void manage(@NotNull Handler handler, @NotNull Context ctx, @NotNull Set<RouteRole> requiredRoles)
			throws Exception
	{
		CwmsUserPrincipal principal = ctx.attribute("principal");

		Set<RouteRole> specifiedRoles = extractRoles(principal);
		if(specifiedRoles.containsAll(requiredRoles))
		{
			// Set the user's session key in the database.
			setSessionKey(ctx, principal.getSessionKey());

			// Let the handler handle the request.
			handler.handle(ctx);
		}
		else
		{
			Set<RouteRole> missing = new LinkedHashSet<>(specifiedRoles);
			missing.removeAll(requiredRoles);

			logger.info("Request for: " + ctx.req.getRequestURI() + " denied. Missing roles: " + missing);
			ctx.status(HttpServletResponse.SC_UNAUTHORIZED).json(RadarError.notAuthorized());
			ctx.status(401).result("Unauthorized");
		}


	}

	private Set<RouteRole> extractRoles(CwmsUserPrincipal principal)
	{
		Set<RouteRole> retval = new LinkedHashSet<>();
		if(principal != null)
		{
			List<String> roleNames = principal.getRoles();
			roleNames.stream().map(CwmsAccessManager::buildRole).forEach(retval::add);
		}
		return retval;
	}

	public static RouteRole buildRole(String roleName)
	{
		return new Role(roleName);
	}

	private void setSessionKey(Context ctx, String sessionKey)
	{
		Connection conn = ctx.attribute("database");
		setSession(conn, sessionKey);
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
