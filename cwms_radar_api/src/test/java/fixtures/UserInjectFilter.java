package fixtures;

import java.io.IOException;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;

import cwms.auth.CwmsUserPrincipal;

public class UserInjectFilter implements Filter
{

	public void init(FilterConfig cfg) throws ServletException
	{
	}

	public void doFilter(ServletRequest req, ServletResponse response,
	                     FilterChain next) throws IOException, ServletException {
		HttpServletRequest request = (HttpServletRequest) req;

		TestRealm realm = RadarApiSetupCallback.realm();
		CwmsUserPrincipal currentPrincipal = realm.getCurrentPrincipal();

		next.doFilter(new CwmsUserPrincipalRequestWrapper(currentPrincipal, request), response);
	}

	public void destroy() {
	}
}

