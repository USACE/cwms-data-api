package fixtures;

import java.security.Principal;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;

import cwms.auth.CwmsUserPrincipal;

public class CwmsUserPrincipalRequestWrapper extends HttpServletRequestWrapper
{

	CwmsUserPrincipal principal;
	HttpServletRequest realRequest;

	public CwmsUserPrincipalRequestWrapper(CwmsUserPrincipal principal, HttpServletRequest request) {
		super(request);
		this.principal = principal;
		this.realRequest = request;
	}

	@Override
	public boolean isUserInRole(String role) {
		if (principal == null) {
			return this.realRequest.isUserInRole(role);
		}
		return principal.getRoles().contains(role);
	}

	@Override
	public Principal getUserPrincipal() {
		if (this.principal == null) {
			return realRequest.getUserPrincipal();
		}

		return principal;
	}
}