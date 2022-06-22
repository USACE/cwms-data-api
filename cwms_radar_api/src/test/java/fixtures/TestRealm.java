package fixtures;

import java.security.Principal;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import cwms.auth.CwmsUserPrincipal;
import cwms.radar.ApiServlet;

import org.apache.catalina.Wrapper;
import org.apache.catalina.realm.RealmBase;

public class TestRealm extends RealmBase
{
	public final static Logger logger = Logger.getLogger(TestRealm.class.getName());



	@Override
	public Principal authenticate(String username) {
		logger.fine("Authenticating " + username);
		assert false; // Should not be called
		return null;
	}

	@Override
	public Principal authenticate(String username, String password) {
		return authenticate(username);
	}



	@Override
	protected String getPassword(String username)
	{
		return null;
	}

	@Override
	public boolean hasRole(Wrapper wrapper, Principal p, String role){
		logger.fine("Checking roles for" + p);
		List<String> roles = ((CwmsUserPrincipal)p).getRoles();
		for( String r: roles){
			if ( r.equals(role)) {
				return true;
			}
		}
		return false;
	}

	@Override
	protected Principal getPrincipal(String username) {
		logger.fine("getting principle for " + username);
		return null;
	}


}
