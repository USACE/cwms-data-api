package fixtures;

import java.security.Principal;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.logging.Logger;

import cwms.auth.CwmsUserPrincipal;
import cwms.radar.ApiServlet;
import org.apache.catalina.realm.RealmBase;

public class TestRealm extends RealmBase
{
	public final static Logger logger = Logger.getLogger(TestRealm.class.getName());

	public static CwmsUserPrincipal NULL = null;
	public static CwmsUserPrincipal USER1 = new TestCwmsUserPrincipal("user1", "user1SessionKey", Arrays.asList(
			ApiServlet.CWMS_USERS_ROLE));
	public static CwmsUserPrincipal USER2 = new TestCwmsUserPrincipal("user2", "user2SessionKey", Collections.emptyList());


	private Map<String, Principal> usernameMap = new LinkedHashMap<>();


	private CwmsUserPrincipal principal = null;

	public TestRealm()
	{
		super();

		usernameMap.put(null, NULL);
		usernameMap.put("user1", USER1);
		usernameMap.put("user2", USER2);
	}

	public void setCurrentPrincipal(CwmsUserPrincipal user){
		principal = user;
		if(user != null){
			usernameMap.put(user.getName(), user);
		} else {
			usernameMap.put(null, null);
		}
	}

	public CwmsUserPrincipal getCurrentPrincipal(){
		return principal;
	}

	@Override
	protected String getPassword(String username)
	{
		return null;
	}

	@Override
	protected Principal getPrincipal(String username)
	{
		logger.info("getPrincipal: " + username);
		return usernameMap.get(username);
	}


}
