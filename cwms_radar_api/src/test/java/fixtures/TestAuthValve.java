package fixtures;

import java.io.IOException;
import java.security.Principal;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.logging.Logger;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.catalina.authenticator.AuthenticatorBase;
import org.apache.catalina.connector.Request;
import org.apache.catalina.realm.GenericPrincipal;

import cwms.auth.CwmsUserPrincipal;
import cwms.radar.ApiServlet;

public class TestAuthValve extends AuthenticatorBase{
    public final static Logger logger = Logger.getLogger(TestAuthValve.class.getName());

	private Map<String, Principal> usernameMap = new LinkedHashMap<>();


	public TestAuthValve() {
		super();
		//usernameMap.put("user1", USER1);
		//usernameMap.put("user2", USER2);
    }

    @Override
    protected boolean doAuthenticate(Request request, HttpServletResponse response) throws IOException {
        logger.fine("handling test authentication");
        String authHeader = request.getHeader("Authorization");
        if(authHeader != null)
        {
            int idx = authHeader.indexOf("name");
            if(idx >= 0)
            {
                String name = authHeader.substring(idx + 5).trim();
                Principal p = usernameMap.get(name);
                if(p != null)
                {
                    register(request, response, p, HttpServletRequest.BASIC_AUTH, p.getName(), "");
                    return true;
                }

            }
        }
        response.sendError(HttpServletResponse.SC_UNAUTHORIZED);
        return false;
    }

    @Override
    protected String getAuthMethod() {
        return "TESTSYSTEM";
    }

    public void addUser( String username, CwmsUserPrincipal principal){
        usernameMap.put(username,principal);
    }

}
