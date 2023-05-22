package fixtures.tomcat;

import java.io.IOException;
import java.security.Principal;

import javax.servlet.ServletException;
import org.apache.catalina.connector.Request;
import org.apache.catalina.connector.Response;

import org.apache.catalina.authenticator.SingleSignOn;

public class SingleSignOnWrapper extends SingleSignOn{
    public void wrappedRegister(String ssoId, Principal principal, String authType,
    String username, String password) {
        this.register(ssoId, principal, authType, username, password);
    }

    @Override
    public void invoke(Request request, Response response)
        throws IOException, ServletException {
            super.invoke(request, response);
        }
}
