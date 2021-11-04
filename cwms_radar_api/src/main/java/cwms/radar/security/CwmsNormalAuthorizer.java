package cwms.radar.security;

import java.security.Principal;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Baseline authorizor that just verifies the
 * aqcuired principal has the CWMS Users role
 */
public class CwmsNormalAuthorizer extends CwmsAuthorizer{

    @Override
    public void can_perform(HttpServletRequest request, HttpServletResponse response) throws CwmsAuthException {

        if( !request.isUserInRole("CWMS Users") ){
            throw new CwmsAuthException("User not authorized to perform desired operations");
        }

    }

}
