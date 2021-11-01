package cwms.radar.security;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * The default Authorizer used currently for the public RADAR instance
 */
public class CwmsNoAuthorizer extends CwmsAuthorizer {

    @Override
    public void can_perform(HttpServletRequest request, HttpServletResponse response) throws CwmsAuthException {
        throw new CwmsAuthException("No change operations are authorized");
    }

}
