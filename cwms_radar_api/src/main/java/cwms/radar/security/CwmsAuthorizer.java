package cwms.radar.security;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import io.javalin.http.Context;

public abstract class CwmsAuthorizer {

    public abstract void can_perform(HttpServletRequest request, HttpServletResponse response) throws CwmsAuthException;

    public void can_perform( Context context ) throws CwmsAuthException {
        can_perform(context.req,context.res);
    }




}
