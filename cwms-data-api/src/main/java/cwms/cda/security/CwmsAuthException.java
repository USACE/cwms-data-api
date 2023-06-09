package cwms.cda.security;

import javax.servlet.http.HttpServletResponse;

public class CwmsAuthException extends RuntimeException {

    private int authFailCode = HttpServletResponse.SC_UNAUTHORIZED;

    public CwmsAuthException(String msg) {
        super(msg);
    }

    public CwmsAuthException(String msg, int code) {
        super(msg);
        authFailCode = code;
    }

    public CwmsAuthException(String msg, Throwable err) {
        super(msg,err);
    }

    public CwmsAuthException(String msg, Throwable err, int code) {
        super(msg,err);
        authFailCode = code;
    }

    public int getAuthFailCode() {
        return this.authFailCode;
    }

}
