package cwms.cda.security;

import cwms.cda.api.errors.ApplicationException;
import java.util.HashMap;
import javax.servlet.http.HttpServletResponse;

public class CwmsAuthException extends ApplicationException {
    private static final String AUTHORIZATION = "Authorization";
    private static final String INVALID_USER = "Invalid User";
    private static final String NOT_AUTHORIZED = "Not Authorized";
    private int authFailCode = HttpServletResponse.SC_UNAUTHORIZED;

    public CwmsAuthException(String msg) {
        super(msg, AUTHORIZATION, msg.isBlank() ? INVALID_USER : msg,
            HttpServletResponse.SC_UNAUTHORIZED, new HashMap<>(), null);
    }

    public CwmsAuthException(String msg, int code) {
        super(msg, AUTHORIZATION, getAuthErrorMessageFromCodeAndMessage(msg, code),
            code, new HashMap<>(), null);
        authFailCode = code;
    }

    public CwmsAuthException(String msg, Throwable err) {
        super(msg, AUTHORIZATION, msg.isBlank() ? INVALID_USER : msg,
            HttpServletResponse.SC_UNAUTHORIZED, new HashMap<>(), err);
    }

    public CwmsAuthException(String msg, Throwable err, int code) {
        super(msg, AUTHORIZATION, getAuthErrorMessageFromCodeAndMessage(msg, code),
            code, new HashMap<>(), err);
        authFailCode = code;
    }

    public int getAuthFailCode() {
        return this.authFailCode;
    }

    private static String getAuthErrorMessageFromCodeAndMessage(String msg, int code) {
        switch (code) {
            case HttpServletResponse.SC_UNAUTHORIZED:
                return msg.isBlank() ? INVALID_USER : msg;
            case HttpServletResponse.SC_FORBIDDEN:
                return NOT_AUTHORIZED;
            default:
                return "Unknown auth error";
        }
    }
}
