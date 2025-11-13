package cwms.cda.security;

import cwms.cda.api.errors.ApplicationException;
import java.util.HashMap;
import java.util.logging.Level;
import javax.servlet.http.HttpServletResponse;

public class CwmsAuthException extends ApplicationException {
    private static final String INVALID_USER = "Invalid User";
    private static final String NOT_AUTHORIZED = "Not Authorized";
    private static final Level LOG_LEVEL = Level.FINE;
    private int authFailCode = HttpServletResponse.SC_UNAUTHORIZED;

    public CwmsAuthException(String msg) {
        super(msg, AUTHORIZATION_SOURCE, msg.isBlank() ? INVALID_USER : msg,
            HttpServletResponse.SC_UNAUTHORIZED, LOG_LEVEL, new HashMap<>(), null);
    }

    public CwmsAuthException(String msg, int code) {
        super(msg, AUTHORIZATION_SOURCE, getAuthErrorMessageFromCodeAndMessage(msg, code),
            code, LOG_LEVEL, new HashMap<>(), null);
        authFailCode = code;
    }

    public CwmsAuthException(String msg, Throwable err) {
        super(msg, AUTHORIZATION_SOURCE, msg.isBlank() ? INVALID_USER : msg,
            HttpServletResponse.SC_UNAUTHORIZED, LOG_LEVEL, new HashMap<>(), err);
    }

    public CwmsAuthException(String msg, Throwable err, int code) {
        super(msg, AUTHORIZATION_SOURCE, getAuthErrorMessageFromCodeAndMessage(msg, code),
            code, LOG_LEVEL, new HashMap<>(), err);
        authFailCode = code;
    }

    public CwmsAuthException(String msg, int code, String rolesMessage) {
        super(msg, AUTHORIZATION_SOURCE, rolesMessage, code, LOG_LEVEL, new HashMap<>(), null);
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
                return "Unknown authorization error";
        }
    }
}
