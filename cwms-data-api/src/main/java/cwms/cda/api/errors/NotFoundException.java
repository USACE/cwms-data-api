package cwms.cda.api.errors;

import java.util.HashMap;
import javax.servlet.http.HttpServletResponse;

public class NotFoundException extends ApplicationException {
    private static final String DATABASE = "Database";
    private static final String NOT_FOUND = "Not Found.";

    public NotFoundException(String message) {
        super(message, DATABASE, NOT_FOUND, HttpServletResponse.SC_NOT_FOUND, new HashMap<>(), null);
    }

    public NotFoundException(String message, Throwable cause) {
        super(message,DATABASE, NOT_FOUND, HttpServletResponse.SC_NOT_FOUND, new HashMap<>(), cause);
    }

    public NotFoundException(Throwable cause) {
        super(NOT_FOUND, DATABASE, NOT_FOUND, HttpServletResponse.SC_NOT_FOUND, new HashMap<>(), cause);
    }

    public NotFoundException() {
        super(NOT_FOUND, DATABASE, NOT_FOUND, HttpServletResponse.SC_NOT_FOUND, new HashMap<>(), null);
    }
}
