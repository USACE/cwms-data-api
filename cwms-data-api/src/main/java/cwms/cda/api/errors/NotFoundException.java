package cwms.cda.api.errors;

import java.util.HashMap;
import java.util.logging.Level;
import javax.servlet.http.HttpServletResponse;

public class NotFoundException extends ApplicationException {
    private static final Level LOG_LEVEL = Level.FINE;
    private static final String NOT_FOUND = "Not Found.";

    public NotFoundException(String message) {
        super(message, DATABASE_SOURCE, message, HttpServletResponse.SC_NOT_FOUND,
            LOG_LEVEL, new HashMap<>(), null);
    }

    public NotFoundException(String message, Throwable cause) {
        super(message, DATABASE_SOURCE, message, HttpServletResponse.SC_NOT_FOUND,
            LOG_LEVEL, new HashMap<>(), cause);
    }

    public NotFoundException(Throwable cause) {
        super(NOT_FOUND, DATABASE_SOURCE, NOT_FOUND, HttpServletResponse.SC_NOT_FOUND,
            LOG_LEVEL, new HashMap<>(), cause);
    }

    public NotFoundException() {
        super(NOT_FOUND, DATABASE_SOURCE, NOT_FOUND, HttpServletResponse.SC_NOT_FOUND,
            LOG_LEVEL, new HashMap<>(), null);
    }
}
