package cwms.cda.api.errors;

import java.util.HashMap;
import java.util.logging.Level;
import javax.servlet.http.HttpServletResponse;

public class AlreadyExists extends ApplicationException {
    private static final String ALREADY_EXISTS = "Already exists";
    private static final Level LOG_LEVEL = Level.INFO;

    public AlreadyExists(String message, Throwable cause) {
        super(message, DATABASE_SOURCE, ALREADY_EXISTS, HttpServletResponse.SC_CONFLICT,
            LOG_LEVEL, buildDetailsMap(message), cause);
    }

    public AlreadyExists(Throwable cause) {
        super(ALREADY_EXISTS, DATABASE_SOURCE, ALREADY_EXISTS, HttpServletResponse.SC_CONFLICT,
            LOG_LEVEL, new HashMap<>(), cause);
    }
}
