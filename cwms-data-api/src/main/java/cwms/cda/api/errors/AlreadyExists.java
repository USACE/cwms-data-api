package cwms.cda.api.errors;

import java.util.HashMap;
import javax.servlet.http.HttpServletResponse;

public class AlreadyExists extends ApplicationException {
    private static final String ALREADY_EXISTS = "Already exists";

    public AlreadyExists(String message, Throwable cause) {
        super(message, "Database", ALREADY_EXISTS, HttpServletResponse.SC_CONFLICT, buildDetailsMap(message), cause);
    }

    public AlreadyExists(Throwable cause) {
        super(ALREADY_EXISTS, "Database", ALREADY_EXISTS, HttpServletResponse.SC_CONFLICT, new HashMap<>(), cause);
    }
}
