package cwms.cda.api.errors;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import javax.servlet.http.HttpServletResponse;

public class FieldException extends ApplicationException {
    private static final Level LOG_LEVEL = null;

    public FieldException(String message) {
        super(message, PARSER_SOURCE, message, HttpServletResponse.SC_BAD_REQUEST, LOG_LEVEL, new HashMap<>(), null);
    }

    public FieldException(String message, Map<String, Serializable> details) {
        super(message, PARSER_SOURCE, message, HttpServletResponse.SC_BAD_REQUEST, LOG_LEVEL, details, null);
    }

    @Override
    public Map<String, Serializable> getDetails() {
        return new HashMap<>();
    }

    protected static Map<String, Serializable> createDetails(String key, Set<String> fields) {
        Map<String, Serializable> details = new HashMap<>();
        String fieldString = String.join(", ", fields);
        details.put(key, fieldString);
        return details;
    }
}
