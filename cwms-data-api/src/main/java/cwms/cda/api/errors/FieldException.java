package cwms.cda.api.errors;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import javax.servlet.http.HttpServletResponse;

public class FieldException extends ApplicationException {

    public FieldException(String message) {
        super(message, "Parser", message, HttpServletResponse.SC_BAD_REQUEST, new HashMap<>(), null);
    }

    public FieldException(String message, Map<String, Serializable> details) {
        super(message, "Parser", message, HttpServletResponse.SC_BAD_REQUEST, details, null);
    }

    @Override
    public Map<String, Serializable> getDetails() {
        return new HashMap<>();
    }
}
