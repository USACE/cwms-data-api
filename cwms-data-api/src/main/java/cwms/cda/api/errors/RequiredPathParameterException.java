package cwms.cda.api.errors;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import jakarta.servlet.http.HttpServletResponse;

public final class RequiredPathParameterException extends ApplicationException {
    private static final Level LOG_LEVEL = Level.INFO;
    public static final String MISSING_PATH_PARAMETERS = "missing Path parameters";
    public static final String MESSAGE = "required Path parameters not present";
    public static final String CDA_MESSAGE = "Bad Request";
    private final Map<String, Serializable> details = new LinkedHashMap<>();

    private RequiredPathParameterException() {
        super(MESSAGE, USER_INPUT_SOURCE, CDA_MESSAGE, HttpServletResponse.SC_BAD_REQUEST,
            LOG_LEVEL, new HashMap<>(), null);
        details.put(MISSING_PATH_PARAMETERS, new ArrayList<>());
    }

    public RequiredPathParameterException(String field) {
        super(MESSAGE, USER_INPUT_SOURCE, CDA_MESSAGE, HttpServletResponse.SC_BAD_REQUEST, LOG_LEVEL,
            buildDetailsMap(Collections.singletonList(field)), null);
        details.put(MISSING_PATH_PARAMETERS, field);
    }

    public RequiredPathParameterException(List<String> fields) {
        super(MESSAGE, USER_INPUT_SOURCE, CDA_MESSAGE, HttpServletResponse.SC_BAD_REQUEST, LOG_LEVEL,
            buildDetailsMap(fields), null);
        String fieldString = String.join(",", fields);
        details.put(MISSING_PATH_PARAMETERS, fieldString);
    }

    @Override
    public Map<String, Serializable> getDetails() {
        return details;
    }

    private static Map<String, Serializable> buildDetailsMap(List<String> fields) {
        Map<String, Serializable> details = new LinkedHashMap<>();
        String fieldString = String.join(",", fields);
        details.put(MISSING_PATH_PARAMETERS, fieldString);
        return details;
    }
}
