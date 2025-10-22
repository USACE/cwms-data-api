package cwms.cda.api.errors;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.servlet.http.HttpServletResponse;

public class RequiredQueryParameterException extends ApplicationException {

    public static final String MISSING_QUERY_PARAMETERS = "missing query parameters";
    public static final String MESSAGE = "required query parameters not present";
    private final Map<String, Serializable> details = new LinkedHashMap<>();

    private RequiredQueryParameterException() {
        super(MESSAGE, "User Input", "Bad Request", HttpServletResponse.SC_BAD_REQUEST, new HashMap<>(), null);
        details.put(MISSING_QUERY_PARAMETERS, new ArrayList<>());
    }

    public RequiredQueryParameterException(String field) {
        this();
        ((List<Serializable>) details.get(MISSING_QUERY_PARAMETERS)).add(field);
    }

    public RequiredQueryParameterException(List<String> fields) {
        this();
        ((List<Serializable>) details.get(MISSING_QUERY_PARAMETERS)).addAll(fields);
    }

    @Override
    public Map<String, Serializable> getDetails() {
        return details;
    }
}
