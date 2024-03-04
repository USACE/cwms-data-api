package cwms.cda.api.errors;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class RequiredQueryParameterException extends RuntimeException{

    public static final String MISSING_QUERY_PARAMETERS = "missing query parameters";
    public static final String MESSAGE = "required query parameters not present";
    private final Map<String, List<String>> details = new LinkedHashMap<>();

    private RequiredQueryParameterException() {
        super(MESSAGE);
        details.put(MISSING_QUERY_PARAMETERS, new ArrayList<>());
    }

    public RequiredQueryParameterException(String field) {
        this();
        details.get(MISSING_QUERY_PARAMETERS).add(field);
    }

    public RequiredQueryParameterException(List<String> fields) {
        this();
        details.get(MISSING_QUERY_PARAMETERS).addAll(fields);
    }


    public Map<String, List<String>> getDetails() {
        return details;
    }
}
