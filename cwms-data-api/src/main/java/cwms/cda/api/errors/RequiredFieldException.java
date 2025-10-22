package cwms.cda.api.errors;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RequiredFieldException extends FieldException {
    public static final String MISSING_FIELDS = "missing fields";
    public static final String MESSAGE = "required fields not present";
    private final Map<String, Serializable> details = new LinkedHashMap<>();

    private RequiredFieldException() {
        super(MESSAGE);
        details.put(MISSING_FIELDS, new ArrayList<>());
    }

    public RequiredFieldException(String field) {
        super(MESSAGE, createDetails(Set.of(field)));
        details.put(MISSING_FIELDS, field);
    }

    public RequiredFieldException(List<String> fields) {
        super(MESSAGE, createDetails(Set.copyOf(fields)));
        String fieldString = String.join(", ", fields);
        details.put(MISSING_FIELDS, fieldString);
    }

    public RequiredFieldException(Set<String> fields) {
        super(MESSAGE, createDetails(fields));
        String fieldString = String.join(", ", fields);
        details.put(MISSING_FIELDS, fieldString);
    }

    @Override
    public Map<String, Serializable> getDetails() {
        return details;
    }

    private static Map<String, Serializable> createDetails(Set<String> fields) {
        Map<String, Serializable> details = new HashMap<>();
        String fieldString = String.join(", ", fields);
        details.put(MISSING_FIELDS, fieldString);
        return details;
    }
}
