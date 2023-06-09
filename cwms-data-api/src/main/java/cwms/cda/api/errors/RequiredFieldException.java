package cwms.cda.api.errors;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class RequiredFieldException extends FieldException {
    public static final String MISSING_FIELDS = "missing fields";
    public static final String MESSAGE = "required fields not present";
    private final Map<String, List<String>> details = new LinkedHashMap<>();

    private RequiredFieldException() {
        super(MESSAGE);
        details.put(MISSING_FIELDS, new ArrayList<>());
    }

    public RequiredFieldException(String field) {
        this();
        details.get(MISSING_FIELDS).add(field);
    }

    public RequiredFieldException(List<String> fields) {
        this();
        details.get(MISSING_FIELDS).addAll(fields);
    }

    @Override
    public Map<String, ? extends List<String>> getDetails() {
        return details;
    }
}
